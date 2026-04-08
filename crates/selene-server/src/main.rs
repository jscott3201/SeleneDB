#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use selene_server::bootstrap;
use selene_server::config::SeleneConfig;
use selene_server::ops;
use selene_server::quic::listener;
use selene_server::tasks;
use selene_server::tls;

#[derive(Parser)]
#[command(name = "selene-server", about = "Selene property graph server")]
#[allow(clippy::struct_excessive_bools)]
struct ServerCli {
    /// Path to TOML config file.
    #[arg(long)]
    config: Option<PathBuf>,
    /// Enable dev mode (self-signed TLS, no auth).
    #[arg(long)]
    dev: bool,
    /// Seed demo data on startup (if graph is empty).
    #[arg(long)]
    seed: bool,
    /// QUIC listen address override.
    #[arg(long)]
    quic_listen: Option<String>,
    /// HTTP listen address override.
    #[arg(long)]
    http_listen: Option<String>,
    /// Data directory override.
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// Print effective config and exit.
    #[arg(long)]
    show_config: bool,
    /// Runtime profile: edge, cloud, or standalone.
    /// Overrides SELENE_PROFILE env var and TOML `profile` setting.
    #[arg(long, value_parser = parse_profile)]
    profile: Option<selene_server::config::ProfileType>,
    /// Start as a read-only replica of the given primary address (host:port).
    #[arg(long)]
    replica_of: Option<String>,
    /// Run MCP server over stdin/stdout instead of HTTP/QUIC.
    /// Designed for IDE integrations and agent frameworks that spawn
    /// the database as a subprocess.
    #[arg(long)]
    stdio: bool,
}

fn parse_profile(s: &str) -> Result<selene_server::config::ProfileType, String> {
    s.parse()
}

fn main() -> anyhow::Result<()> {
    // Read and clear vault passphrase before spawning any threads.
    // SAFETY: Single-threaded context — tokio runtime not yet started.
    let vault_passphrase = std::env::var("SELENE_VAULT_PASSPHRASE").ok();
    if vault_passphrase.is_some() {
        unsafe { std::env::remove_var("SELENE_VAULT_PASSPHRASE") };
    }

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(vault_passphrase))
}

async fn async_main(vault_passphrase: Option<String>) -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "selene_server=info".into()),
        )
        .init();

    let cli = ServerCli::parse();

    let data_dir_str = cli
        .data_dir
        .as_ref()
        .map(|p| p.to_string_lossy().to_string());

    let mut config = SeleneConfig::load(cli.config.as_deref(), data_dir_str.as_deref())?;

    // CLI overrides (highest priority)
    if let Some(profile) = cli.profile {
        config.apply_profile(profile);
    }
    if cli.dev {
        config.dev_mode = true;
    }
    if let Some(ref addr) = cli.quic_listen {
        config.listen_addr = addr.parse()?;
    }
    if let Some(ref addr) = cli.http_listen {
        config.http.listen_addr = addr.parse()?;
    }

    if cli.show_config {
        println!("{config:#?}");
        return Ok(());
    }

    let seed = cli.seed;
    ops::init_start_time();

    if config.dev_mode {
        tracing::warn!("=== DEV MODE ACTIVE — no authentication, self-signed TLS ===");
        tracing::warn!("Do NOT use dev mode in production deployments");
    }

    // Set embedding model config before bootstrap
    {
        let model_path = config
            .vector
            .model_path
            .clone()
            .unwrap_or_else(|| config.data_dir.join("models").join("embeddinggemma-300m"));
        selene_gql::runtime::embed::set_model_config(
            config.vector.model.clone(),
            model_path,
            config.vector.dimensions,
        );
    }

    let mut state = bootstrap::bootstrap(config, vault_passphrase).await?;
    if cli.replica_of.is_some() {
        state.set_replica(true);
        state.set_replica_primary_addr(cli.replica_of.clone());
        state.set_replica_lag(Some(Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX))));
    }
    let state = Arc::new(state);

    // Seed demo data if --seed flag is present and graph is empty
    if seed {
        seed_demo_data(&state);
    }

    // Initialize history provider
    if let Some(vs_svc) = state
        .services()
        .get::<selene_server::version_store::VersionStoreService>()
    {
        selene_server::history::init_history_provider_with_versions(
            Arc::clone(state.changelog()),
            Arc::clone(&vs_svc.store),
        );
    } else {
        selene_server::history::init_history_provider(Arc::clone(state.changelog()));
    }

    // Initialize TS history provider for cold tier Parquet queries
    {
        let ts_dir = state.config().data_dir.join("ts");
        let provider = selene_server::ts_history::ParquetTsHistoryProvider::new(ts_dir);
        selene_gql::runtime::procedures::ts_history_provider::set_ts_history_provider(Arc::new(
            provider,
        ));
    }

    let cancel = tokio_util::sync::CancellationToken::new();
    let bg = tasks::spawn_background_tasks(Arc::clone(&state), cancel);

    // stdio mode: serve MCP over stdin/stdout, skip HTTP/QUIC
    if cli.stdio {
        state.set_ready();
        tracing::info!("serving MCP over stdio (stdin/stdout)");

        if let Err(e) = selene_server::serve_stdio_mcp(Arc::clone(&state)).await {
            tracing::error!("stdio MCP server error: {e}");
        }

        tracing::info!("stdio client disconnected, shutting down");
        bg.shutdown();
        tasks::shutdown_snapshot(&state);
        bg.wait().await;
        return Ok(());
    }

    // Bootstrap federation peers
    if let Some(fed_svc) = state
        .services()
        .get::<selene_server::federation::FederationService>()
    {
        let mgr = Arc::clone(&fed_svc.manager);
        tokio::spawn(async move {
            mgr.bootstrap().await;
        });

        let mgr = Arc::clone(&fed_svc.manager);
        let refresh_secs = state.config().federation.refresh_interval_secs;
        let cancel = bg.cancel.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(refresh_secs));
            interval.tick().await;
            loop {
                tokio::select! {
                    _ = interval.tick() => { mgr.prune(); }
                    _ = cancel.cancelled() => { return; }
                }
            }
        });
    }

    // Replica replication task
    if let Some(ref primary_addr) = cli.replica_of {
        let replica_state = Arc::clone(&state);
        let addr = primary_addr.clone();
        let token = bg.cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = selene_server::replica::run_replica(replica_state, &addr, token).await {
                tracing::error!("replica replication failed: {e}");
            }
        });
        tracing::info!(primary = %primary_addr, "replica mode — mutations disabled");
    }

    // Configure QUIC TLS
    let (server_config, _certs) = if state.config().dev_mode {
        #[cfg(feature = "dev-tls")]
        {
            tls::dev_server_config()?
        }
        #[cfg(not(feature = "dev-tls"))]
        {
            anyhow::bail!(
                "--dev mode requires the `dev-tls` feature (compile with --features dev-tls)"
            )
        }
    } else {
        let tls_config = state
            .config()
            .tls
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("production mode requires TLS configuration"))?;
        tls::prod_server_config(tls_config)?
    };

    // QUIC listener (always on)
    let quic_state = Arc::clone(&state);
    let quic_handle = tokio::spawn(async move {
        if let Err(e) = listener::serve(quic_state, server_config).await {
            tracing::error!("QUIC listener error: {e}");
        }
    });

    // HTTP listener (runtime config toggle, with graceful shutdown)
    let http_handle = if state.config().http.enabled {
        let http_state = Arc::clone(&state);
        let http_cancel = bg.cancel.clone();
        Some(tokio::spawn(async move {
            if let Err(e) = selene_server::http::serve(http_state, Some(http_cancel)).await {
                tracing::error!("HTTP listener error: {e}");
            }
        }))
    } else {
        None
    };

    // All listeners and background tasks are running.
    state.set_ready();
    tracing::info!("server ready");

    // Wait for shutdown signal or listener exit
    let http_future = async {
        match http_handle {
            Some(h) => h.await,
            None => std::future::pending().await,
        }
    };

    // Register SIGTERM handler for graceful shutdown
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received SIGINT, shutting down");
        }
        _ = sigterm.recv() => {
            tracing::info!("received SIGTERM, shutting down");
        }
        r = quic_handle => {
            tracing::error!("QUIC listener exited: {r:?}");
        }
        r = http_future => {
            tracing::error!("HTTP listener exited: {r:?}");
        }
    }

    // Graceful shutdown
    tracing::info!("shutting down...");
    bg.shutdown();
    tasks::shutdown_snapshot(&state);
    bg.wait().await;
    tracing::info!("shutdown complete");

    Ok(())
}

/// Seed the graph with demo data for development and testing.
fn seed_demo_data(state: &selene_server::ServerState) {
    use selene_server::auth::handshake::AuthContext;
    let auth = AuthContext::dev_admin();

    // Check if already seeded
    let count = state.graph().read(|g| g.node_count());
    if count > 0 {
        tracing::info!(
            existing_nodes = count,
            "graph already has data, skipping seed"
        );
        return;
    }

    tracing::info!("seeding demo data...");

    // Import common schema pack
    if let Some(pack) = selene_packs::builtin("common") {
        let _ = ops::schema::import_pack(state, &auth, pack);
    }

    // Import Brick schema pack
    if let Some(pack) = selene_packs::builtin("brick") {
        let _ = ops::schema::import_pack(state, &auth, pack);
    }

    // Create building hierarchy
    let labels =
        |names: &[&str]| -> selene_core::LabelSet { selene_core::LabelSet::from_strs(names) };
    let props = |pairs: &[(&str, selene_core::Value)]| -> selene_core::PropertyMap {
        selene_core::PropertyMap::from_pairs(
            pairs
                .iter()
                .map(|(k, v)| (selene_core::IStr::new(k), v.clone())),
        )
    };
    let s = |v: &str| selene_core::Value::str(v);
    let f = |v: f64| selene_core::Value::Float(v);
    let i = |v: i64| selene_core::Value::Int(v);

    // Site
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["site"]),
        props(&[("name", s("Demo Campus")), ("city", s("Austin"))]),
        None,
    );
    // Building
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["building"]),
        props(&[("name", s("Headquarters")), ("area_sqft", f(50000.0))]),
        Some(1),
    );
    // Floors
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["floor"]),
        props(&[("name", s("Floor 1")), ("level", i(1))]),
        Some(2),
    );
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["floor"]),
        props(&[("name", s("Floor 2")), ("level", i(2))]),
        Some(2),
    );
    // Zones
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["zone"]),
        props(&[("name", s("Zone A")), ("zone_type", s("thermal"))]),
        Some(3),
    );
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["zone"]),
        props(&[("name", s("Zone B")), ("zone_type", s("thermal"))]),
        Some(4),
    );
    // Equipment
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["ahu", "equipment"]),
        props(&[("name", s("AHU-1")), ("supply_air_flow_cfm", f(10000.0))]),
        Some(2),
    );
    // Sensors
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["temperature_sensor", "point"]),
        props(&[("name", s("Zone-A Temp")), ("unit", s("°F"))]),
        Some(5),
    );
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["temperature_sensor", "point"]),
        props(&[("name", s("Zone-B Temp")), ("unit", s("°F"))]),
        Some(6),
    );
    let _ = ops::nodes::create_node(
        state,
        &auth,
        labels(&["humidity_sensor", "point"]),
        props(&[("name", s("Zone-A Humidity")), ("unit", s("%RH"))]),
        Some(5),
    );

    // Equipment-to-zone edges
    let _ = ops::edges::create_edge(
        state,
        &auth,
        7,
        5,
        selene_core::IStr::new("feeds"),
        selene_core::PropertyMap::new(),
    );
    let _ = ops::edges::create_edge(
        state,
        &auth,
        7,
        6,
        selene_core::IStr::new("feeds"),
        selene_core::PropertyMap::new(),
    );

    // Sensor-to-equipment edges
    let _ = ops::edges::create_edge(
        state,
        &auth,
        8,
        7,
        selene_core::IStr::new("isPointOf"),
        selene_core::PropertyMap::new(),
    );
    let _ = ops::edges::create_edge(
        state,
        &auth,
        9,
        7,
        selene_core::IStr::new("isPointOf"),
        selene_core::PropertyMap::new(),
    );
    let _ = ops::edges::create_edge(
        state,
        &auth,
        10,
        7,
        selene_core::IStr::new("isPointOf"),
        selene_core::PropertyMap::new(),
    );

    // Write some time-series data
    let now = selene_core::now_nanos();
    let mut samples = Vec::new();
    for i in 0..60 {
        let ts = now - (60 - i) * 60_000_000_000; // last 60 minutes
        samples.push(selene_wire::dto::ts::TsSampleDto {
            entity_id: 8,
            property: "temperature".into(),
            timestamp_nanos: ts,
            value: 72.0 + (i as f64 * 0.05),
        });
        samples.push(selene_wire::dto::ts::TsSampleDto {
            entity_id: 9,
            property: "temperature".into(),
            timestamp_nanos: ts,
            value: 71.0 + (i as f64 * 0.03),
        });
        samples.push(selene_wire::dto::ts::TsSampleDto {
            entity_id: 10,
            property: "humidity".into(),
            timestamp_nanos: ts,
            value: 45.0 + (i as f64 * 0.1),
        });
    }
    let _ = ops::ts::ts_write(state, &auth, samples);

    let node_count = state.graph().read(|g| g.node_count());
    let edge_count = state.graph().read(|g| g.edge_count());
    tracing::info!(
        nodes = node_count,
        edges = edge_count,
        "demo data seeded: site > building > 2 floors > 2 zones > AHU + 3 sensors, 180 TS samples"
    );
}
