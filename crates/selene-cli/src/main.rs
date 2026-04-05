#![forbid(unsafe_code)]
//! CLI tool for interacting with a Selene server.
//!
//! ```text
//! selene health
//! selene node create --labels sensor,temperature --props '{"unit":"°F"}'
//! selene node get 1
//! selene gql "MATCH (n) RETURN count(*) AS cnt"
//! selene shell
//! ```

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod formatters;

use anyhow::Result;
use clap::{Parser, Subcommand};
use selene_client::{AuthCredentials, ClientConfig, ClientTlsConfig, SeleneClient};

use formatters::{
    CsvFormatter, JsonFormatter, ResultFormatter, TableFormatter, parse_gql_json_data,
};

#[derive(Parser)]
#[command(name = "selene", about = "Selene graph database CLI")]
struct Cli {
    /// Server address (default: 127.0.0.1:4510)
    #[arg(long, default_value = "127.0.0.1:4510")]
    server: String,

    /// Server name for TLS (default: localhost)
    #[arg(long, default_value = "localhost")]
    server_name: String,

    /// Skip TLS certificate verification (dev mode only).
    #[arg(long)]
    insecure: bool,

    /// Path to PEM-encoded CA certificate for server verification.
    #[arg(long)]
    tls_ca: Option<std::path::PathBuf>,

    /// Path to PEM-encoded client certificate for mTLS.
    #[arg(long)]
    tls_cert: Option<std::path::PathBuf>,

    /// Path to PEM-encoded client private key for mTLS.
    #[arg(long)]
    tls_key: Option<std::path::PathBuf>,

    /// Auth type: dev, token, psk.
    #[arg(long)]
    auth_type: Option<String>,

    /// Principal identity (username).
    #[arg(long)]
    auth_identity: Option<String>,

    /// Auth credential (password/token).
    #[arg(long)]
    auth_secret: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Health check (QUIC by default, --http for HTTP probe).
    Health {
        /// Use HTTP health endpoint instead of QUIC.
        /// Useful for Docker/K8s health checks without TLS setup.
        #[arg(long)]
        http: bool,
        /// HTTP address to probe (default: 127.0.0.1:8080).
        #[arg(long, default_value = "127.0.0.1:8080")]
        http_addr: String,
    },

    /// Node operations
    Node {
        #[command(subcommand)]
        action: NodeAction,
    },

    /// Edge operations
    Edge {
        #[command(subcommand)]
        action: EdgeAction,
    },

    /// Time-series operations
    Ts {
        #[command(subcommand)]
        action: TsAction,
    },

    /// Execute a GQL query (primary query interface)
    Gql {
        /// GQL query string (optional if --file is provided)
        query: Option<String>,
        /// Show execution plan without executing
        #[arg(long)]
        explain: bool,
        /// Show execution plan with per-operator timing
        #[arg(long)]
        profile: bool,
        /// Execute from file instead of argument
        #[arg(long)]
        file: Option<std::path::PathBuf>,
        /// Output format: table, json, csv
        #[arg(long, default_value = "json")]
        format: String,
    },

    /// Interactive GQL REPL with history
    Shell {
        /// Output format: table, json, csv
        #[arg(long, default_value = "table")]
        format: String,
    },

    /// Analyze system and recommend configuration tuning.
    Tune {
        /// Resource profile: conservative, balanced, greedy
        #[arg(long, default_value = "balanced")]
        profile: String,
        /// Deployment scope: dedicated (Selene owns the machine) or embedded (shared resources)
        #[arg(long, default_value = "dedicated")]
        scope: String,
    },

    /// Generate a 256-bit master key for the secure vault
    Keygen {
        /// Output file (default: print to stdout)
        #[arg(long)]
        output: Option<std::path::PathBuf>,
        /// Output format: base64 (default) or hex
        #[arg(long, default_value = "base64")]
        format: String,
    },

    /// Get a graph slice
    Slice {
        /// Slice type: full, labels, containment
        #[arg(long, default_value = "full")]
        slice_type: String,
        /// Labels for label-based slice (comma-separated)
        #[arg(long)]
        labels: Option<String>,
        /// Root node ID for containment slice
        #[arg(long)]
        root: Option<u64>,
        /// Max depth for containment slice
        #[arg(long)]
        depth: Option<u32>,
    },
}

#[derive(Subcommand)]
enum NodeAction {
    /// Create a new node
    Create {
        /// Comma-separated labels
        #[arg(long)]
        labels: String,
        /// JSON properties (optional)
        #[arg(long, default_value = "{}")]
        props: String,
    },
    /// Get a node by ID
    Get {
        /// Node ID
        id: u64,
    },
    /// List nodes
    List {
        /// Filter by label
        #[arg(long)]
        label: Option<String>,
        /// Max results
        #[arg(long, default_value = "100")]
        limit: u64,
    },
    /// Modify a node
    Modify {
        /// Node ID
        id: u64,
        /// JSON properties to set
        #[arg(long, default_value = "{}")]
        set: String,
        /// Comma-separated property keys to remove
        #[arg(long, default_value = "")]
        remove_props: String,
        /// Comma-separated labels to add
        #[arg(long, default_value = "")]
        add_labels: String,
        /// Comma-separated labels to remove
        #[arg(long, default_value = "")]
        remove_labels: String,
    },
    /// Delete a node
    Delete {
        /// Node ID
        id: u64,
    },
}

#[derive(Subcommand)]
enum TsAction {
    /// Write a time-series sample
    Write {
        /// Entity ID
        entity_id: u64,
        /// Property name
        property: String,
        /// Value
        value: f64,
    },
    /// Query time-series range
    Query {
        /// Entity ID
        entity_id: u64,
        /// Property name
        property: String,
        /// Start timestamp (nanos, default: 0)
        #[arg(long, default_value = "0")]
        start: i64,
        /// End timestamp (nanos, default: max)
        #[arg(long, default_value = "9223372036854775807")]
        end: i64,
        /// Maximum number of results to return
        #[arg(long)]
        limit: Option<u64>,
    },
}

#[derive(Subcommand)]
enum EdgeAction {
    /// Create an edge between two nodes
    Create {
        /// Source node ID
        #[arg(long)]
        source: u64,
        /// Target node ID
        #[arg(long)]
        target: u64,
        /// Edge label
        #[arg(long)]
        label: String,
        /// JSON properties (optional)
        #[arg(long, default_value = "{}")]
        props: String,
    },
    /// Get an edge by ID
    Get {
        /// Edge ID
        id: u64,
    },
    /// List edges
    List {
        /// Filter by label
        #[arg(long)]
        label: Option<String>,
        /// Max results
        #[arg(long, default_value = "100")]
        limit: u64,
    },
    /// Delete an edge
    Delete {
        /// Edge ID
        id: u64,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Commands that run locally without a server connection.
    if let Commands::Health {
        http: true,
        ref http_addr,
    } = cli.command
    {
        let body = http_health_check(http_addr).await?;
        println!("{body}");
        return Ok(());
    }

    if let Commands::Keygen {
        ref output,
        ref format,
    } = cli.command
    {
        let mut key = [0u8; 32];
        fill_random(&mut key);

        let encoded = match format.as_str() {
            "hex" => hex_encode(&key),
            _ => encode_base64_key(&key),
        };

        if let Some(path) = output {
            std::fs::write(path, format!("{encoded}\n"))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
            println!("Master key written to {}", path.display());
        } else {
            println!("{encoded}");
        }
        key.fill(0);
        return Ok(());
    }

    if let Commands::Tune {
        ref profile,
        ref scope,
    } = cli.command
    {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        let total_mem_mb = detect_system_memory_mb();

        let (resource_label, cpu_fraction, mem_fraction) = match profile.as_str() {
            "conservative" => ("Conservative", 0.25, 0.25),
            "greedy" => ("Greedy", 0.90, 0.80),
            _ => ("Balanced", 0.50, 0.50),
        };

        let scope_factor = match scope.as_str() {
            "embedded" => 0.5,
            _ => 1.0,
        };

        let rayon_threads = ((cores as f64 * cpu_fraction * scope_factor).ceil() as usize).max(1);
        let memory_budget_mb =
            ((total_mem_mb as f64 * mem_fraction * scope_factor) as usize).max(64);
        let max_concurrent = (rayon_threads * 4).clamp(4, 256);
        let query_timeout_ms = match profile.as_str() {
            "conservative" => 15_000u64,
            "greedy" => 120_000,
            _ => 30_000,
        };

        println!("Selene Tune - {resource_label} profile, {scope} scope");
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!();
        println!("System:");
        println!("  CPU cores:        {cores}");
        println!("  Total memory:     {total_mem_mb} MB");
        println!();
        println!("Recommendations:");
        println!("  [PERF] rayon_threads:         {rayon_threads}");
        println!("  [PERF] memory_budget_mb:      {memory_budget_mb}");
        println!("  [PERF] max_concurrent_queries: {max_concurrent}");
        println!("  [SAFE] query_timeout_ms:      {query_timeout_ms}");
        println!();
        println!("TOML config:");
        println!("  [performance]");
        println!("  rayon_threads = {rayon_threads}");
        println!("  memory_budget_mb = {memory_budget_mb}");
        println!("  max_concurrent_queries = {max_concurrent}");
        println!("  query_timeout_ms = {query_timeout_ms}");
        return Ok(());
    }

    let config = validate_and_build_config(
        &cli.server,
        cli.server_name,
        cli.insecure,
        cli.tls_ca,
        cli.tls_cert,
        cli.tls_key,
        cli.auth_type,
        cli.auth_identity,
        cli.auth_secret,
    )?;

    let client = SeleneClient::connect(&config).await?;

    match cli.command {
        Commands::Health { .. } => {
            let health = client.health().await?;
            println!("{}", serde_json::to_string_pretty(&health)?);
        }

        Commands::Node { action } => match action {
            NodeAction::Create { labels, props } => {
                let label_str = labels
                    .split(',')
                    .map(|s| s.trim())
                    .collect::<Vec<_>>()
                    .join(":");
                let prop_pairs = parse_json_props(&props)?;
                let prop_str = format_gql_props(&prop_pairs);
                let query = format!("INSERT (:{label_str} {prop_str})");
                print_gql(&client.gql(&query).await?);
            }
            NodeAction::Get { id } => {
                let query = format!(
                    "MATCH (n) FILTER id(n) = {id}u RETURN id(n) AS id, labels(n) AS labels"
                );
                print_gql(&client.gql(&query).await?);
            }
            NodeAction::List { label, limit } => {
                let query = if let Some(l) = label {
                    format!("MATCH (n:{l}) RETURN id(n) AS id, labels(n) AS labels LIMIT {limit}")
                } else {
                    format!("MATCH (n) RETURN id(n) AS id, labels(n) AS labels LIMIT {limit}")
                };
                print_gql(&client.gql(&query).await?);
            }
            NodeAction::Modify {
                id,
                set,
                remove_props,
                add_labels,
                remove_labels,
            } => {
                let mut parts = Vec::new();
                let prop_pairs = parse_json_props(&set)?;
                for (key, val) in &prop_pairs {
                    let val_str = format_gql_value(val);
                    parts.push(format!("SET n.{key} = {val_str}"));
                }
                for key in remove_props
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                {
                    parts.push(format!("REMOVE n.{key}"));
                }
                for label in add_labels
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                {
                    parts.push(format!("SET n:{label}"));
                }
                for label in remove_labels
                    .split(',')
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
                {
                    parts.push(format!("REMOVE n:{label}"));
                }
                if parts.is_empty() {
                    eprintln!("No modifications specified");
                } else {
                    let mutations = parts.join(" ");
                    let query = format!("MATCH (n) FILTER id(n) = {id}u {mutations}");
                    print_gql(&client.gql(&query).await?);
                }
            }
            NodeAction::Delete { id } => {
                let query = format!("MATCH (n) FILTER id(n) = {id}u DETACH DELETE n");
                print_gql(&client.gql(&query).await?);
            }
        },

        Commands::Edge { action } => match action {
            EdgeAction::Create {
                source,
                target,
                label,
                props,
            } => {
                let prop_pairs = parse_json_props(&props)?;
                let prop_str = format_gql_props(&prop_pairs);
                let query = format!(
                    "MATCH (s), (t) FILTER id(s) = {source}u AND id(t) = {target}u INSERT (s)-[:{label} {prop_str}]->(t)"
                );
                print_gql(&client.gql(&query).await?);
            }
            EdgeAction::Get { id } => {
                let query = format!(
                    "MATCH (s)-[e]->(t) FILTER id(e) = {id}u RETURN id(e) AS id, type(e) AS label, id(s) AS source, id(t) AS target"
                );
                print_gql(&client.gql(&query).await?);
            }
            EdgeAction::List { label, limit } => {
                let query = if let Some(l) = label {
                    format!(
                        "MATCH (s)-[e:{l}]->(t) RETURN id(e) AS id, type(e) AS label, id(s) AS source, id(t) AS target LIMIT {limit}"
                    )
                } else {
                    format!(
                        "MATCH (s)-[e]->(t) RETURN id(e) AS id, type(e) AS label, id(s) AS source, id(t) AS target LIMIT {limit}"
                    )
                };
                print_gql(&client.gql(&query).await?);
            }
            EdgeAction::Delete { id } => {
                let query = format!("MATCH ()-[e]->() FILTER id(e) = {id}u DELETE e");
                print_gql(&client.gql(&query).await?);
            }
        },

        Commands::Ts { action } => match action {
            TsAction::Write {
                entity_id,
                property,
                value,
            } => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64;
                let sample = selene_wire::dto::ts::TsSampleDto {
                    entity_id,
                    property,
                    timestamp_nanos: now,
                    value,
                };
                let count = client.ts_write(vec![sample]).await?;
                println!("Wrote {count} sample(s)");
            }
            TsAction::Query {
                entity_id,
                property,
                start,
                end,
                limit,
            } => {
                let samples = client
                    .ts_range(entity_id, &property, start, end, limit)
                    .await?;
                println!("{}", serde_json::to_string_pretty(&samples)?);
            }
        },

        Commands::Gql {
            query,
            explain,
            profile,
            file,
            format,
        } => {
            let query_text = if let Some(path) = file {
                std::fs::read_to_string(&path)?
            } else if let Some(q) = query {
                q
            } else {
                anyhow::bail!("provide a query string or --file");
            };

            if explain || profile {
                let result = client
                    .gql_explain_with_profile(&query_text, profile)
                    .await?;
                if let Some(plan) = &result.plan {
                    println!("{plan}");
                } else {
                    println!("Status: {} - {}", result.status_code, result.message);
                }
            } else {
                let (result, data) = client.gql_with_data(&query_text).await?;
                display_gql_result(&result, &data, &format, 0);
            }
        }

        Commands::Shell { format } => {
            use rustyline::DefaultEditor;

            let mut rl = DefaultEditor::new()?;
            let history_path = dirs_home().join(".selene_history");
            let _ = rl.load_history(&history_path);

            println!("Selene GQL Shell - type .help for commands, .exit to quit");
            let mut explain_mode = false;
            let mut current_format = format;
            let row_limit = 100usize;

            loop {
                let prompt = if explain_mode {
                    "selene [explain]> "
                } else {
                    "selene> "
                };
                match rl.readline(prompt) {
                    Ok(line) => {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        let _ = rl.add_history_entry(trimmed);

                        match trimmed {
                            ".exit" | ".quit" => break,
                            ".help" => {
                                println!("Commands:");
                                println!("  .exit              Exit the shell");
                                println!("  .explain on/off    Toggle EXPLAIN mode");
                                println!("  .format table|json|csv  Change output format");
                                println!("  .clear             Clear screen");
                                println!("  .help              Show this help");
                                println!();
                                println!("Enter any GQL query to execute it.");
                                continue;
                            }
                            ".explain on" => {
                                explain_mode = true;
                                println!("EXPLAIN mode ON");
                                continue;
                            }
                            ".explain off" => {
                                explain_mode = false;
                                println!("EXPLAIN mode OFF");
                                continue;
                            }
                            ".format table" | ".format json" | ".format csv" => {
                                current_format = trimmed[8..].to_string();
                                println!("Format: {current_format}");
                                continue;
                            }
                            ".clear" => {
                                print!("\x1B[2J\x1B[1;1H");
                                continue;
                            }
                            _ => {}
                        }

                        if explain_mode {
                            match client.gql_explain(trimmed).await {
                                Ok(r) => {
                                    if let Some(plan) = &r.plan {
                                        println!("{plan}");
                                    }
                                }
                                Err(e) => eprintln!("Error: {e}"),
                            }
                        } else {
                            match client.gql_with_data(trimmed).await {
                                Ok((r, data)) => {
                                    display_gql_result(&r, &data, &current_format, row_limit);
                                }
                                Err(e) => eprintln!("Error: {e}"),
                            }
                        }
                    }
                    Err(rustyline::error::ReadlineError::Interrupted) => {
                        println!("^C");
                    }
                    Err(rustyline::error::ReadlineError::Eof) => break,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        break;
                    }
                }
            }

            let _ = rl.save_history(&history_path);
        }

        // Tune and Keygen are handled above (early return, no server connection).
        Commands::Tune { .. } | Commands::Keygen { .. } => unreachable!(),

        Commands::Slice {
            slice_type,
            labels,
            root,
            depth,
        } => {
            let result = match slice_type.as_str() {
                "full" => client.graph_slice_full().await?,
                "labels" => {
                    let labels: Vec<String> = labels
                        .unwrap_or_default()
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    client.graph_slice_by_labels(labels).await?
                }
                "containment" => {
                    let root_id = root.unwrap_or(1);
                    client.graph_slice_containment(root_id, depth).await?
                }
                other => {
                    anyhow::bail!("unknown slice type: {other} (use full, labels, containment)");
                }
            };
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }

    Ok(())
}

/// Return the user's home directory, falling back to the current directory.
fn dirs_home() -> std::path::PathBuf {
    std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .map_or_else(|_| std::path::PathBuf::from("."), std::path::PathBuf::from)
}

/// Detect total system memory in MB. Falls back to 4096 MB if detection fails.
fn detect_system_memory_mb() -> usize {
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            && let Ok(s) = String::from_utf8(output.stdout)
            && let Ok(bytes) = s.trim().parse::<u64>()
        {
            return (bytes / (1024 * 1024)) as usize;
        }
    }

    #[cfg(target_os = "linux")]
    {
        if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
            for line in content.lines() {
                if line.starts_with("MemTotal:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if let Some(kb) = parts.get(1).and_then(|s| s.parse::<u64>().ok()) {
                        return (kb / 1024) as usize;
                    }
                }
            }
        }
    }

    4096
}

/// HTTP GET health check using raw TCP (zero extra dependencies).
/// Returns the JSON response body. Designed for Docker HEALTHCHECK /
/// Kubernetes livenessProbe.
async fn http_health_check(addr: &str) -> Result<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    const HEALTH_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
    const MAX_RESPONSE_BYTES: u64 = 1_048_576; // 1 MB

    let mut stream = tokio::time::timeout(HEALTH_CHECK_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| anyhow::anyhow!("health check connect timed out after 5s"))?
        .map_err(|e| anyhow::anyhow!("cannot connect to {addr}: {e}"))?;

    let host = addr.split(':').next().unwrap_or("localhost");
    let request = format!("GET /health HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;

    let mut buf = Vec::with_capacity(1024);
    tokio::time::timeout(
        HEALTH_CHECK_TIMEOUT,
        (&mut stream).take(MAX_RESPONSE_BYTES).read_to_end(&mut buf),
    )
    .await
    .map_err(|_| anyhow::anyhow!("health check read timed out after 5s"))??;
    let response = String::from_utf8_lossy(&buf);

    let status_line = response.lines().next().unwrap_or("");
    let status_code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);

    if status_code != 200 {
        anyhow::bail!("health check failed: {status_line}");
    }

    if let Some(pos) = response.find("\r\n\r\n") {
        Ok(response[pos + 4..].to_string())
    } else {
        Ok(response.to_string())
    }
}

/// Display a GQL query result with error handling, mutation stats, and formatted data.
/// Shared by the `gql` subcommand and interactive `shell`.
fn display_gql_result(
    result: &selene_wire::dto::gql::GqlResultResponse,
    data: &str,
    format: &str,
    row_limit: usize,
) {
    if result.status_code != "00000" && result.status_code != "02000" {
        println!("Error [{}]: {}", result.status_code, result.message);
        return;
    }
    if let Some(m) = &result.mutations
        && (m.nodes_created > 0
            || m.nodes_deleted > 0
            || m.edges_created > 0
            || m.edges_deleted > 0)
    {
        println!(
            "Mutations: +{} nodes, -{} nodes, +{} edges, -{} edges",
            m.nodes_created, m.nodes_deleted, m.edges_created, m.edges_deleted
        );
    }
    let (columns, rows) = parse_gql_json_data(data);
    if !columns.is_empty() {
        let formatter = make_formatter_with_limit(format, row_limit);
        print!("{}", formatter.format(&columns, &rows));
    }
    println!("{} rows", result.row_count);
}

fn print_gql(result: &selene_wire::dto::gql::GqlResultResponse) {
    println!("Status: {} - {}", result.status_code, result.message);
    if let Some(m) = &result.mutations
        && (m.nodes_created > 0
            || m.nodes_deleted > 0
            || m.edges_created > 0
            || m.edges_deleted > 0)
    {
        println!(
            "Mutations: +{} nodes, -{} nodes, +{} edges, -{} edges",
            m.nodes_created, m.nodes_deleted, m.edges_created, m.edges_deleted
        );
    }
    println!("{} rows", result.row_count);
}

fn parse_json_props(json: &str) -> Result<Vec<(String, serde_json::Value)>> {
    let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(json)?;
    Ok(map.into_iter().collect())
}

fn format_gql_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => {
            let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
            format!("'{escaped}'")
        }
        other => format!("{other}"),
    }
}

fn format_gql_props(pairs: &[(String, serde_json::Value)]) -> String {
    if pairs.is_empty() {
        return String::new();
    }
    let props: Vec<String> = pairs
        .iter()
        .map(|(k, v)| format!("{k}: {}", format_gql_value(v)))
        .collect();
    format!("{{{}}}", props.join(", "))
}

/// Fill buffer with OS-provided cryptographic random bytes via getrandom(2).
/// Panics if the OS entropy source is unavailable.
fn fill_random(buf: &mut [u8]) {
    getrandom::fill(buf)
        .expect("OS entropy source unavailable - cannot generate cryptographic key");
}

/// Encode 32 bytes as base64.
fn encode_base64_key(data: &[u8; 32]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Encode bytes as lowercase hex.
fn hex_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(data.len() * 2);
    for b in data {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Create a formatter from a format string with a row limit (0 = unlimited).
fn make_formatter_with_limit(format: &str, row_limit: usize) -> Box<dyn ResultFormatter> {
    let width = terminal_width();
    match format {
        "json" => Box::new(JsonFormatter),
        "csv" => Box::new(CsvFormatter),
        _ => Box::new(TableFormatter::new(width, row_limit)),
    }
}

/// Detect terminal width, defaulting to 120 if unavailable.
fn terminal_width() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120)
}

/// Validate CLI flag combinations and build a `ClientConfig`.
///
/// Enforces:
/// - Exactly one of `--insecure` or `--tls-ca` (no system CA fallback).
/// - `--tls-cert` and `--tls-key` must be provided together.
/// - mTLS flags are incompatible with `--insecure`.
/// - Auth flags must all be provided or all omitted.
#[allow(clippy::too_many_arguments)]
fn validate_and_build_config(
    server: &str,
    server_name: String,
    insecure: bool,
    tls_ca: Option<std::path::PathBuf>,
    tls_cert: Option<std::path::PathBuf>,
    tls_key: Option<std::path::PathBuf>,
    auth_type: Option<String>,
    auth_identity: Option<String>,
    auth_secret: Option<String>,
) -> Result<ClientConfig> {
    // Rule 1: Exactly one of --insecure or --tls-ca.
    if insecure && tls_ca.is_some() {
        anyhow::bail!("--insecure and --tls-ca are mutually exclusive");
    }
    if !insecure && tls_ca.is_none() {
        anyhow::bail!("specify --insecure for dev mode or --tls-ca <path> for production TLS");
    }

    // Rule 2: mTLS cert and key must be paired.
    if tls_cert.is_some() != tls_key.is_some() {
        anyhow::bail!("--tls-cert and --tls-key must be provided together");
    }

    // Rule 3: mTLS is incompatible with --insecure.
    if insecure && tls_cert.is_some() {
        anyhow::bail!("--tls-cert/--tls-key require --tls-ca (incompatible with --insecure)");
    }

    // Rule 4: Auth flags must all be provided or all omitted.
    let has_any_auth = auth_type.is_some() || auth_identity.is_some() || auth_secret.is_some();
    if has_any_auth && (auth_type.is_none() || auth_identity.is_none() || auth_secret.is_none()) {
        anyhow::bail!(
            "--auth-type, --auth-identity, and --auth-secret must all be provided together"
        );
    }

    let tls = if insecure {
        None
    } else {
        Some(ClientTlsConfig {
            ca_cert_path: tls_ca.unwrap(),
            cert_path: tls_cert,
            key_path: tls_key,
        })
    };

    let auth = auth_type.map(|at| AuthCredentials {
        auth_type: at,
        identity: auth_identity.unwrap(),
        credentials: auth_secret.unwrap(),
    });

    Ok(ClientConfig {
        server_addr: server.parse()?,
        server_name,
        insecure,
        tls,
        auth,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insecure_mode_builds_config() {
        let config = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            true,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(config.insecure);
        assert!(config.tls.is_none());
        assert!(config.auth.is_none());
    }

    #[test]
    fn tls_ca_mode_builds_config() {
        let config = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            false,
            Some("ca.pem".into()),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(!config.insecure);
        let tls = config.tls.as_ref().unwrap();
        assert_eq!(tls.ca_cert_path, std::path::PathBuf::from("ca.pem"));
        assert!(tls.cert_path.is_none());
        assert!(tls.key_path.is_none());
    }

    #[test]
    fn mtls_builds_config() {
        let config = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            false,
            Some("ca.pem".into()),
            Some("cert.pem".into()),
            Some("key.pem".into()),
            None,
            None,
            None,
        )
        .unwrap();
        let tls = config.tls.as_ref().unwrap();
        assert_eq!(
            tls.cert_path.as_deref(),
            Some(std::path::Path::new("cert.pem"))
        );
        assert_eq!(
            tls.key_path.as_deref(),
            Some(std::path::Path::new("key.pem"))
        );
    }

    #[test]
    fn auth_builds_config() {
        let config = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            true,
            None,
            None,
            None,
            Some("dev".into()),
            Some("admin".into()),
            Some("secret".into()),
        )
        .unwrap();
        let auth = config.auth.as_ref().unwrap();
        assert_eq!(auth.auth_type, "dev");
        assert_eq!(auth.identity, "admin");
    }

    #[test]
    fn no_insecure_no_tls_ca_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("--insecure"), "got: {err}");
    }

    #[test]
    fn insecure_and_tls_ca_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            true,
            Some("ca.pem".into()),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("mutually exclusive"), "got: {err}");
    }

    #[test]
    fn mtls_cert_without_key_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            false,
            Some("ca.pem".into()),
            Some("cert.pem".into()),
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("together"), "got: {err}");
    }

    #[test]
    fn mtls_key_without_cert_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            false,
            Some("ca.pem".into()),
            None,
            Some("key.pem".into()),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("together"), "got: {err}");
    }

    #[test]
    fn mtls_with_insecure_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            true,
            None,
            Some("cert.pem".into()),
            Some("key.pem".into()),
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("incompatible"), "got: {err}");
    }

    #[test]
    fn auth_type_without_identity_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            true,
            None,
            None,
            None,
            Some("dev".into()),
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("together"), "got: {err}");
    }

    #[test]
    fn auth_secret_alone_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            true,
            None,
            None,
            None,
            None,
            None,
            Some("secret".into()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("together"), "got: {err}");
    }

    #[test]
    fn auth_identity_without_type_errors() {
        let err = validate_and_build_config(
            "127.0.0.1:4510",
            "localhost".into(),
            true,
            None,
            None,
            None,
            None,
            Some("admin".into()),
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("together"), "got: {err}");
    }
}
