# Industry Applications

Selene combines a property graph, time-series engine, and AI agent interface in a
single binary that runs at the edge. This combination addresses problems that
traditionally require three or more separate systems, and it does so on hardware as
small as a Raspberry Pi 5.

This document maps Selene's capabilities to the industries and applications where
that combination solves real problems today.

## What Makes Selene Different

Most databases solve one problem well. Graph databases model relationships. Time-series
databases store telemetry. Vector databases power search. Selene combines all three in
a 14 MB Docker image with sub-millisecond read latency, and exposes the result to AI
agents through the Model Context Protocol.

| Capability | Neo4j | InfluxDB | TimescaleDB | Azure Digital Twins | Selene |
|---|---|---|---|---|---|
| Property graph | Yes | No | No | Yes (DTDL) | Yes |
| Time-series | No | Yes | Yes | Via ADX | Yes |
| Edge deployment (<20 MB) | No | No | No | No | Yes |
| MCP native (36 tools) | No | No | No | No | Yes |
| RDF/Brick/SOSA | Plugin | No | No | Partial | Yes |
| Vector search | Yes | No | Yes | No | Yes |
| Offline-first + federation | No | No | No | No | Yes |
| ISO GQL | Planned | N/A | N/A | N/A | Yes |
| Graph algorithms | Yes | No | No | No | Yes |

No other product combines all of these capabilities in a single runtime.

---

## Tier 1: Strong Natural Fit

### Smart Buildings and Building Management

**Fit: Very High. This is Selene's primary design target.**

Buildings generate two kinds of data that belong together: relationship data (what
connects to what) and time-series telemetry (sensor readings, energy consumption). An
AHU serves a set of zones, those zones contain temperature sensors, and those sensors
produce readings every 15 seconds. Today this requires a BMS for control, a separate
database for history, a spreadsheet for the asset register, and often a cloud service
for analytics.

**Industry pain points Selene addresses:**

- **Data fragmentation.** Building data sits in BMS controllers, CAFM systems, energy
  portals, and vendor dashboards. Half of it does not line up. Selene's property graph
  models the full equipment hierarchy (campus, building, floor, zone, equipment, sensor)
  with time-series attached to every node.

- **Vendor lock-in.** When a BMS vendor is replaced, the building's data model and
  often the data itself vanish. Selene runs on-premises, is open source, and stores
  data in open formats (Parquet for cold-tier time-series, RDF/Turtle for export).

- **Brick Schema adoption is stalled.** Creating and maintaining Brick models is
  "time-consuming, labor-intensive, and prone to errors," which is "one of the main
  reasons that the Brick Schema has not been widely used in real buildings"
  ([ScienceDirect, 2024](https://www.sciencedirect.com/science/article/abs/pii/S2352710224022654)).
  Selene's `selene-rdf` crate provides native Brick and ASHRAE 223P ontology support
  with SOSA observation materialization, enabling standard-compliant models without
  external tooling.

- **Edge resilience.** Safety, access, and comfort systems cannot depend on cloud
  connectivity. Selene's 14 MB Docker image runs inside the building on commodity
  hardware, with offline-first operation and federation for campus-wide queries when
  connectivity is available.

- **AI-assisted operations.** With 36 MCP tools, an AI agent can query equipment
  relationships, check recent sensor readings, detect anomalies, and suggest
  maintenance actions through natural language, all against local data with
  sub-millisecond latency.

**Relevant capabilities:** Schema inheritance (equipment type hierarchies), gap-filling
(LOCF/linear interpolation for missed readings), `ts.peerAnomalies` (graph-aware
outlier detection comparing equipment to its BFS neighbors), `ts.scopedAggregate`
(building/floor-level rollups via graph traversal), Cedar authorization (multi-tenant
buildings), dictionary encoding (83% memory savings on enum-like BMS point types).

**Market context:** The digital twin for smart building market is growing rapidly
through 2032. A March 2026 AutomatedBuildings article explicitly called for knowledge
graphs that "stay with the building" and are "free, open source, and vendor-neutral,
while also retaining data on premises."

---

### AI Agent Infrastructure (MCP Ecosystem)

**Fit: Very High. Strategic multiplier for all other verticals.**

The Model Context Protocol has become the universal standard for connecting AI agents
to external services, with 97M+ monthly SDK downloads and backing from Anthropic,
OpenAI, Google, and Microsoft. Major database vendors (Oracle, Microsoft) are rushing
to add MCP adapters to cloud databases. Selene has MCP as a first-class transport, not
an adapter.

**Industry pain points Selene addresses:**

- **Edge-native MCP is unoccupied territory.** Most MCP servers today are cloud
  services. An MCP server running on a Raspberry Pi inside a building, on a factory
  floor, or in a tactical vehicle is a differentiated offering. AI agents can interact
  with local infrastructure data without cloud round-trips.

- **Multi-database orchestration.** A typical agentic workflow that needs graph
  relationships, time-series telemetry, and vector similarity requires orchestrating
  three separate databases. Selene provides all three through a single MCP endpoint.

- **Agent authorization.** "MCP Firewalls" and governance registries are anticipated
  needs for 2026. Selene's Cedar authorization engine provides fine-grained,
  policy-based access control for which agents can access which data through which
  tools.

- **Graph queries suit LLM reasoning.** GQL expresses relationships naturally, which
  aligns with how language models reason about connected entities. "Find all HVAC units
  in building A that had temperature anomalies in the last week, and find similar
  patterns in the vector embedding space" is a single Selene interaction.

**Relevant capabilities:** 36 MCP tools + 5 resources + 3 prompt templates, semantic
search with containment path reconstruction, hybrid search (RRF-fused BM25 + vector),
auto-embedding background task, `gql_explain` for agent self-debugging.

**Market context:** MCP joined the Linux Foundation's Agentic AI Foundation in December
2025. By 2026, "agent squads" are expected to be orchestrated dynamically, with
specialized agents for diagnosis, remediation, validation, and documentation
([Equinix, 2025](https://blog.equinix.com/blog/2025/08/06/what-is-the-model-context-protocol-mcp-how-will-it-enable-the-future-of-agentic-ai/)).

---

### Defense and Military (Tactical Edge)

**Fit: High. DDIL requirements are an exact architectural match.**

Military operations frequently occur in Denied, Degraded, Intermittent, or Limited
(DDIL) communications environments. The Joint All-Domain Command and Control (JADC2)
vision requires a "data fabric" with edge-to-cloud replication, which is Selene's
architecture.

**Industry pain points Selene addresses:**

- **DDIL resilience.** Selene nodes operate independently when disconnected and sync
  when connectivity returns. This is the fundamental requirement for JADC2 data fabric:
  "distributed processing and data replication between remote computing nodes and the
  central cloud to provide continuity of operations in the case of network outages"
  ([FedTech, 2025](https://fedtechmagazine.com/article/2025/03/ddil-environments-managing-cloud-edge-computing-defense-agencies-perfcon)).

- **Situational awareness is a graph problem.** Force positions, threat locations,
  supply routes, communication links, and command hierarchies are all graph
  relationships with time-varying state (time-series for positions, sensor feeds,
  status changes).

- **SWaP constraints.** The 14 MB single binary with zero C/C++ dependencies fits
  tactical edge hardware. Pure Rust with static musl linking simplifies security
  auditing and reduces attack surface.

- **QUIC transport** is designed for unreliable networks with packet loss, exactly the
  conditions in tactical environments.

**Relevant capabilities:** Offline-first federation, CDC replication with
subscribe-before-snapshot protocol, Cedar authorization (classification and
need-to-know mapping), Dijkstra (route planning), PageRank (critical node
identification), connected components (isolated unit detection), community detection
(force grouping).

**Market context:** CJADC2 funding exceeds $1.4B in FY25, with $572.8M in FY26 R&D.
Long sales cycles but very large contracts.

---

### Telecommunications (Network Operations)

**Fit: High. Graph + time-series for root cause analysis is a proven approach.**

Telecommunication networks are inherently graphs with heterogeneous node and edge
types. Traditional monitoring uses correlation, not causation. NTT DOCOMO demonstrated
15-second failure isolation using graph-based root cause analysis versus hours with
traditional methods
([AWS, 2025](https://aws.amazon.com/blogs/database/beyond-correlation-finding-root-causes-using-a-network-digital-twin-graph-and-agentic-ai/)).

**Industry pain points Selene addresses:**

- **Network topology + metrics in one runtime.** Routers, switches, base stations,
  core functions, and subscribers are nodes. Physical links, logical connections, and
  service dependencies are edges. Each carries time-series metrics (throughput, error
  rate, CPU utilization). Today this requires separate graph and time-series databases.

- **Root cause analysis.** The AWS telecom RCA case study describes the need for
  combining "graph structure (what connects to what)" with "time-series metrics
  (performance degradation timing)." This is Selene's core model.

- **MEC edge deployment.** Multi-access Edge Computing (Phase 4, 2024-2026) is
  expanding edge computing in 5G networks. A Selene node at each MEC site can model
  local topology and metrics, federating to regional or national views.

- **Agentic network operations.** An AI agent connected via MCP can query topology,
  correlate anomalies with time-series data, and propose remediations, matching the
  agentic architecture described in the DOCOMO case study.

**Relevant capabilities:** PageRank (critical node identification), Dijkstra (path
analysis), connected components (partition detection), `ts.anomalies` (Z-score
detection), `ts.peerAnomalies` (graph-aware outlier detection), federation for
multi-site views.

**Market context:** 54% of mobile data traffic is expected via 5G by 2026. The graph
database market in telecom is growing as operators move from correlation-based to
causation-based network analytics.

---

## Tier 2: Strong Capability Alignment

### Energy and Utilities

**Fit: High. Grid topology + metering time-series is a natural match.**

Power grids are graphs: substations, transformers, feeders, DERs, meters, all connected
by physical and logical relationships. Distributed Energy Resource (DER) capacity is
projected to reach 530 GW by late 2026, creating a massively distributed topology
that requires local coordination.

**Why Selene fits:**
- Graph topology for grid modeling with time-series for energy metering
- Gap filling (LOCF/linear interpolation) and time-weighted averages for demand
  response calculations
- Edge deployment at the microgrid or substation level with federation for grid-wide
  queries when connectivity permits
- Dijkstra for power flow analysis, PageRank for critical node identification,
  connected components for islanding detection
- Offline-first design matches grid resilience requirements

**Market context:** China alone is investing approximately $574B in grid modernization
2026-2030. Microgrids are growing at 4.48% annually.

---

### Industrial IoT and Industry 4.0

**Fit: High. OT/IT convergence needs exactly this combination.**

Traditional OT architectures use isolated protocols (Modbus, OPC-UA, BACnet) creating
independent data islands. Cloud latency is unacceptable for real-time manufacturing
decisions. Gartner predicts 40% of organizations will slow cloud adoption by 2026 due
to cost management difficulty.

**Why Selene fits:**
- Sub-millisecond reads at the edge with zero cloud dependency
- Equipment topology as a graph with sensor data as time-series models the Unified
  Namespace (UNS) concept emerging as the Industry 4.0 backbone
- Louvain community detection identifies natural equipment clusters; connected
  components detect isolated subsystems (predictive maintenance)
- GQL as a single query interface bridges the IT/OT communication gap
- Federation maps to multi-site manufacturing: each factory runs its own node,
  corporate federates queries across all of them

---

### Retail and Supply Chain

**Fit: Moderate-High. Cold chain and graph reasoning are growing fast.**

The cold chain monitoring market is growing from $8.31B to $15.04B by 2030. By 2026,
"graph reasoning becomes an expected component of enterprise planning, with vendors
integrating graph frameworks directly into control towers and network design tools"
([Logistics Viewpoints, 2025](https://logisticsviewpoints.com/2025/12/15/the-top-10-supply-chain-technology-trends-to-watch-in-2026/)).

**Why Selene fits:**
- Cold chain is graph + time-series: shipment routes are graph traversals with
  temperature/humidity time-series attached to every node along the way
- Edge deployment in vehicles and warehouses with offline-first sync when connected
- Supply chain topology queries ("which stores received product from warehouse X
  between dates Y and Z?") are native GQL traversals with time filtering
- Louvain community detection for natural supply chain cluster identification

---

## Tier 3: Good Fit With Caveats

### Healthcare Facilities

**Fit: Moderate-High. Strong compliance and on-premises requirements, but the market is
crowded with specialized solutions.**

Equipment location tracking wastes over 40 hours per month per nurse. Over 3,850 smart
devices per hospital by 2026, with 53% having at least one security vulnerability.

**Why Selene fits:** Graph-based asset tracking (equipment assignments, room
relationships, maintenance history) with environmental monitoring time-series (OR
temperature, pharmaceutical storage). Cedar authorization maps to HIPAA-style access
control. On-premises operation addresses data residency requirements.

**Caveat:** Healthcare has many specialized solutions (Nurse Call, RTLS, EMR
integration) with established vendor relationships.

---

### Smart Cities and Infrastructure

**Fit: Moderate. Good technical alignment, but complex procurement and political
dynamics.**

IoT in Smart Cities market projected at $312.2B by 2026. Multi-domain integration
(traffic, water, waste, energy, public safety) is the core challenge.

**Why Selene fits:** District-level digital twins with federation for city-wide queries.
Graph algorithms for urban optimization (routing, critical infrastructure
identification, damage isolation). Multi-tier time-series handles data lifecycle from
real-time to compliance archives.

**Caveat:** Smart cities involve many specialized subsystems (SCADA for water,
proprietary traffic management) with their own data stores. Selene works best as a
unifying layer rather than a replacement for each subsystem. Municipal procurement
cycles are long.

---

### Agriculture and Precision Farming

**Fit: Moderate. Strong edge profile, but the domain lacks established ontology
standards.**

1.2 billion farm sensors globally, with cloud processing impractical for most
operations. Real-time irrigation adjustment can achieve water savings up to 50%.

**Why Selene fits:** Raspberry Pi deployment matches the farm edge profile. Time-series
with gap filling handles intermittent sensor data common in outdoor environments.
Graph model for field topology (fields, zones, sensors, valves, controllers).
Offline-first for unreliable rural connectivity. Federation for multi-farm aggregation.

**Caveat:** Agriculture lacks the strong ontology standards (like Brick for buildings)
that would showcase Selene's RDF capabilities. Large-scale operations may push beyond
in-memory capacity.

---

## Summary

Selene's unique value comes from combining capabilities that are otherwise spread
across multiple systems:

1. **Graph + time-series fusion** eliminates the need to synchronize separate databases
   for relationship data and telemetry
2. **Edge-first deployment** (14 MB, sub-ms latency, offline-first) enables operation
   where cloud databases cannot run
3. **MCP-native AI integration** positions Selene as infrastructure for the emerging
   agentic AI ecosystem
4. **Standards compliance** (ISO GQL, RDF/SPARQL, Brick/SOSA) provides interoperability
   without vendor lock-in
5. **Federation** enables distributed architectures where topology is configuration,
   not a fundamental constraint

The industries with the strongest fit share a common pattern: physical infrastructure
with rich relationship structure, continuous sensor telemetry, edge deployment
requirements, and an emerging need for AI-assisted operations.

---

## References

- [AutomatedBuildings: Knowledge Graphs in the Modern Building (March 2026)](https://www.automatedbuildings.com/2026/03/knowledge-graphs-in-the-modern-building/)
- [ScienceDirect: Automatic Brick Integration (2024)](https://www.sciencedirect.com/science/article/abs/pii/S2352710224022654)
- [Cohesion: Smart Building Technology 2026 Outlook](https://www.cohesionib.com/post/smart-building-technology-2026-outlook)
- [AWS: Network Digital Twin Graph and Agentic AI (2025)](https://aws.amazon.com/blogs/database/beyond-correlation-finding-root-causes-using-a-network-digital-twin-graph-and-agentic-ai/)
- [Pento: A Year of MCP (2025 Review)](https://www.pento.ai/blog/a-year-of-mcp-2025-review)
- [Anthropic: MCP and the Agentic AI Foundation (2025)](https://www.anthropic.com/news/donating-the-model-context-protocol-and-establishing-of-the-agentic-ai-foundation)
- [Equinix: MCP and the Future of Agentic AI (2025)](https://blog.equinix.com/blog/2025/08/06/what-is-the-model-context-protocol-mcp-how-will-it-enable-the-future-of-agentic-ai/)
- [Military Embedded Systems: Data Fabric for Tactical Edge](https://militaryembedded.com/comms/satellites/creating-the-data-fabric-for-tactical-edge-with-software-defined-wide-area-networking)
- [FedTech: DDIL Environments (2025)](https://fedtechmagazine.com/article/2025/03/ddil-environments-managing-cloud-edge-computing-defense-agencies-perfcon)
- [Lockheed Martin: CJADC2 Interoperability](https://www.lockheedmartin.com/en-us/news/features/2025/demonstrating-CJADC2-interoperability-factory.html)
- [SpringerOpen: Smart Grids In-Depth Survey](https://jesit.springeropen.com/articles/10.1186/s43067-025-00195-z)
- [FutureMarketInsights: Smart Grid Technology Market](https://www.futuremarketinsights.com/reports/smart-grid-technology-market)
- [HiveMQ: IT/OT Convergence Predictions (2025)](https://www.hivemq.com/blog/predictions-for-2025-convergence-of-ai-it-ot/)
- [Logistics Viewpoints: Supply Chain Technology Trends 2026](https://logisticsviewpoints.com/2025/12/15/the-top-10-supply-chain-technology-trends-to-watch-in-2026/)
- [MarketsandMarkets: Cold Chain Monitoring](https://www.marketsandmarkets.com/PressReleases/cold-chain-monitoring.asp)
- [MDPI: Edge Computing in Smart Agriculture](https://www.mdpi.com/1424-8220/25/17/5302)
- [Oracle: AI Database Agentic Innovations (2026)](https://www.oracle.com/news/announcement/oracle-unveils-ai-database-agentic-innovations-for-business-data-2026-03-24/)
- [Microsoft: Advancing Agentic AI with Databases (2026)](https://www.microsoft.com/en-us/sql-server/blog/2026/03/18/advancing-agentic-ai-with-microsoft-databases-across-a-unified-data-estate/)
