# RDF and SPARQL Guide

SeleneDB provides RDF import/export and SPARQL query support for interoperability with semantic web tools, building ontologies (Brick, ASHRAE 223P), and linked data workflows. The implementation uses oxrdf and oxttl for RDF data modeling and serialization, and spareval for SPARQL query evaluation.

RDF support is feature-gated:

- `--features rdf` -- enables import/export (Turtle, N-Triples, N-Quads)
- `--features rdf-sparql` -- adds SPARQL query evaluation on top of `rdf`

## Property Graph to RDF Mapping

SeleneDB maps its property graph to RDF using a configurable namespace prefix. The default namespace is `selene:`.

### Nodes

Each node becomes an RDF subject with:

- One `rdf:type` triple per label -- `<ns:node/42> rdf:type <ns:type/sensor>`
- One datatype triple per property -- `<ns:node/42> <ns:prop/temperature> "72.3"^^xsd:double`

Null properties are skipped. List-valued properties expand into multiple triples for the same predicate.

### Edges

Each edge produces:

- A base relationship triple -- `<ns:node/1> <ns:rel/contains> <ns:node/42>`
- Edge identity triples linking the edge URI to source, target, and label -- useful for round-tripping edge properties
- One property triple per edge property

### URI Structure

All URIs share the configured namespace prefix. The path segments are:

| Segment | Example | Description |
|---------|---------|-------------|
| `node/` | `selene:node/42` | Node subject URI |
| `type/` | `selene:type/sensor` | Label type URI (used with rdf:type) |
| `prop/` | `selene:prop/temperature` | Property predicate URI |
| `rel/` | `selene:rel/contains` | Relationship predicate URI |
| `edge/` | `selene:edge/7` | Edge subject URI (for reification) |
| `obs/` | `selene:obs/42/temperature` | SOSA Observation URI |

## Import

### HTTP API

Import RDF data via `POST /graph/rdf`. The format defaults to Turtle if not specified.

```bash
curl -X POST "http://localhost:8080/graph/rdf?format=turtle" \
  -H "Content-Type: text/turtle" \
  -d '@prefix ex: <https://example.org/building#> .
ex:node/1 a ex:type/sensor ;
    ex:prop/name "SAT-1" ;
    ex:prop/temperature "72.3"^^<http://www.w3.org/2001/XMLSchema#double> .'
```

Supported formats: `turtle` (or `ttl`), `ntriples` (or `nt`), `nquads` (or `nq`).

The maximum number of quads per import is 1,000,000 by default.

### GQL Procedure

```sql
CALL graph.importRdf('@prefix ex: <https://example.org/building#> .
ex:node/1 a ex:type/sensor ;
    ex:prop/name "SAT-1" .', 'turtle')
YIELD nodesCreated, edgesCreated, labelsAdded, propertiesSet, ontologyTriplesLoaded
```

| Parameter | Type | Description |
|-----------|------|-------------|
| data | STRING | RDF data as a string |
| format | STRING | Serialization format (turtle, ntriples, nquads) |
| targetGraph | STRING | (Optional) target graph -- use 'ontology' for TBox data |

**Yields:** `nodesCreated` (INT), `edgesCreated` (INT), `labelsAdded` (INT), `propertiesSet` (INT), `ontologyTriplesLoaded` (INT)

## Export

### HTTP API

Export the graph as RDF via `GET /graph/rdf`. The format defaults to Turtle.

```bash
# Export as Turtle
curl "http://localhost:8080/graph/rdf?format=turtle"

# Export as N-Triples
curl "http://localhost:8080/graph/rdf?format=ntriples"

# Export as N-Quads, including ontology graph
curl "http://localhost:8080/graph/rdf?format=nquads&graphs=all"
```

The `graphs=all` parameter includes ontology quads in the output (N-Quads only).

### GQL Procedure

```sql
CALL graph.exportRdf('turtle') YIELD data, format
```

To include ontology quads (N-Quads format):

```sql
CALL graph.exportRdf('nquads', true) YIELD data, format
```

| Parameter | Type | Description |
|-----------|------|-------------|
| format | STRING | Output format (turtle, ntriples, nquads) |
| includeAll | BOOL | (Optional) include ontology graph (default: false) |

**Yields:** `data` (STRING), `format` (STRING)

## Ontology Store

SeleneDB maintains a separate ontology store for TBox data (class hierarchies, property definitions). This is used for Brick Schema, ASHRAE 223P, or any OWL/RDFS ontology.

Import ontology triples by adding `graph=ontology` to the HTTP query string or passing `'ontology'` as the third argument to `graph.importRdf`:

```bash
# HTTP
curl -X POST "http://localhost:8080/graph/rdf?format=turtle&graph=ontology" \
  -H "Content-Type: text/turtle" \
  --data-binary @brick.ttl
```

```sql
-- GQL
CALL graph.importRdf(ontologyData, 'turtle', 'ontology')
YIELD nodesCreated, edgesCreated, labelsAdded, propertiesSet, ontologyTriplesLoaded
```

Ontology triples are persisted in snapshot extra sections and survive restarts. They are available to SPARQL queries via the named graph mechanism.

## SOSA Observations

When `materialize_observations` is enabled in the RDF configuration, SeleneDB maintains one `sosa:Observation` node per sensor-property pair. Each Observation node carries:

- `observedProperty` -- the property name (string)
- `simpleResult` -- the latest reading (float)
- `resultTime` -- the reading timestamp

The Observation is linked to its sensor via a `madeBySensor` edge. This makes the current sensor state visible to SPARQL queries without materializing the full time-series history.

```toml
[rdf]
materialize_observations = true
observation_debounce_ms = 1000
```

## SPARQL Queries

SPARQL query evaluation requires `--features rdf-sparql`. SeleneDB evaluates SPARQL against the property graph viewed as an RDF dataset, routing triple patterns to label bitmaps, CSR adjacency, and TypedIndex for efficient evaluation without materializing quads.

### HTTP API

```bash
# GET with query parameter
curl "http://localhost:8080/sparql?query=SELECT%20%3Fs%20%3Fname%20WHERE%20%7B%20%3Fs%20a%20%3Cselene%3Atype%2Fsensor%3E%20.%20%3Fs%20%3Cselene%3Aprop%2Fname%3E%20%3Fname%20%7D"

# POST with SPARQL body
curl -X POST http://localhost:8080/sparql \
  -H "Content-Type: application/sparql-query" \
  -d 'SELECT ?s ?name WHERE {
    ?s a <selene:type/sensor> .
    ?s <selene:prop/name> ?name
  }'
```

The response format defaults to JSON. Both `GET /sparql?query=...` and `POST /sparql` are supported.

### GQL Procedure

```sql
CALL graph.sparql('SELECT ?s ?name WHERE {
  ?s a <selene:type/sensor> .
  ?s <selene:prop/name> ?name
}') YIELD results
```

| Parameter | Type | Description |
|-----------|------|-------------|
| query | STRING | SPARQL query string |

**Yields:** `results` (STRING) -- JSON-serialized SPARQL results

### Example Queries

Find all sensors:

```sparql
SELECT ?s ?name WHERE {
  ?s a <selene:type/sensor> .
  ?s <selene:prop/name> ?name
}
```

Find sensors connected to a specific AHU:

```sparql
SELECT ?sensor ?name WHERE {
  <selene:node/1> <selene:rel/serves> ?sensor .
  ?sensor <selene:prop/name> ?name
}
```

Count nodes by label:

```sparql
SELECT ?type (COUNT(?s) AS ?count) WHERE {
  ?s a ?type
}
GROUP BY ?type
```

## Configuration

The `[rdf]` section in `selene.toml` controls RDF behavior:

```toml
[rdf]
namespace = "https://example.org/building#"
materialize_observations = true
observation_debounce_ms = 1000
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| namespace | STRING | `"selene:"` | Base namespace URI for minting RDF URIs |
| materialize_observations | BOOL | `false` | Maintain SOSA Observation nodes for TS data |
| observation_debounce_ms | INT | `1000` | Debounce interval for observation materialization |

The namespace determines the prefix for all generated URIs. For production deployments, set this to a proper HTTP URI such as `https://example.org/building#`. The prefix is normalized to end with `/` or `#` if it does not already.
