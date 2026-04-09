# GQL Scalar Functions

SeleneDB provides 101 built-in scalar functions that can be used in `RETURN`, `FILTER`, `LET`, and `SET` expressions. This reference documents every function, organized by category.

## Core Functions

Core functions handle null coalescing, string basics, graph element introspection, degree analysis, path inspection, and property reflection.

| Name | Parameters | Returns | Description |
|------|-----------|---------|-------------|
| `coalesce` | value, value, ... | any | Return the first non-null argument |
| `char_length` | string | INT | Number of characters in a string |
| `upper` | string | STRING | Convert string to uppercase |
| `lower` | string | STRING | Convert string to lowercase |
| `trim` | string | STRING | Remove leading and trailing whitespace |
| `size` | list or path | INT | Number of elements in a list or edges in a path |
| `duration` | string | DURATION | Parse an ISO 8601 duration string |
| `zoned_datetime` | string? | ZONED_DATETIME | Parse ISO 8601 datetime, or return current time if no argument |
| `id` | node or edge | UINT | Numeric ID of a node or edge |
| `element_id` | node or edge | UINT | ISO alias for `id()` |
| `type` | edge | STRING | Label (type name) of an edge |
| `start_node` | edge | NODE | Source node of an edge |
| `end_node` | edge | NODE | Target node of an edge |
| `degree` | node | INT | Total degree (in + out) of a node |
| `in_degree` | node | INT | Number of incoming edges |
| `out_degree` | node | INT | Number of outgoing edges |
| `path_length` | path | INT | Number of edges in a path |
| `nodes` | path | LIST\<NODE\> | List of nodes in a path |
| `edges` | path | LIST\<EDGE\> | List of edges in a path |
| `is_acyclic` | path | BOOL | True if no node appears more than once |
| `is_trail` | path | BOOL | True if no edge appears more than once |
| `is_simple` | path | BOOL | True if no node repeats (start=end allowed) |
| `property_names` | node | LIST\<STRING\> | List of property keys on a node |
| `properties` | node or edge | STRING | Formatted string of all properties |
| `labels` | node or edge | LIST\<STRING\> | Labels on a node or type label on an edge. Parser keyword expression (`LABELS(x)`), not a registry function. |

```gql
-- Find sensors with high degree (many connections)
MATCH (s:sensor)
FILTER degree(s) > 5
RETURN s.name AS name, degree(s) AS connections

-- Null-safe property access
MATCH (n:equipment)
RETURN coalesce(n.displayName, n.name, 'unnamed') AS label

-- Inspect edge endpoints
MATCH (s:sensor)-[r:isPointOf]->(e:equipment)
RETURN type(r) AS rel, id(s) AS src_id, id(e) AS tgt_id
```

## String and Collection Functions

Functions for string manipulation, type checking, list operations, and value conversion.

| Name | Parameters | Returns | Description |
|------|-----------|---------|-------------|
| `replace` | string, search, replacement | STRING | Replace all occurrences of search with replacement |
| `reverse` | string or list | STRING or LIST | Reverse a string or list |
| `substring` | string, start, length? | STRING | Extract a substring (0-based start index) |
| `to_string` | any | STRING | Convert any value to its string representation |
| `value_type` | any | STRING | Return the type name of a value |
| `head` | list | any | First element of a list |
| `tail` | list | LIST | All elements except the first |
| `last` | list | any | Last element of a list |
| `range` | start, end, step? | LIST\<INT\> | Generate an integer list (step defaults to 1) |
| `keys` | node or edge | LIST\<STRING\> | Property keys of a node or edge |
| `nullif` | a, b | any or NULL | Return NULL if a equals b, otherwise return a |
| `left` | string, n | STRING | First n characters of a string |
| `right` | string, n | STRING | Last n characters of a string |
| `ltrim` | string | STRING | Remove leading whitespace |
| `rtrim` | string | STRING | Remove trailing whitespace |
| `starts_with` | string, prefix | BOOL | True if string starts with prefix |
| `ends_with` | string, suffix | BOOL | True if string ends with suffix |
| `contains` | string, substring | BOOL | True if string contains substring |
| `list_contains` | list, element | BOOL | True if list contains the element |
| `list_slice` | list, from, to | LIST | Extract a sublist (0-based indices) |
| `list_append` | list, element | LIST | Append an element to the end of a list |
| `list_prepend` | list, element | LIST | Prepend an element to the beginning of a list |
| `list_length` | list | INT | Number of elements in a list |
| `list_reverse` | list | LIST | Reverse the order of elements |
| `list_sort` | list | LIST | Sort elements in ascending order |
| `length` | list, string, or path | INT | Length of a list, string (chars), or path (edges) |
| `normalize` | string, form? | STRING | Unicode normalization (NFC/NFD/NFKC/NFKD, default NFC) |
| `double` | any numeric | FLOAT | Convert a value to DOUBLE (FLOAT) |

```gql
-- Filter sensors by name prefix
MATCH (s:sensor)
FILTER starts_with(s.name, 'temp-')
RETURN s.name AS name

-- Build a comma-separated list of property keys
MATCH (e:equipment) FILTER e.name = 'AHU-1'
RETURN keys(e) AS property_keys

-- Type-safe conditional logic
MATCH (n) FILTER value_type(n.reading) = 'FLOAT'
RETURN n.name AS name, n.reading AS value
```

## Math Functions

Standard mathematical operations. All trigonometric functions expect and return radians unless otherwise noted. Integer and float arguments are accepted; results are generally FLOAT.

| Name | Parameters | Returns | Description |
|------|-----------|---------|-------------|
| `abs` | number | INT or FLOAT | Absolute value |
| `ceil` | number | INT | Round up to nearest integer |
| `floor` | number | INT | Round down to nearest integer |
| `round` | number, places? | FLOAT or INT | Round to nearest integer or N decimal places |
| `sqrt` | number | FLOAT | Square root |
| `sign` | number | INT | Returns -1, 0, or 1 |
| `power` | base, exponent | FLOAT | Raise base to exponent |
| `log` | number | FLOAT | Natural logarithm (returns NULL for 0) |
| `log10` | number | FLOAT | Base-10 logarithm (returns NULL for 0) |
| `exp` | number | FLOAT | Euler's number raised to power |
| `sin` | number | FLOAT | Sine (radians) |
| `cos` | number | FLOAT | Cosine (radians) |
| `tan` | number | FLOAT | Tangent (radians) |
| `pi` | (none) | FLOAT | The constant pi (3.14159...) |
| `mod` | a, b | INT | Integer modulus (errors on division by zero) |
| `ln` | number | FLOAT | Natural logarithm |
| `cot` | number | FLOAT | Cotangent (1/tan, errors when tan(x) = 0) |
| `sinh` | number | FLOAT | Hyperbolic sine |
| `cosh` | number | FLOAT | Hyperbolic cosine |
| `tanh` | number | FLOAT | Hyperbolic tangent |
| `asin` | number | FLOAT | Inverse sine |
| `acos` | number | FLOAT | Inverse cosine |
| `atan` | number | FLOAT | Inverse tangent |
| `atan2` | y, x | FLOAT | Two-argument arctangent |
| `degrees` | radians | FLOAT | Convert radians to degrees |
| `radians` | degrees | FLOAT | Convert degrees to radians |
| `cardinality` | list | INT | List length (ISO GQL alias for `list_length`) |

```gql
-- Round sensor readings to 1 decimal place
MATCH (s:sensor)
RETURN s.name AS name, round(s.reading, 1) AS value

-- Compute distance between two 2D coordinates
MATCH (a:location), (b:location)
FILTER a.name = 'lobby' AND b.name = 'mech-room'
RETURN sqrt(power(a.x - b.x, 2) + power(a.y - b.y, 2)) AS distance

-- Convert bearing from degrees to radians
MATCH (s:sensor) FILTER s.type = 'wind_direction'
RETURN s.name AS name, radians(s.bearing) AS bearing_rad
```

## Temporal Functions

Functions for working with dates, times, datetimes, and durations. SeleneDB supports five temporal types: `DATE`, `LOCAL_TIME`, `ZONED_TIME`, `LOCAL_DATETIME`, and `ZONED_DATETIME`.

| Name | Parameters | Returns | Description |
|------|-----------|---------|-------------|
| `now` | (none) | ZONED_DATETIME | Current UTC timestamp |
| `current_date` | (none) | DATE | Current date (days since epoch) |
| `current_time` | (none) | ZONED_TIME | Current UTC time of day |
| `extract` | field, temporal | INT | Extract a component (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EPOCH) |
| `date_add` | timestamp, nanos | INT | Add nanoseconds to a timestamp |
| `date_sub` | timestamp, nanos | INT | Subtract nanoseconds from a timestamp |
| `timestamp_to_string` | nanos | STRING | Format epoch nanoseconds as ISO 8601 string |
| `local_time` | (none) | LOCAL_TIME | Current local time of day |
| `local_datetime` | (none) | LOCAL_DATETIME | Current local datetime |
| `date` | string | DATE | Parse an ISO date string (YYYY-MM-DD) |
| `time` | string | LOCAL_TIME | Parse an ISO time string (HH:MM:SS) |
| `zoned_time` | string | ZONED_TIME | Parse a time with timezone offset (HH:MM:SSZ or HH:MM:SS+02:00) |
| `duration_between` | temporal_a, temporal_b | DURATION | Duration between two temporal instants (a - b) |

The `extract` function works with all temporal types and durations. The available fields depend on the input type:

- **Date/DateTime types:** YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, EPOCH
- **Time types:** HOUR, MINUTE, SECOND
- **Duration:** DAY, DAYS, HOUR, HOURS, MINUTE, MINUTES, SECOND, SECONDS

```gql
-- Tag readings with current timestamp
MATCH (s:sensor) FILTER s.name = 'temp-1'
SET s.lastRead = now()

-- Extract hour from a timestamp for time-of-day analysis
MATCH (s:sensor) FILTER s.lastRead IS NOT NULL
RETURN s.name AS name, extract('HOUR', s.lastRead) AS hour

-- Parse and compare dates
MATCH (e:equipment) FILTER e.installDate IS NOT NULL
LET installed = date(e.installDate)
RETURN e.name AS name, duration_between(now(), installed) AS age
```

## Vector and Similarity Functions

Functions for computing similarity between vector embeddings. Both functions accept `VECTOR` values or `LIST<FLOAT>` values.

| Name | Parameters | Returns | Description |
|------|-----------|---------|-------------|
| `cosine_similarity` | vector_a, vector_b | FLOAT | Cosine similarity (-1.0 to 1.0). Returns 0.0 for zero-magnitude vectors. |
| `euclidean_distance` | vector_a, vector_b | FLOAT | Euclidean (L2) distance between two vectors. |

Both functions require vectors of equal dimensions and return an error on dimension mismatch.

```gql
-- Find sensors most similar to a reference embedding
MATCH (ref:sensor {name: 'reference'}), (s:sensor)
FILTER s.name <> 'reference' AND s.embedding IS NOT NULL
RETURN s.name AS name,
       cosine_similarity(ref.embedding, s.embedding) AS similarity
ORDER BY similarity DESC
LIMIT 10
```

## Text Matching

A case-insensitive text matching function for simple keyword search.

| Name | Parameters | Returns | Description |
|------|-----------|---------|-------------|
| `text_match` | text, query | BOOL | True if all whitespace-separated words in `query` appear in `text` (case-insensitive) |

```gql
-- Find equipment matching a search query
MATCH (e:equipment)
FILTER text_match(e.description, 'air handler rooftop')
RETURN e.name AS name, e.description AS desc
```

## Embedding

Generate vector embeddings from text using a locally loaded BERT model (all-MiniLM-L6-v2, 384 dimensions).

Requires: `--features vector`

| Name | Parameters | Returns | Description |
|------|-----------|---------|-------------|
| `embed` | string | VECTOR (384-dim) | Generate a sentence embedding from text. Returns NULL for NULL input. |

The embedding model is loaded on first call and cached for the server lifetime. If the model is not available, the function returns an error with instructions to run `scripts/fetch-model.sh`.

```gql
-- Generate and store an embedding for a sensor description
MATCH (s:sensor) FILTER s.name = 'temp-1'
SET s.embedding = embed(s.description)
```

## Aliases

Several functions have alternative names for ISO GQL spec alignment:

| Alias | Resolves To |
|-------|------------|
| `character_length` | `char_length` |
| `current_timestamp` | `now` |
| `datetime` | `zoned_datetime` |
| `null_if` | `nullif` |
| `local_timestamp` | `local_datetime` |

All aliases are fully interchangeable with their primary names. Using `character_length('hello')` is identical to `char_length('hello')`.

## Null Handling

All scalar functions follow consistent null-propagation rules:

- When a required argument is `NULL`, most functions return `NULL` (null-in, null-out).
- `coalesce` is the exception -- it skips null arguments to find the first non-null value.
- `nullif` returns `NULL` when its two arguments are equal.
- Functions that require specific types (such as `degree` requiring a node) return a type error when called with incompatible arguments, but return `NULL` for null inputs.
