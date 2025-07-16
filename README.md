# Subgraph to Hyperindex Query Converter

A Rust service that converts TheGraph subgraph GraphQL queries to Hyperindex/Hasura GraphQL format and forwards them to a Hyperindex endpoint.

## Overview

This service acts as a translation layer between TheGraph's subgraph query interface and Hyperindex's Hasura-based GraphQL API. It accepts subgraph-style queries, converts them to the appropriate Hyperindex format, forwards them to the configured endpoint, and returns the live response.

## Features

- **Query Conversion**: Converts subgraph GraphQL syntax to Hyperindex format
- **HTTP Forwarding**: Forwards converted queries to Hyperindex endpoints
- **Environment Configuration**: Configurable endpoints via environment variables
- **Error Handling**: Comprehensive error handling and logging
- **Debug Endpoint**: Optional debug endpoint to inspect query conversion

## Current Conversion Rules

### Entity Name Conversion

- Plural entity names are singularized and capitalized
- Example: `streams` → `Stream`

### Parameter Mapping

| Subgraph Parameter | Hyperindex Parameter | Notes                               |
| ------------------ | -------------------- | ----------------------------------- |
| `first`            | `limit`              | Number of records to return         |
| `skip`             | `offset`             | Number of records to skip           |
| `orderBy`          | `order_by`           | Field to sort by (currently unused) |
| `orderDirection`   | `order_by` direction | Sort direction (currently unused)   |

### Special Handling

- **Stream Entity**: Automatically adds `where: {chainId: {_eq: "1"}}` clause
- **Selection Sets**: Preserved as-is in the converted query
- **Single Entity by Primary Key**: Singular entity queries with only an `id` parameter are converted to `entity_by_pk(id: ...)` format

### Filter Conversions

The following table shows how TheGraph filter syntax is converted to Hasura equivalents:

| The Graph Filter               | Hasura Equivalent                      | Description                          | Example (The Graph)                  | Example (Hasura)                        |
| ------------------------------ | -------------------------------------- | ------------------------------------ | ------------------------------------ | --------------------------------------- |
| `field`                        | `field: { _eq: val }`                  | Equal                                | `name: "Alice"`                      | `name: { _eq: "Alice" }`                |
| `field_not`                    | `field: { _neq: val }`                 | Not equal                            | `id_not: "0x123"`                    | `id: { _neq: "0x123" }`                 |
| `field_gt`                     | `field: { _gt: val }`                  | Greater than                         | `value_gt: 100`                      | `value: { _gt: 100 }`                   |
| `field_gte`                    | `field: { _gte: val }`                 | Greater than or equal                | `value_gte: 100`                     | `value: { _gte: 100 }`                  |
| `field_lt`                     | `field: { _lt: val }`                  | Less than                            | `timestamp_lt: 1650000000`           | `timestamp: { _lt: 1650000000 }`        |
| `field_lte`                    | `field: { _lte: val }`                 | Less than or equal                   | `timestamp_lte: 1650000000`          | `timestamp: { _lte: 1650000000 }`       |
| `field_in`                     | `field: { _in: [...] }`                | Matches any in array                 | `status_in: ["OPEN", "CLOSED"]`      | `status: { _in: ["OPEN", "CLOSED"] }`   |
| `field_not_in`                 | `field: { _nin: [...] }`               | Excludes values in array             | `id_not_in: ["0x1", "0x2"]`          | `id: { _nin: ["0x1", "0x2"] }`          |
| `field_contains`               | `field: { _ilike: "%val%" }`           | Substring match (case-insensitive)   | `name_contains: "graph"`             | `name: { _ilike: "%graph%" }`           |
| `field_not_contains`           | `field: { _not: { _ilike: "%val%" } }` | Substring mismatch                   | `name_not_contains: "graph"`         | `name: { _not: { _ilike: "%graph%" } }` |
| `field_starts_with`            | `field: { _ilike: "val%" }`            | Starts with                          | `symbol_starts_with: "ETH"`          | `symbol: { _ilike: "ETH%" }`            |
| `field_ends_with`              | `field: { _ilike: "%val" }`            | Ends with                            | `symbol_ends_with: "USD"`            | `symbol: { _ilike: "%USD" }`            |
| `field_not_starts_with`        | `field: { _not: { _ilike: "val%" } }`  | Doesn't start with                   | `name_not_starts_with: "A"`          | `name: { _not: { _ilike: "A%" } }`      |
| `field_not_ends_with`          | `field: { _not: { _ilike: "%val" } }`  | Doesn't end with                     | `name_not_ends_with: "x"`            | `name: { _not: { _ilike: "%x" } }`      |
| `field_contains_nocase`        | `field: { _ilike: "%val%" }`           | Substring match, case-insensitive    | `name_contains_nocase: "alice"`      | `name: { _ilike: "%alice%" }`           |
| `field_not_contains_nocase`    | `field: { _not: { _ilike: "%val%" } }` | Substring mismatch, case-insensitive | `name_not_contains_nocase: "alice"`  | `name: { _not: { _ilike: "%alice%" } }` |
| `field_starts_with_nocase`     | `field: { _ilike: "val%" }`            | Case-insensitive prefix match        | `id_starts_with_nocase: "0xabc"`     | `id: { _ilike: "0xabc%" }`              |
| `field_ends_with_nocase`       | `field: { _ilike: "%val" }`            | Case-insensitive suffix match        | `id_ends_with_nocase: "def"`         | `id: { _ilike: "%def" }`                |
| `field_not_starts_with_nocase` | `field: { _not: { _ilike: "val%" } }`  | Case-insensitive negated prefix      | `name_not_starts_with_nocase: "foo"` | `name: { _not: { _ilike: "foo%" } }`    |
| `field_not_ends_with_nocase`   | `field: { _not: { _ilike: "%val" } }`  | Case-insensitive negated suffix      | `name_not_ends_with_nocase: "bar"`   | `name: { _not: { _ilike: "%bar" } }`    |
| `field_containsAny`            | ❌ No direct equivalent                | Array overlap (string[] fields)      | `tags_containsAny: ["foo", "bar"]`   | ❌ Requires custom SQL                  |
| `field_containsAll`            | ❌ No direct equivalent                | Field contains all values            | `tags_containsAll: ["foo", "bar"]`   | ❌                                      |
| `id (top-level)`               | `entity_by_pk(id: ...)`                | Get by primary key                   | `user(id: "0x123")`                  | `user_by_pk(id: "0x123")`               |

## Setup

### Prerequisites

- Rust (latest stable version)
- Cargo

### Installation

1. Clone the repository:

```bash
git clone <repository-url>
cd subgraph-to-hyperindex-query-converter-poc
```

2. Create environment configuration:

```bash
cp .env.example .env
# Edit .env with your Hyperindex URL
```

3. Build and run:

```bash
cargo run
```

The service will start on `http://localhost:3000`

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
HYPERINDEX_URL=https://indexer.hyperindex.xyz/53b7e25/v1/graphql
```

## Usage

### Main Endpoint

POST requests to `/` will convert and forward queries to Hyperindex:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/
```

### Debug Endpoint

POST requests to `/debug` will return the converted query without forwarding:

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "query { streams(first: 2, skip: 10) { category cliff cliffTime chainId } }"}' \
  http://localhost:3000/debug
```

## Example Query Conversions

### Collection Query

#### Input (Subgraph Format)

```graphql
query {
  streams(first: 2, skip: 10) {
    category
    cliff
    cliffTime
    chainId
  }
}
```

#### Output (Hyperindex Format)

```graphql
query {
  Stream(limit: 2, offset: 10, where: { chainId: { _eq: "1" } }) {
    category
    cliff
    cliffTime
    chainId
  }
}
```

### Single Entity Query

#### Input (Subgraph Format)

```graphql
query {
  post(id: "0xabc...") {
    title
  }
}
```

#### Output (Hyperindex Format)

```graphql
query {
  post_by_pk(id: "0xabc...") {
    title
  }
}
```

### Response

```json
{
  "data": {
    "Stream": [
      {
        "category": "LockupDynamic",
        "chainId": "1",
        "cliff": false,
        "cliffTime": null
      },
      {
        "category": "LockupLinear",
        "chainId": "1",
        "cliff": false,
        "cliffTime": null
      }
    ]
  }
}
```

## Current Limitations

### Known Issues

1. **Hardcoded Where Clause**: The `where: {chainId: {_eq: "1"}}` clause is currently hardcoded for Stream entities
2. **Basic Parsing**: Uses simple string parsing instead of a proper GraphQL parser
3. **Limited Entity Support**: Currently optimized for Stream entities
4. **Order By**: `orderBy` and `orderDirection` parameters are extracted but not used in conversion
5. **No Block Queries**: Time-traveling queries with `block` parameters are not supported as Hyperindex doesn't natively support historical queries
6. **Data Limit**: Unless Hyperindex is configured via environment variables to support 5000 datapoints, the `limit` parameter should be set to a maximum of 1000

### Planned Improvements

- [ ] Use proper GraphQL parser for robust query handling
- [ ] Make where clauses configurable per entity
- [ ] Support for nested queries and fragments
- [ ] Add support for variables and directives
- [ ] Implement proper order_by conversion
- [ ] Add comprehensive test coverage

## Development

### Project Structure

```
src/
├── main.rs          # HTTP server and routing
└── conversion.rs    # Query conversion logic
```

### Adding New Conversion Rules

To add support for new entities or conversion rules, modify the `convert_query_structure` function in `src/conversion.rs`.

### Testing

```bash
# Check compilation
cargo check

# Run with debug output
RUST_LOG=debug cargo run

# Test conversion only
curl -X POST -H "Content-Type: application/json" \
  -d '{"query": "your query here"}' \
  http://localhost:3000/debug
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
