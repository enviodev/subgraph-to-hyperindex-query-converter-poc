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

## Example Query Conversion

### Input (Subgraph Format)

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

### Output (Hyperindex Format)

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
