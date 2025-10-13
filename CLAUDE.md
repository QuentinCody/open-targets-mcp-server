# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an Open Targets MCP Server built with Cloudflare Workers and Durable Objects. It provides a Model Context Protocol (MCP) interface to query the Open Targets Platform GraphQL API and converts responses into queryable SQLite tables for analysis.

## Common Development Commands

- `npm run dev` - Start local development server with Wrangler
- `npm run deploy` - Deploy to Cloudflare Workers
- `npm run format` - Format code with Biome
- `npm run lint:fix` - Fix linting issues with Biome
- `wrangler types` - Generate TypeScript types for Cloudflare Workers

## Architecture

### Core Components

- **src/index.ts**: Main MCP server implementation (`OpenTargetsMCP` class)
  - Provides two tools: `opentargets_graphql_query` and `opentargets_query_sql`
  - Handles GraphQL queries to Open Targets API
  - Manages data staging via Durable Objects

- **src/do.ts**: Durable Object implementation (`JsonToSqlDO` class)
  - Converts JSON responses to SQLite tables
  - Provides SQL querying capabilities
  - Handles chunked content for large data fields
  - Schema-aware processing for Open Targets data structures

- **src/lib/**: Core processing engines
  - `SchemaInferenceEngine.ts`: Infers table schemas from JSON data
  - `DataInsertionEngine.ts`: Handles data insertion into SQLite
  - `ChunkingEngine.ts`: Manages large content chunking
  - `PaginationAnalyzer.ts`: Extracts pagination information
  - `SchemaParser.ts`: Parses GraphQL schema definitions
  - `types.ts`: Shared TypeScript interfaces

### Data Flow

1. GraphQL query executed against Open Targets API
2. Response processed and converted to normalized SQLite tables
3. Data staged in Durable Object with unique access ID
4. SQL queries executed against staged data with automatic chunked content resolution

### Open Targets Integration

The server is specifically designed for Open Targets Platform data:
- **Targets**: Gene/protein targets (Ensembl IDs)
- **Diseases**: Diseases/phenotypes (EFO IDs)
- **Drugs**: Compounds/drugs (ChEMBL IDs)
- **Associations**: Target-disease associations with evidence scores

## Configuration

- **wrangler.jsonc**: Cloudflare Workers configuration
  - Durable Objects bindings: `MCP_OBJECT` and `JSON_TO_SQL_DO`
  - Node.js compatibility enabled
  - Migration tags for schema versioning

- **biome.json**: Code formatting and linting
  - 4-space indentation, 100 character line width
  - Disabled rules: `noExplicitAny`, `noDebugger`, `noConsoleLog`

- **tsconfig.json**: TypeScript configuration with Cloudflare Workers types

## Development Notes

- **Dual Transport Support**: Server supports both Streamable HTTP (`/mcp`) and SSE (`/sse`) transports
  - Streamable HTTP is recommended for new integrations (MCP 2025-03-26 specification)
  - SSE transport maintained for backward compatibility with existing clients
- All SQL operations are read-only for security
- Introspection queries bypass staging and return JSON directly
- Large content fields are automatically chunked and can be resolved in SQL queries
- Schema inference handles Open Targets-specific data patterns
- Pagination information is extracted and included in processing results

## MCP Client Configuration

### Streamable HTTP Transport (Recommended)
Connect via Streamable HTTP endpoint: `https://open-targets-mcp-server.quentincody.workers.dev/mcp`

Example Claude Desktop config:
```json
"open-targets-worker": {
  "command": "npx",
  "args": ["mcp-remote", "https://open-targets-mcp-server.quentincody.workers.dev/mcp"]
}
```

### SSE Transport (Legacy Support)
For backward compatibility, the SSE endpoint is still available: `https://open-targets-mcp-server.quentincody.workers.dev/sse`

Legacy Claude Desktop config:
```json
"open-targets-worker": {
  "command": "npx",
  "args": ["mcp-remote", "https://open-targets-mcp-server.quentincody.workers.dev/sse"]
}
```

## License

MIT License with Academic Citation Requirement - see LICENSE.md and CITATION.md for details.