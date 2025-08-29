# Open Targets MCP Server User Guide

## License and Citation

This project is available under the MIT License with an Academic Citation Requirement. This means you can freely use, modify, and distribute the code, but any academic or scientific publication that uses this software must provide appropriate attribution.

### For academic/research use:
If you use this software in a research project that leads to a publication, presentation, or report, you **must** cite this work according to the format provided in [CITATION.md](CITATION.md).

### For commercial/non-academic use:
Commercial and non-academic use follows the standard MIT License terms without the citation requirement.

By using this software, you agree to these terms. See [LICENSE.md](LICENSE.md) for the complete license text.This guide provides instructions for utilizing the Open Targets MCP Server to query the Open Targets Platform API via Model Context Protocol (MCP) clients such as Claude Desktop.

## Overview

This MCP server provides a bridge to the [Open Targets Platform API](https://platform.opentargets.org/data-code-access/graphql-api). The Open Targets Platform integrates evidence from genetics, genomics, transcriptomics, drugs, animal models, and scientific literature to score and rank target-disease associations for drug discovery.

This server exposes a single tool that allows users to execute GraphQL queries against the Open Targets API endpoint: `https://api.platform.opentargets.org/api/v4/graphql`.

**Key data accessible via this server includes:**
*   Information on specific biological **targets** (e.g., genes identified by Ensembl IDs).
*   Details regarding **diseases** or phenotypes (e.g., identified by EFO IDs).
*   Data on **drugs** and chemical compounds (e.g., identified by ChEMBL IDs).

## MCP Server and Client Concepts

*   **MCP Server:** A service that exposes specific functionalities (called "tools") from a data source or API. This Open Targets MCP Server provides one such tool for GraphQL queries.
*   **MCP Client:** An application (e.g., Claude Desktop, AI Playground) capable of connecting to MCP Servers to utilize their exposed tools.

## Server Connection Details

This server supports two transport protocols for connecting MCP clients:

### Streamable HTTP Transport (Recommended)

For new integrations, use the Streamable HTTP endpoint which provides better reliability and infrastructure compatibility:

`https://open-targets-mcp-server.quentincody.workers.dev/mcp`

**Configuration for Claude Desktop:**

1.  Launch Claude Desktop.
2.  Navigate to MCP Server configuration: `Settings > Developer > Edit Config` (or similar, depending on the version).
3.  Add a new server entry to the `mcpServers` object in the JSON configuration:

    ```json
    "open-targets-worker": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://open-targets-mcp-server.quentincody.workers.dev/mcp"
      ]
    }
    ```
    *   Ensure this new entry is correctly placed within the `mcpServers` JSON object (e.g., add a comma after the preceding entry if it's not the first).
    *   `"open-targets-worker"` is a suggested name; you can choose any descriptive name.
4.  Save the configuration file.

### SSE Transport (Legacy Support)

For backward compatibility with existing clients, the original SSE (Server-Sent Events) endpoint is still available:

`https://open-targets-mcp-server.quentincody.workers.dev/sse`

**Legacy Configuration for Claude Desktop:**

    ```json
    "open-targets-worker": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://open-targets-mcp-server.quentincody.workers.dev/sse"
      ]
    }
    ```

5.  Restart Claude Desktop. The new server and its tools should become available.

## Using the `opentargets_graphql_query` Tool

This server provides one tool: `opentargets_graphql_query`.

**Tool Parameters:**
*   `query` (string, required): The GraphQL query string to execute.
*   `variables` (JSON object, optional): A JSON object containing variables for the GraphQL query.

**GraphQL Fundamentals:**

GraphQL is a query language for APIs that enables clients to request precisely the data they need. Unlike traditional REST APIs that often return fixed data structures, GraphQL allows you to specify which fields to include in the response, minimizing data transfer and tailoring the response to your exact requirements.

**Workflow:**

1.  **Schema Introspection (Recommended First Step):**
    Before formulating data queries, use GraphQL introspection to understand the available data types, fields, and relationships in the Open Targets API schema.

    *   **Example 1: List all available data types:**
        ```graphql
        {
          __schema {
            types {
              name
              kind
            }
          }
        }
        ```

    *   **Example 2: Get details for a specific type (e.g., `Target`):**
        ```graphql
        {
          __type(name: "Target") {
            name
            kind
            description
            fields {
              name
              description
              type { name kind ofType { name kind } } # Shows field type and if it's a list, etc.
            }
          }
        }
        ```
    These introspection queries should be provided as the value for the `query` parameter of the `opentargets_graphql_query` tool.

2.  **Executing Data Queries:**
    Once familiar with the schema, construct queries to retrieve specific data.

    *   **Example: Retrieve information for a specific target (gene ENSG00000169083 - AR):**
        ```graphql
        query GetTargetDetails {
          target(ensemblId: "ENSG00000169083") {
            id
            approvedSymbol
            approvedName
            biotype
            geneticConstraint {
              constraintType
              exp
              score
            }
            tractability {
              label
              modality
              value
            }
          }
        }
        ```
        Provide this as the `query` parameter. The MCP client will display the JSON response from the API.

    *   **Example with Variables:**
        Query for `query` parameter:
        ```graphql
        query GetTargetWithVariable($geneId: String!) {
          target(ensemblId: $geneId) {
            id
            approvedSymbol
          }
        }
        ```
        JSON for `variables` parameter:
        ```json
        {
          "geneId": "ENSG00000169083"
        }
        ```

**Identifying Entity IDs:**
*   **Targets:** Use Ensembl IDs (e.g., `ENSG00000169083`).
*   **Diseases:** Use Experimental Factor Ontology (EFO) IDs (e.g., `EFO_0000270` for asthma).
*   **Drugs:** Use ChEMBL IDs (e.g., `CHEMBL1201236` for a specific compound).
    These identifiers can typically be found by searching the [Open Targets Platform website](https://platform.opentargets.org/).

## Important Considerations

*   **Response Format:** The API returns data in JSON format. MCP clients usually provide a way to view this structured data.
*   **Rate Limits & Query Complexity:** Be mindful of potential API rate limits. Very complex queries or requests for excessive amounts of data might be slow or throttled by the Open Targets API.
*   **Bulk Data Access:** For systematic, large-scale data retrieval (e.g., data for all human genes), Open Targets recommends using their [official data downloads or Google BigQuery instance](https://platform.opentargets.org/data-and-code-access) rather than repeated GraphQL queries via this API.

## Additional Resources

*   **Open Targets Platform:** [https://platform.opentargets.org/](https://platform.opentargets.org/)
*   **Open Targets GraphQL API Documentation:** [https://platform.opentargets.org/data-code-access/graphql-api](https://platform.opentargets.org/data-code-access/graphql-api) (Includes a browser-based GraphQL Playground for testing queries.)
*   **GraphQL Introduction:** [https://graphql.org/learn/](https://graphql.org/learn/)

This MCP server aims to facilitate targeted exploration of the Open Targets Platform. Refer to the official Open Targets documentation for comprehensive information on their data and API.
