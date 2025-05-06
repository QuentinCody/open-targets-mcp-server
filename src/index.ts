import { McpAgent } from "agents/mcp";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";

// OpenTargets API Configuration
const OPEN_TARGETS_GRAPHQL_ENDPOINT = "https://api.platform.opentargets.org/api/v4/graphql";

export class OpenTargetsMcp extends McpAgent {
	server = new McpServer({
		name: "OpenTargetsExplorer",
		version: "0.1.0",
		description: `MCP Server for querying the Open Targets Platform GraphQL API.
This server uses the Open Targets GraphQL endpoint: ${OPEN_TARGETS_GRAPHQL_ENDPOINT}.

The Open Targets Platform integrates evidence from genetics, genomics, transcriptomics,
drugs, animal models and scientific literature to score and rank target-disease associations
for drug discovery.

**Key Features of the API:**
- Access data for targets, diseases/phenotypes, drugs, variants, studies, and credible sets.
- Construct precise queries to retrieve only the needed fields.
- Traverse the data graph through resolvable entities.

**Strongly Recommended: Use GraphQL Introspection**
Before constructing specific data queries, use GraphQL introspection to explore the schema:
- **Discover types, fields, and relationships:** Understand what data is available.
- **Avoid errors:** Verify field names and types directly.
- **Adapt to schema changes:** Dynamically adjust to API evolution.
- **Craft efficient queries:** Reduce trial-and-error.

**Example Introspection Queries:**
To list all types:
\`\`\`graphql
{
  __schema {
    types {
      name
      kind
    }
  }
}
\`\`\`
To get details for a specific type (e.g., "Target"):
\`\`\`graphql
{
  __type(name: "Target") {
    name
    kind
    description
    fields {
      name
      description
      type { name kind ofType { name kind } }
    }
  }
}
\`\`\`
You can use the GraphQL API playground (typically accessible by navigating to ${OPEN_TARGETS_GRAPHQL_ENDPOINT} in a browser) for schema exploration and testing queries.

For systematic queries (e.g., for multiple targets), Open Targets recommends using their data downloads or Google BigQuery instance. This API is best for targeted queries on single entities or specific associations.
Refer to Open Targets Platform documentation: https://platform.opentargets.org/data-and-code-access and their GraphQL API info: https://platform.opentargets.org/data-code-access/graphql-api`,
		capabilities: {
			tools: {}, // Indicates tool support. The tool itself is registered in init().
		}
	});

	async init() {
		console.error("OpenTargets MCP Server initializing...");

		this.server.tool(
			"opentargets_graphql_query",
			`Executes a GraphQL query against the Open Targets Platform API (${OPEN_TARGETS_GRAPHQL_ENDPOINT}).

**API Structure & Querying:**
The API is structured around biological and pharmacological entities. You query for these entities at the root level of your GraphQL query. Common root query fields include:
- **target**: Retrieve information for specific targets (e.g., by Ensembl ID). Includes details like genetic constraints, tractability assessments, associated diseases, and expression profiles.
  Example query root: \`query { target(ensemblId: "ENSG...") { ... } }\`
- **disease**: Fetch information for diseases or phenotypes (e.g., by EFO ID). Includes ontology details, known drugs, associated targets, and clinical signs.
  Example query root: \`query { disease(efoId: "EFO_...") { ... } }\`
- **drug**: Access data for compounds and drugs (e.g., by ChEMBL ID). Includes mechanisms of action, indications, and pharmacovigilance data.
  Example query root: \`query { drug(chemblId: "CHEMBL...") { ... } }\`
- **variantInfo**: (or similar, check schema) Get information for genetic variants.
  Example query root might be: \`query { variantInfo(variantId: "rs...") { ... } }\` (Verify exact field name via introspection)
- **search**: Perform searches across various entities like targets, diseases, and drugs.
  Example query root: \`query { search(queryString: "...", entityNames: [TARGET, DISEASE]) { ... } }\`

**Before constructing complex queries, ALWAYS use GraphQL introspection to confirm field names, types, available arguments, and query structures.**

**Example GraphQL Query for a Target (AR - ENSG00000169083):**
\`\`\`graphql
query targetInfo {
  target(ensemblId: "ENSG00000169083") {
    id
    approvedSymbol
    biotype
    geneticConstraint {
      constraintType
      exp
      obs
      score
      oe
    }
    tractability {
      label
      modality
      value
    }
  }
}
\`\`\`
**Example Query with Variables:**
Query string for the 'query' parameter:
\`\`\`graphql
query targetWithVariable($geneId: String!) {
  target(ensemblId: $geneId) {
    id
    approvedSymbol
  }
}
\`\`\`
JSON object for the 'variables' parameter of this tool:
\`\`\`json
{
  "geneId": "ENSG00000169083"
}
\`\`\`

**Important Notes:**
- For systematic queries involving multiple entities (e.g., all targets), Open Targets strongly recommends using their bulk data downloads or Google BigQuery instance for performance and efficiency. This GraphQL API is optimized for targeted lookups.
- Refer to the official Open Targets API documentation and their GraphQL playground for the most current schema details, query examples, and best practices.
- If your queries fail, the first step should be to use introspection to verify your query structure, field names, and types against the live API schema.`,
			{
				query: z.string().describe(
					`The GraphQL query string to execute. Example: 'query { target(ensemblId: "ENSG00000169083") { id approvedSymbol } }'. Use introspection queries like '{ __schema { types { name } } }' to explore available fields and types.`
				),
				variables: z
					.record(z.any())
					.optional()
					.describe(
						`Optional dictionary of variables for the GraphQL query. Example: { "ensemblId": "ENSG00000169083" }`
					),
			},
			async ({ query, variables }: { query: string; variables?: Record<string, any> }) => {
				console.error(`Executing opentargets_graphql_query with query: ${query.substring(0, 200)}...`);
				if (variables) {
					console.error(`With variables: ${JSON.stringify(variables).substring(0, 200)}...`);
				}

				const result = await this.executeOpenTargetsGraphQLQuery(query, variables);

				return {
					content: [
						{
							type: "text",
							text: JSON.stringify(result, null, 2), // Pretty-print JSON for readability
						},
					],
				};
			}
		);
		console.error("OpenTargets MCP Server initialized and tool 'opentargets_graphql_query' registered.");
	}

	private async executeOpenTargetsGraphQLQuery(
		query: string,
		variables?: Record<string, any>
	): Promise<any> {
		try {
			const headers = {
				"Content-Type": "application/json",
				"Accept": "application/json",
				"User-Agent": "OpenTargetsMcp/0.1.0 (ModelContextProtocol; +https://modelcontextprotocol.io)",
			};

			const bodyData: Record<string, any> = { query };
			if (variables && Object.keys(variables).length > 0) {
				bodyData.variables = variables;
			}

			console.error(`Making GraphQL request to: ${OPEN_TARGETS_GRAPHQL_ENDPOINT}`);

			const response = await fetch(OPEN_TARGETS_GRAPHQL_ENDPOINT, {
				method: "POST",
				headers,
				body: JSON.stringify(bodyData),
				// For Node.js environments, consider AbortController for timeouts:
				// signal: AbortSignal.timeout(30000) // 30 seconds timeout
			});

			console.error(`OpenTargets API response status: ${response.status}`);

			if (!response.ok) {
				let errorText = `OpenTargets API HTTP Error ${response.status}`;
				let responseBodyText = "";
				try {
					responseBodyText = await response.text();
					errorText += `: ${responseBodyText.substring(0, 500)}`; // Limit error text length
				} catch (e) {
					// Ignore if can't read body, errorText already has status
				}
				console.error(errorText);
				return {
					errors: [
						{
							message: `OpenTargets API HTTP Error ${response.status}`,
							extensions: {
								statusCode: response.status,
								responseText: responseBodyText.substring(0, 1000), // Provide some context
							},
						},
					],
				};
			}

			let responseBody: any;
			try {
				responseBody = await response.json();
			} catch (e) {
				// This case might be covered by !response.ok if API returns non-JSON on error,
				// but good to have a specific catch if response.ok but body is not JSON.
				const errorTextContent = await response.text(); // Re-read or use stored if available
				console.error(
					`OpenTargets API response is not JSON. Status: ${response.status}, Body: ${errorTextContent.substring(0, 500)}`
				);
				return {
					errors: [
						{
							message: `OpenTargets API Error: Non-JSON response.`,
							extensions: {
								statusCode: response.status,
								responseText: errorTextContent.substring(0, 1000),
							},
						},
					],
				};
			}

			// Log GraphQL-specific errors if present in the response body
			if (responseBody.errors) {
				console.error(`OpenTargets API GraphQL errors: ${JSON.stringify(responseBody.errors).substring(0, 500)}`);
			}
			return responseBody; // Return the full GraphQL response (can contain both 'data' and 'errors')

		} catch (error: any) {
			// Handles network errors or other issues with the fetch call itself
			const errorMessage = error instanceof Error ? error.message : String(error);
			console.error(
				`Client-side error during OpenTargets GraphQL request: ${errorMessage}`
			);
			return {
				errors: [
					{
						message: `Client-side error executing OpenTargets request: ${errorMessage}`,
						// Optionally, include error.name or error.stack in extensions for debugging if appropriate
						// extensions: { errorName: error.name }
					},
				],
			};
		}
	}
}

// Define Env interface for environment variables, if any specific to this server.
// Standard MCP_HOST, MCP_PORT might be handled by the SDK or runtime.
interface Env {
	MCP_HOST?: string;
	MCP_PORT?: string;
	// Example: OPEN_TARGETS_API_KEY?: string; (if OpenTargets ever requires one)
}

// Dummy ExecutionContext for type compatibility, as seen in the NCI GDC example.
// This is often provided by serverless runtimes like Cloudflare Workers.
interface ExecutionContext {
	waitUntil(promise: Promise<any>): void;
	passThroughOnException(): void;
	props: any; // Changed from optional to required
}

// Export the fetch handler, standard for environments like Cloudflare Workers or Deno Deploy.
export default {
	async fetch(
		request: Request,
		// The 'env' type might vary based on the deployment environment.
		// For Cloudflare Workers with Durable Objects, it's more complex.
		// For a simple server, it might be Record<string, string | undefined> or a custom Env interface.
		env: Env | Record<string, any>, // Using a general type here
		ctx: ExecutionContext
	): Promise<Response> {
		const url = new URL(request.url);

		// SSE transport is primary
		if (url.pathname === "/sse" || url.pathname.startsWith("/sse/")) {
			// The `OpenTargetsMcp.serveSSE` static method is assumed to be provided by
			// McpAgent or a base class, similar to the NCI GDC example.
			// This method should set up the SSE connection and handle MCP communication.
			// @ts-ignore McpAgent or a base class should provide serveSSE. The type might need casting or the SDK provides better typing.
			const sseHandler = OpenTargetsMcp.serveSSE("/sse");
			// The sseHandler.fetch is expected to conform to the Fetch API handler signature.
			return sseHandler.fetch(request, env as any, ctx); // Cast env to any as a common workaround for SDK type mismatches
		}

		// Fallback for unhandled paths
		console.error(
			`OpenTargets MCP Server. Requested path ${url.pathname} not found. Listening for SSE on /sse.`
		);
		return new Response(
			`OpenTargets MCP Server - Path not found.\nAvailable MCP paths:\n- /sse (for Server-Sent Events transport)`,
			{
				status: 404,
				headers: { "Content-Type": "text/plain" },
			}
		);
	},
};

// Export the Agent class if it needs to be used by other modules or for testing.
export { OpenTargetsMcp as MyMCP };
