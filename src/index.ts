import { McpAgent } from "agents/mcp";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { JsonToSqlDO } from "./do.js";

// ========================================
// API CONFIGURATION - Open Targets Platform
// ========================================
const GRAPHQL_TOOL_DESCRIPTION = [
	"Run any Open Targets GraphQL query and optionally stage the response as normalized SQLite tables.",
	"Typical flow: 1) run schema introspection or a data query, 2) if staging occurs you’ll receive a data_access_id for downstream SQL analysis.",
	"Inputs: query (required), variables (optional JSON map). Responses under ~500 characters or introspection payloads are returned raw without staging.",
	"Excellent for bespoke queries, schema discovery, or retrieving large result sets you plan to aggregate with SQL.",
	"Returns JSON containing either raw GraphQL data or staging metadata with usage_instructions."
].join("\n\n");

const SQL_TOOL_DESCRIPTION = [
	"Execute read-only SQL analytics against a staged Open Targets dataset.",
	"Use after opentargets_graphql_query returns a data_access_id; supports SELECT, CTEs, PRAGMA, and EXPLAIN.",
	"Inputs: data_access_id from the GraphQL tool, SQL string, optional positional params.",
	"Great for summarising association scores, filtering targets by tractability, or joining staged tables.",
	"Results include row_count, column_names, and automatically resolved chunked content."
].join("\n\n");

const TARGET_INFO_TOOL_DESCRIPTION = [
	"Retrieve baseline identity details for a single target (gene/protein) by Ensembl ID.",
	"Returns IDs, symbols, genomic location, synonyms, and protein references—ideal for grounding follow-up association or safety queries.",
	"When to use: you already know the Ensembl ID and need authoritative metadata without manual GraphQL.",
	"When not to use: you need to discover IDs first—use opentargets_graphql_query with search or the Open Targets website."
].join("\n\n");

const DISEASE_ASSOCIATED_TARGETS_DESCRIPTION = [
	"List the highest scoring targets associated with a disease (EFO) including association scores and datatype breakdown.",
	"Great for triaging target lists for a therapeutic area or preparing downstream evidence queries.",
	"Parameters include optional pagination controls that map to the Open Targets association table.",
	"Bypasses SQL staging and returns JSON directly for quick inspection."
].join("\n\n");

const GET_TARGET_INFO_QUERY = `
	query GetTargetInfo($ensemblId: String!) {
		target(ensemblId: $ensemblId) {
			id
			approvedSymbol
			approvedName
			biotype
			functionDescriptions
			synonyms {
				label
				source
			}
			genomicLocation {
				chromosome
				start
				end
				strand
			}
			proteinIds {
				id
				source
			}
		}
	}
`;

const DISEASE_ASSOCIATED_TARGETS_QUERY = `
	query DiseaseAssociatedTargets($efoId: String!, $pageIndex: Int!, $pageSize: Int!) {
		disease(efoId: $efoId) {
			id
			name
			associatedTargets(page: { index: $pageIndex, size: $pageSize }) {
				count
				rows {
					target {
						id
						approvedSymbol
						approvedName
						biotype
					}
					score
					datatypeScores {
						id
						score
					}
				}
			}
		}
	}
`;

const API_CONFIG = {
	name: "OpenTargetsExplorer",
	version: "1.0.0",
	description: "MCP Server for querying Open Targets Platform GraphQL API and converting responses to queryable SQLite tables",
	
	// GraphQL API settings
	endpoint: 'https://api.platform.opentargets.org/api/v4/graphql',
	headers: {
		"Content-Type": "application/json",
		"User-Agent": "OpenTargetsMCP/1.0.0"
	},
	
	// Tool names and descriptions
	tools: {
		graphql: {
			name: "opentargets_graphql_query",
			description: GRAPHQL_TOOL_DESCRIPTION
		},
		sql: {
			name: "opentargets_query_sql", 
			description: SQL_TOOL_DESCRIPTION
		}
	}
};

// In-memory registry of staged datasets
const datasetRegistry = new Map<string, { created: string; table_count?: number; total_rows?: number }>();

// ========================================
// ENVIRONMENT INTERFACE
// ========================================
interface OpenTargetsEnv {
	MCP_HOST?: string;
	MCP_PORT?: string;
	JSON_TO_SQL_DO: DurableObjectNamespace;
}

// ========================================
// CORE MCP SERVER CLASS - Open Targets
// ========================================

export class OpenTargetsMCP extends McpAgent {
	server = new McpServer({
		name: API_CONFIG.name,
		version: API_CONFIG.version,
		description: API_CONFIG.description
	}, {
		capabilities: {
			tools: {
				listChanged: true
			}
		}
	});

	async init() {
		// Tool #1: GraphQL to SQLite staging
		this.server.tool(
			API_CONFIG.tools.graphql.name,
			API_CONFIG.tools.graphql.description,
			{
				query: z.string().describe("GraphQL query string to execute against Open Targets Platform API"),
				variables: z.record(z.any()).optional().describe("Optional variables for the GraphQL query"),
			},
            async ({ query, variables }) => {
                try {
                    // Validate query before execution
                    const validation = this.validateAndSuggestQuery(query);
                    if (!validation.isValid && validation.suggestions) {
                        return {
                            content: [{
                                type: "text" as const,
                                text: JSON.stringify({
                                    success: false,
                                    error: "Query validation failed",
                                    suggestions: validation.suggestions,
                                    corrected_examples: {
                                        "For diseases": "diseases(efoIds: [\"EFO_0000270\"]) { id name }",
                                        "For targets": "targets(ensemblIds: [\"ENSG00000169083\"]) { id approvedSymbol }",
                                        "For search": "search(queryString: \"cancer\", entityNames: [\"disease\"]) { hits { id name } }",
                                        "Schema introspection": "{ __type(name: \"Query\") { fields { name args { name type { name } } } } }"
                                    }
                                }, null, 2)
                            }]
                        };
                    }

                    const graphqlResult = await this.executeGraphQLQuery(query, variables);

                    if (this.shouldBypassStaging(graphqlResult, query)) {
                        return {
                            content: [{
                                type: "text" as const,
                                text: JSON.stringify(graphqlResult, null, 2)
                            }]
                        };
                    }

                    const stagingResult = await this.stageDataInDurableObject(graphqlResult);
                    return {
                        content: [{
                            type: "text" as const,
                            text: JSON.stringify({
                                ...stagingResult,
                                usage_instructions: [
                                    `Use data_access_id="${stagingResult.data_access_id}" with the opentargets_query_sql tool to analyze the staged data`,
                                    "Example queries for Open Targets data:",
                                    "- Targets with high tractability: SELECT * FROM target WHERE json_extract(tractability_json, '$.score') > 0.8",
                                    "- Disease associations: SELECT * FROM disease_target_association ORDER BY score DESC LIMIT 10",
                                    "- Drug mechanisms: SELECT name, json_extract(mechanisms_json, '$[*].mechanismOfAction') FROM drug"
                                ]
                            }, null, 2)
                        }]
                    };

                } catch (error) {
                    return this.createErrorResponse("GraphQL execution failed", error);
                }
            }
        );

		// Tool #2: SQL querying against staged data
		this.server.tool(
			API_CONFIG.tools.sql.name,
			API_CONFIG.tools.sql.description,
			{
				data_access_id: z.string().describe("Data access ID from the GraphQL query tool"),
				sql: z.string().describe("SQL SELECT query to execute against the staged Open Targets data"),
				params: z.array(z.string()).optional().describe("Optional query parameters"),
			},
			async ({ data_access_id, sql }) => {
				try {
					const queryResult = await this.executeSQLQuery(data_access_id, sql);
					return { content: [{ type: "text" as const, text: JSON.stringify(queryResult, null, 2) }] };
				} catch (error) {
					return this.createErrorResponse("SQL execution failed", error);
				}
			}
		);

		// Tool #3: Direct target identity lookup (no staging)
		this.server.tool(
			"get_target_info",
			TARGET_INFO_TOOL_DESCRIPTION,
			{
				ensembl_id: z
					.string()
					.min(3)
					.describe("Ensembl target ID such as \"ENSG00000157764\"")
			},
			async ({ ensembl_id }) => {
				try {
					const result = await this.executeGraphQLQuery(GET_TARGET_INFO_QUERY, { ensemblId: ensembl_id });
					const data = result.data ?? result;
					return this.createJsonResponse(data);
				} catch (error) {
					return this.createErrorResponse("get_target_info failed", error);
				}
			}
		);

		// Tool #4: Disease-associated targets (no staging)
		this.server.tool(
			"get_disease_associated_targets",
			DISEASE_ASSOCIATED_TARGETS_DESCRIPTION,
			{
				efo_id: z
					.string()
					.min(3)
					.describe("Disease EFO identifier such as \"EFO_0000270\""),
				page_index: z
					.number()
					.int()
					.min(0)
					.default(0)
					.describe("Zero-based page index (default 0)"),
				page_size: z
					.number()
					.int()
					.min(1)
					.max(50)
					.default(10)
					.describe("Number of rows per page (default 10, max 50)")
			},
			async ({ efo_id, page_index, page_size }) => {
				try {
					const variables = {
						efoId: efo_id,
						pageIndex: page_index ?? 0,
						pageSize: page_size ?? 10
					};
					const result = await this.executeGraphQLQuery(DISEASE_ASSOCIATED_TARGETS_QUERY, variables);
					const data = result.data ?? result;
					return this.createJsonResponse(data);
				} catch (error) {
					return this.createErrorResponse("get_disease_associated_targets failed", error);
				}
			}
		);
	}

	// ========================================
	// QUERY VALIDATION AND ERROR PREVENTION
	// ========================================
	
	private validateAndSuggestQuery(query: string): { isValid: boolean; suggestions?: string[] } {
		const suggestions: string[] = [];
		const queryLower = query.toLowerCase();
		
		// Only check for DEFINITELY WRONG patterns - be very conservative
		
		// Check for field names that definitely don't exist
		if (queryLower.includes('items')) {
			suggestions.push("❌ FIELD ERROR: 'items' field doesn't exist in Open Targets API. Check schema with introspection.");
		}
		
		// Check for likely missing required parameters - these are well-established
		if (queryLower.includes('diseases(') && !queryLower.includes('efoids') && !queryLower.includes('associateddiseases')) {
			suggestions.push("❌ MISSING REQUIRED PARAMETER: 'diseases' requires 'efoIds: [String!]!' parameter.");
			suggestions.push('💡 SUGGESTION: Use \'diseases(efoIds: ["EFO_0000270"])\' or search first: \'search(queryString: "cancer", entityNames: ["disease"])\'');
		}
		
		if (queryLower.includes('targets(') && !queryLower.includes('ensemblids') && !queryLower.includes('associatedtargets')) {
			suggestions.push("❌ MISSING REQUIRED PARAMETER: 'targets' requires 'ensemblIds: [String!]!' parameter.");
			suggestions.push('💡 SUGGESTION: Use \'targets(ensemblIds: ["ENSG00000169083"])\' or search first: \'search(queryString: "BRCA1", entityNames: ["target"])\'');
		}
		
		if (queryLower.includes('drugs(') && !queryLower.includes('chemblids') && !queryLower.includes('knowndrugs')) {
			suggestions.push("❌ MISSING REQUIRED PARAMETER: 'drugs' requires 'chemblIds: [String!]!' parameter.");
			suggestions.push('💡 SUGGESTION: Use \'drugs(chemblIds: ["CHEMBL1201236"])\' or search first: \'search(queryString: "aspirin", entityNames: ["drug"])\'');
		}
		
		// For the original problematic pattern from user's example
		if (queryLower.includes('diseases(page:') && queryLower.includes('rows {') && !queryLower.includes('associateddiseases')) {
			suggestions.push("❌ WRONG COMBINATION: 'diseases' query with pagination and 'rows' field doesn't work.");
			suggestions.push("💡 Use 'diseases(efoIds: [...])' for multiple diseases or 'search(...)' to find disease IDs first");
		}
		
		return {
			isValid: suggestions.length === 0,
			suggestions: suggestions.length > 0 ? suggestions : undefined
		};
	}	// ========================================
	// GRAPHQL CLIENT - Open Targets API
	// ========================================
    private async executeGraphQLQuery(query: string, variables?: Record<string, any>): Promise<any> {
        const headers = {
            ...API_CONFIG.headers
        };
		
		const body = { query, ...(variables && { variables }) };
		
		const response = await fetch(API_CONFIG.endpoint, {
			method: 'POST',
			headers,
			body: JSON.stringify(body),
		});
		
		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`Open Targets API HTTP ${response.status}: ${errorText}`);
		}
		
        const result = await response.json() as any;
        
        if (result.errors) {
            throw new Error(`GraphQL errors: ${JSON.stringify(result.errors)}`);
        }
        
        return result;
    }

    private isIntrospectionQuery(query: string): boolean {
        if (!query) return false;
        
        // Remove comments and normalize whitespace for analysis
        const normalizedQuery = query
            .replace(/\s*#.*$/gm, '') // Remove comments
            .replace(/\s+/g, ' ')     // Normalize whitespace
            .trim()
            .toLowerCase();
        
        // Check for common introspection patterns
        const introspectionPatterns = [
            '__schema',           // Schema introspection
            '__type',            // Type introspection
            '__typename',        // Typename introspection
            'introspectionquery', // Named introspection queries
            'getintrospectionquery'
        ];
        
        return introspectionPatterns.some(pattern => 
            normalizedQuery.includes(pattern)
        );
    }

    private shouldBypassStaging(result: any, originalQuery?: string): boolean {
        if (!result) return true;

        // Always bypass introspection queries
        if (originalQuery && this.isIntrospectionQuery(originalQuery)) {
            return true;
        }

        // Always bypass if GraphQL reported errors
        if (result.errors) {
            return true;
        }

        // Always bypass introspection response structures
        if (result.data && (result.data.__schema || result.data.__type)) {
            return true;
        }

        // Simple rule: if it's a short response, bypass staging; if long, stage it
        try {
            const resultSize = JSON.stringify(result).length;
            return resultSize < 500; // Bypass staging for responses under 500 characters
        } catch {
            return true; // Bypass if we can't serialize it
        }
    }

	// ========================================
	// DURABLE OBJECT INTEGRATION
	// ========================================
	private async stageDataInDurableObject(graphqlResult: any): Promise<any> {
		const env = this.env as OpenTargetsEnv;
		if (!env?.JSON_TO_SQL_DO) {
			throw new Error("JSON_TO_SQL_DO binding not available");
		}
		
		const accessId = crypto.randomUUID();
		const doId = env.JSON_TO_SQL_DO.idFromName(accessId);
		const stub = env.JSON_TO_SQL_DO.get(doId);
		
		const response = await stub.fetch("http://do/process", {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(graphqlResult)
		});
		
		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`DO staging failed: ${errorText}`);
		}
		
        const processingResult = await response.json() as any;
        datasetRegistry.set(accessId, {
            created: new Date().toISOString(),
            table_count: processingResult.table_count,
            total_rows: processingResult.total_rows
        });
        return {
            data_access_id: accessId,
            processing_details: processingResult
        };
		}

	    private async executeSQLQuery(dataAccessId: string, sql: string): Promise<any> {
			const env = this.env as OpenTargetsEnv;
			if (!env?.JSON_TO_SQL_DO) {
				throw new Error("JSON_TO_SQL_DO binding not available");
			}

			if (!datasetRegistry.has(dataAccessId)) {
				throw new Error(
					`Unknown data_access_id "${dataAccessId}". Run opentargets_graphql_query first or verify the ID.`
				);
			}
			
			const doId = env.JSON_TO_SQL_DO.idFromName(dataAccessId);
			const stub = env.JSON_TO_SQL_DO.get(doId);
			
			// Use enhanced SQL execution that automatically resolves chunked content
		const response = await stub.fetch("http://do/query-enhanced", {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ sql })
		});
		
		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`SQL execution failed: ${errorText}`);
		}
		
		return await response.json();
	}

	private async deleteDataset(dataAccessId: string): Promise<boolean> {
        const env = this.env as OpenTargetsEnv;
        if (!env?.JSON_TO_SQL_DO) {
            throw new Error("JSON_TO_SQL_DO binding not available");
        }

        const doId = env.JSON_TO_SQL_DO.idFromName(dataAccessId);
        const stub = env.JSON_TO_SQL_DO.get(doId);

        const response = await stub.fetch("http://do/delete", { method: 'DELETE' });

        return response.ok;
	}

	private createJsonResponse(payload: unknown) {
		return {
			content: [
				{
					type: "text" as const,
					text: JSON.stringify(payload, null, 2)
				}
			]
		};
	}

	// ========================================
	// ERROR HANDLING - Reusable
	// ========================================
	private createErrorResponse(message: string, error: unknown) {
		const errorString = error instanceof Error ? error.message : String(error);
		
		// Parse common GraphQL errors and provide helpful suggestions
		let suggestions: string[] = [];
		
		if (errorString.includes("Unknown argument")) {
			suggestions.push("❌ PARAMETER ERROR: This field doesn't accept the argument you provided.");
			suggestions.push("💡 Use schema introspection to check valid arguments: { __type(name: \"Query\") { fields { name args { name type { name } } } } }");
		}
		
		if (errorString.includes("Cannot query field")) {
			suggestions.push("❌ FIELD ERROR: This field doesn't exist on this type.");
			suggestions.push("💡 Check available fields with: { __type(name: \"YourTypeName\") { fields { name type { name } } } }");
		}
		
		if (errorString.includes("is required but not provided")) {
			suggestions.push("❌ MISSING REQUIRED PARAMETER: A required argument is missing.");
			suggestions.push("💡 CORRECT PATTERNS:");
			suggestions.push("  - target(ensemblId: \"ENSG00000169083\") { ... }");
			suggestions.push("  - disease(efoId: \"EFO_0000270\") { ... }");
			suggestions.push("  - search(queryString: \"cancer\", entityNames: [\"disease\"]) { ... }");
		}
		
		if (errorString.includes("page") || errorString.includes("rows")) {
			suggestions.push("❌ PAGINATION ERROR: Open Targets doesn't use standard pagination.");
			suggestions.push("💡 CORRECT APPROACHES:");
			suggestions.push("  - For search: search(queryString: \"...\") { hits { ... } }");
			suggestions.push("  - For associations: associationsOnTheFly(...) { rows { ... } }");
			suggestions.push("  - For specific entities: provide exact IDs");
		}
		
		return {
			content: [{
				type: "text" as const,
				text: JSON.stringify({
					success: false,
					error: message,
					details: errorString,
					...(suggestions.length > 0 && { helpful_suggestions: suggestions }),
					quick_fixes: {
						schema_check: "{ __type(name: \"Query\") { fields { name args { name type { name } } } } }",
						search_example: "search(queryString: \"cancer\", entityNames: [\"disease\"]) { hits { id name } }",
						target_example: "target(ensemblId: \"ENSG00000169083\") { id approvedSymbol approvedName }",
						disease_example: "disease(efoId: \"EFO_0000270\") { id name }"
					}
				}, null, 2)
			}]
		};
	}
}

// ========================================
// CLOUDFLARE WORKERS BOILERPLATE
// ========================================
interface Env {
	MCP_HOST?: string;
	MCP_PORT?: string;
	JSON_TO_SQL_DO: DurableObjectNamespace;
}

interface ExecutionContext {
	waitUntil(promise: Promise<any>): void;
	passThroughOnException(): void;
}

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const url = new URL(request.url);

        // NEW: Streamable HTTP transport (MCP 2025-03-26 specification)
        if (url.pathname === "/mcp" || url.pathname.startsWith("/mcp/")) {
            const protocolVersion = request.headers.get("MCP-Protocol-Version");
            
            // @ts-ignore - Streamable HTTP transport handling
            const response = await OpenTargetsMCP.serve("/mcp").fetch(request, env, ctx);
            
            // Add protocol version header if provided in request
            if (protocolVersion && response instanceof Response) {
                const headers = new Headers(response.headers);
                headers.set("MCP-Protocol-Version", protocolVersion);
                return new Response(response.body, {
                    status: response.status,
                    statusText: response.statusText,
                    headers
                });
            }
            
            return response;
        }

        // LEGACY: SSE transport (maintain backward compatibility)
        if (url.pathname === "/sse" || url.pathname.startsWith("/sse/")) {
            const protocolVersion = request.headers.get("MCP-Protocol-Version");
            
            // @ts-ignore - SSE transport handling
            const response = await OpenTargetsMCP.serveSSE("/sse").fetch(request, env, ctx);
            
            if (protocolVersion && response instanceof Response) {
                const headers = new Headers(response.headers);
                headers.set("MCP-Protocol-Version", protocolVersion);
                return new Response(response.body, {
                    status: response.status,
                    statusText: response.statusText,
                    headers
                });
            }
            
            return response;
        }

        if (url.pathname === "/datasets" && request.method === "GET") {
            const list = Array.from(datasetRegistry.entries()).map(([id, info]) => ({
                data_access_id: id,
                ...info
            }));
            return new Response(JSON.stringify({ datasets: list }, null, 2), {
                headers: { "Content-Type": "application/json" }
            });
        }

        if (url.pathname.startsWith("/datasets/") && request.method === "DELETE") {
            const id = url.pathname.split("/")[2];
            if (!id || !datasetRegistry.has(id)) {
                return new Response(JSON.stringify({ error: "Dataset not found" }), {
                    status: 404,
                    headers: { "Content-Type": "application/json" }
                });
            }

            const doId = env.JSON_TO_SQL_DO.idFromName(id);
            const stub = env.JSON_TO_SQL_DO.get(doId);
            const resp = await stub.fetch("http://do/delete", { method: "DELETE" });
            if (resp.ok) {
                datasetRegistry.delete(id);
                return new Response(JSON.stringify({ success: true }), {
                    headers: { "Content-Type": "application/json" }
                });
            }

            const text = await resp.text();
            return new Response(JSON.stringify({ success: false, error: text }), {
                status: 500,
                headers: { "Content-Type": "application/json" }
            });
        }

        // Schema initialization endpoint
        if (url.pathname === "/initialize-schema" && request.method === "POST") {
            const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
            const stub = env.JSON_TO_SQL_DO.get(globalDoId);
            const resp = await stub.fetch("http://do/initialize-schema", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: await request.text()
            });
            return new Response(await resp.text(), {
                status: resp.status,
                headers: { "Content-Type": "application/json" }
            });
        }

        // Chunking stats endpoint
        if (url.pathname === "/chunking-stats" && request.method === "GET") {
            const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
            const stub = env.JSON_TO_SQL_DO.get(globalDoId);
            const resp = await stub.fetch("http://do/chunking-stats");
            return new Response(await resp.text(), {
                status: resp.status,
                headers: { "Content-Type": "application/json" }
            });
        }

        // Chunking analysis endpoint
        if (url.pathname === "/chunking-analysis" && request.method === "GET") {
            const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
            const stub = env.JSON_TO_SQL_DO.get(globalDoId);
            const resp = await stub.fetch("http://do/chunking-analysis");
            return new Response(await resp.text(), {
                status: resp.status,
                headers: { "Content-Type": "application/json" }
            });
        }

        return new Response(
            `${API_CONFIG.name}

Available endpoints:
- /mcp (Streamable HTTP transport - recommended)
- /sse (SSE transport - legacy support)`,
            { status: 404, headers: { "Content-Type": "text/plain" } }
        );
    },
};

export { OpenTargetsMCP as MyMCP };
export { JsonToSqlDO };
