import { TableSchema } from "./types.js";

export interface ChunkMetadata {
	contentId: string;
	totalChunks: number;
	originalSize: number;
	contentType: 'json' | 'text';
	compressed: boolean;
	encoding?: string;
}

export interface ChunkRecord {
	id?: number;
	content_id: string;
	chunk_index: number;
	chunk_data: string;
	chunk_size: number;
}

export interface GraphQLFieldInfo {
	name: string;
	type: string;
	isList: boolean;
	isNullable: boolean;
	description?: string;
}

export interface GraphQLTypeInfo {
	name: string;
	kind: 'OBJECT' | 'SCALAR' | 'ENUM' | 'INTERFACE';
	fields: Record<string, GraphQLFieldInfo>;
	description?: string;
}

export interface GraphQLSchemaInfo {
	types: Record<string, GraphQLTypeInfo>;
	relationships: Array<{
		fromType: string;
		toType: string;
		fieldName: string;
		cardinality: string;
	}>;
}

export interface FieldChunkingRule {
	fieldName: string;
	typeName: string; // '*' for all types
	chunkThreshold: number;
	priority: 'always' | 'size-based' | 'never';
	reason?: string;
}

/**
 * ChunkingEngine handles storage and retrieval of large content by breaking it into chunks.
 * This improves performance, avoids SQLite size limits, and enables better memory management.
 * 
 * Optimized for Open Targets Platform API responses with biomedical data patterns.
 */
export class ChunkingEngine {
	private readonly CHUNK_SIZE_THRESHOLD = 32 * 1024; // 32KB - configurable
	private readonly CHUNK_SIZE = 16 * 1024; // 16KB per chunk - optimal for SQLite
	private readonly ENABLE_COMPRESSION = true; // Feature flag for compression
	
	private schemaInfo?: GraphQLSchemaInfo;
	private chunkingRules: FieldChunkingRule[] = [];

	/**
	 * Configure schema-aware chunking
	 */
	configureSchemaAwareness(schemaInfo: GraphQLSchemaInfo): void {
		this.schemaInfo = schemaInfo;
		this.generateOpenTargetsChunkingRules();
	}

	/**
	 * Generate intelligent chunking rules based on Open Targets schema patterns
	 */
	private generateOpenTargetsChunkingRules(): void {
		this.chunkingRules = [
			// Base rules that apply to all types
			{ fieldName: 'id', typeName: '*', chunkThreshold: Infinity, priority: 'never', reason: 'ID fields should never be chunked' },
			{ fieldName: 'ensemblId', typeName: '*', chunkThreshold: Infinity, priority: 'never', reason: 'Ensembl ID fields should never be chunked' },
			{ fieldName: 'efoId', typeName: '*', chunkThreshold: Infinity, priority: 'never', reason: 'EFO ID fields should never be chunked' },
			{ fieldName: 'chemblId', typeName: '*', chunkThreshold: Infinity, priority: 'never', reason: 'ChEMBL ID fields should never be chunked' },
			
			// Open Targets-specific large content fields
			{ fieldName: 'description', typeName: 'Target', chunkThreshold: 2048, priority: 'always', reason: 'Target descriptions are typically very long' },
			{ fieldName: 'description', typeName: 'Disease', chunkThreshold: 2048, priority: 'always', reason: 'Disease descriptions can be extensive' },
			{ fieldName: 'description', typeName: 'Drug', chunkThreshold: 2048, priority: 'always', reason: 'Drug descriptions can be extensive' },
			{ fieldName: 'synonyms', typeName: '*', chunkThreshold: 1024, priority: 'size-based', reason: 'Synonym arrays can be large' },
			
			// Tractability and constraint data
			{ fieldName: 'tractability', typeName: 'Target', chunkThreshold: 4096, priority: 'size-based', reason: 'Tractability data contains extensive nested information' },
			{ fieldName: 'geneticConstraint', typeName: 'Target', chunkThreshold: 2048, priority: 'size-based', reason: 'Genetic constraint data can be detailed' },
			{ fieldName: 'safety', typeName: 'Target', chunkThreshold: 4096, priority: 'size-based', reason: 'Safety information can be extensive' },
			
			// Association and evidence data
			{ fieldName: 'evidences', typeName: '*', chunkThreshold: 8192, priority: 'size-based', reason: 'Evidence arrays can be very large' },
			{ fieldName: 'associations', typeName: '*', chunkThreshold: 8192, priority: 'size-based', reason: 'Association arrays can be very large' },
			{ fieldName: 'studies', typeName: '*', chunkThreshold: 6144, priority: 'size-based', reason: 'Study arrays can be extensive' },
			
			// Pharmacovigilance and drug data
			{ fieldName: 'pharmacovigilance', typeName: 'Drug', chunkThreshold: 8192, priority: 'size-based', reason: 'Pharmacovigilance data can be very large' },
			{ fieldName: 'mechanismsOfAction', typeName: 'Drug', chunkThreshold: 4096, priority: 'size-based', reason: 'Mechanism of action data can be extensive' },
			{ fieldName: 'indications', typeName: 'Drug', chunkThreshold: 4096, priority: 'size-based', reason: 'Drug indications can be numerous' },
			
			// Ontology and classification data
			{ fieldName: 'ontology', typeName: 'Disease', chunkThreshold: 4096, priority: 'size-based', reason: 'Ontology data can contain extensive hierarchical information' },
			{ fieldName: 'therapeuticAreas', typeName: 'Disease', chunkThreshold: 2048, priority: 'size-based', reason: 'Therapeutic area data can be extensive' },
			
			// Expression and interaction data
			{ fieldName: 'expressions', typeName: 'Target', chunkThreshold: 6144, priority: 'size-based', reason: 'Expression data can be extensive across tissues' },
			{ fieldName: 'interactions', typeName: 'Target', chunkThreshold: 6144, priority: 'size-based', reason: 'Interaction data can be extensive' },
			{ fieldName: 'pathways', typeName: 'Target', chunkThreshold: 4096, priority: 'size-based', reason: 'Pathway data can be extensive' },
			
			// Conservative chunking for names and identifiers
			{ fieldName: 'approvedName', typeName: '*', chunkThreshold: 512, priority: 'size-based', reason: 'Approved names are usually short but can be long' },
			{ fieldName: 'approvedSymbol', typeName: '*', chunkThreshold: 256, priority: 'size-based', reason: 'Symbols are usually short' },
			{ fieldName: 'name', typeName: '*', chunkThreshold: 512, priority: 'size-based', reason: 'Names are usually short but can be long' },
		];

		// Generate type-specific rules based on schema analysis
		if (this.schemaInfo) {
			for (const [typeName, typeInfo] of Object.entries(this.schemaInfo.types)) {
				if (typeInfo.kind === 'OBJECT') {
					for (const [fieldName, fieldInfo] of Object.entries(typeInfo.fields)) {
						// Large list fields should be chunked aggressively
						if (fieldInfo.isList && this.isLikelyLargeContent(fieldInfo)) {
							this.chunkingRules.push({
								fieldName,
								typeName,
								chunkThreshold: 8192,
								priority: 'size-based',
								reason: `List field ${fieldName} on ${typeName} likely contains large content`
							});
						}
					}
				}
			}
		}
	}

	/**
	 * Determine if a field is likely to contain large content based on Open Targets patterns
	 */
	private isLikelyLargeContent(fieldInfo: GraphQLFieldInfo): boolean {
		const largeContentIndicators = [
			'description', 'summary', 'evidence', 'associations', 'studies',
			'tractability', 'constraint', 'safety', 'pharmacovigilance',
			'mechanisms', 'indications', 'ontology', 'expressions', 'interactions',
			'pathways', 'therapeuticAreas', 'synonyms', 'alternativeNames'
		];
		
		return largeContentIndicators.some(indicator => 
			fieldInfo.name.toLowerCase().includes(indicator) ||
			fieldInfo.description?.toLowerCase().includes(indicator)
		);
	}

	/**
	 * Schema-aware JSON stringification with intelligent chunking decisions
	 */
	async schemaAwareJsonStringify(
		obj: any, 
		typeName: string, 
		fieldName: string, 
		sql: any
	): Promise<string> {
		const jsonString = JSON.stringify(obj);
		
		// Check schema-based chunking rules first
		const applicableRule = this.getApplicableChunkingRule(fieldName, typeName);
		
		if (applicableRule) {
			if (applicableRule.priority === 'never') {
				return jsonString;
			} else if (applicableRule.priority === 'always' && jsonString.length > applicableRule.chunkThreshold) {
				const metadata = await this.storeChunkedContent(jsonString, 'json', sql);
				return this.createContentReference(metadata);
			} else if (applicableRule.priority === 'size-based' && jsonString.length > applicableRule.chunkThreshold) {
				const metadata = await this.storeChunkedContent(jsonString, 'json', sql);
				return this.createContentReference(metadata);
			}
		}
		
		// Fallback to default behavior
		if (!this.shouldChunk(jsonString)) {
			return jsonString;
		}

		const metadata = await this.storeChunkedContent(jsonString, 'json', sql);
		return this.createContentReference(metadata);
	}

	/**
	 * Get the most specific chunking rule for a field
	 */
	private getApplicableChunkingRule(fieldName: string, typeName: string): FieldChunkingRule | null {
		// Try exact type match first
		let rule = this.chunkingRules.find(r => r.fieldName === fieldName && r.typeName === typeName);
		if (rule) return rule;
		
		// Try wildcard type match
		rule = this.chunkingRules.find(r => r.fieldName === fieldName && r.typeName === '*');
		if (rule) return rule;
		
		return null;
	}

	/**
	 * Determines if content should be chunked based on size threshold
	 */
	shouldChunk(content: string): boolean {
		return content.length > this.CHUNK_SIZE_THRESHOLD;
	}

	/**
	 * Stores large content as chunks, returns metadata for retrieval
	 */
	async storeChunkedContent(
		content: string, 
		contentType: 'json' | 'text',
		sql: any
	): Promise<ChunkMetadata> {
		const contentId = this.generateContentId();
		let processedContent = content;
		let compressed = false;

		// Optional compression (when available in environment)
		if (this.ENABLE_COMPRESSION && this.shouldCompress(content)) {
			try {
				processedContent = await this.compress(content);
				compressed = true;
			} catch (error) {
				console.warn('Compression failed, storing uncompressed:', error);
				processedContent = content;
			}
		}

		// Ensure chunks table exists
		await this.ensureChunksTable(sql);

		// Split into chunks
		const chunks = this.splitIntoChunks(processedContent);
		
		// Store each chunk
		for (let i = 0; i < chunks.length; i++) {
			const chunkRecord: ChunkRecord = {
				content_id: contentId,
				chunk_index: i,
				chunk_data: chunks[i],
				chunk_size: chunks[i].length
			};
			
			await this.insertChunk(chunkRecord, sql);
		}

		// Store metadata
		const metadata: ChunkMetadata = {
			contentId,
			totalChunks: chunks.length,
			originalSize: content.length,
			contentType,
			compressed,
			encoding: compressed ? 'gzip' : undefined
		};

		await this.storeMetadata(metadata, sql);
		
		return metadata;
	}

	/**
	 * Retrieves and reassembles chunked content
	 */
	async retrieveChunkedContent(contentId: string, sql: any): Promise<string | null> {
		try {
			// Get metadata
			const metadata = await this.getMetadata(contentId, sql);
			if (!metadata) return null;

			// Retrieve all chunks in order
			const chunks = await this.getChunks(contentId, metadata.totalChunks, sql);
			if (chunks.length !== metadata.totalChunks) {
				throw new Error(`Missing chunks: expected ${metadata.totalChunks}, found ${chunks.length}`);
			}

			// Reassemble content
			const reassembled = chunks.join('');

			// Decompress if needed
			if (metadata.compressed) {
				try {
					return await this.decompress(reassembled);
				} catch (error) {
					console.error('Decompression failed:', error);
					throw new Error('Failed to decompress content');
				}
			}

			return reassembled;
		} catch (error) {
			console.error(`Failed to retrieve chunked content ${contentId}:`, error);
			return null;
		}
	}

	/**
	 * Creates a content reference for schema columns instead of storing large content directly
	 */
	createContentReference(metadata: ChunkMetadata): string {
		return `__CHUNKED__:${metadata.contentId}`;
	}

	/**
	 * Checks if a value is a chunked content reference
	 */
	isContentReference(value: any): boolean {
		return typeof value === 'string' && value.startsWith('__CHUNKED__:');
	}

	/**
	 * Extracts content ID from a content reference
	 */
	extractContentId(reference: string): string {
		return reference.replace('__CHUNKED__:', '');
	}

	/**
	 * Enhanced JSON stringification with automatic chunking
	 */
	async smartJsonStringify(obj: any, sql: any): Promise<string> {
		const jsonString = JSON.stringify(obj);
		
		if (!this.shouldChunk(jsonString)) {
			return jsonString;
		}

		// Store as chunks and return reference
		const metadata = await this.storeChunkedContent(jsonString, 'json', sql);
		return this.createContentReference(metadata);
	}

	/**
	 * Enhanced JSON parsing with automatic chunk retrieval
	 */
	async smartJsonParse(value: string, sql: any): Promise<any> {
		if (!this.isContentReference(value)) {
			return JSON.parse(value);
		}

		const contentId = this.extractContentId(value);
		const retrievedContent = await this.retrieveChunkedContent(contentId, sql);
		
		if (!retrievedContent) {
			throw new Error(`Failed to retrieve chunked content: ${contentId}`);
		}

		return JSON.parse(retrievedContent);
	}

	/**
	 * Cleanup chunked content (for maintenance)
	 */
	async cleanupChunkedContent(contentId: string, sql: any): Promise<void> {
		try {
			// Delete chunks
			sql.exec(
				`DELETE FROM content_chunks WHERE content_id = ?`,
				contentId
			);

			// Delete metadata
			sql.exec(
				`DELETE FROM chunk_metadata WHERE content_id = ?`,
				contentId
			);
		} catch (error) {
			console.error(`Failed to cleanup chunked content ${contentId}:`, error);
		}
	}

	/**
	 * Get statistics about chunked content storage
	 */
	async getChunkingStats(sql: any): Promise<any> {
		try {
			const metadataResult = sql.exec(`
				SELECT 
					COUNT(*) as total_chunked_items,
					SUM(original_size) as total_original_size,
					AVG(original_size) as avg_original_size,
					SUM(total_chunks) as total_chunks,
					COUNT(CASE WHEN compressed = 1 THEN 1 END) as compressed_items
				FROM chunk_metadata
			`).one();

			const chunksResult = sql.exec(`
				SELECT 
					COUNT(*) as total_chunk_records,
					SUM(chunk_size) as total_stored_size,
					AVG(chunk_size) as avg_chunk_size
				FROM content_chunks
			`).one();

			return {
				metadata: metadataResult || {},
				chunks: chunksResult || {},
				compression_ratio: metadataResult?.total_original_size && chunksResult?.total_stored_size 
					? (metadataResult.total_original_size / chunksResult.total_stored_size).toFixed(2)
					: null
			};
		} catch (error) {
			return { error: error instanceof Error ? error.message : 'Failed to get stats' };
		}
	}

	/**
	 * Analyze chunking effectiveness and provide Open Targets-specific recommendations
	 */
	async analyzeChunkingEffectiveness(sql: any): Promise<any> {
		const stats = await this.getChunkingStats(sql);
		
		if (!this.schemaInfo) {
			return {
				...stats,
				recommendation: "Enable schema-aware chunking by providing Open Targets GraphQL schema",
				schema_awareness: false
			};
		}

		// Analyze which fields are being chunked most
		const fieldAnalysis = await this.analyzeChunkedFields(sql);
		
		return {
			...stats,
			schema_awareness: true,
			field_analysis: fieldAnalysis,
			recommendations: this.generateOpenTargetsRecommendations(fieldAnalysis)
		};
	}

	private async analyzeChunkedFields(sql: any): Promise<any> {
		try {
			const result = sql.exec(`
				SELECT 
					content_type,
					original_size,
					compressed,
					COUNT(*) as chunk_count
				FROM chunk_metadata 
				GROUP BY content_type, compressed
				ORDER BY chunk_count DESC
			`).toArray();
			
			return result;
		} catch (error) {
			return { error: "Could not analyze chunked fields" };
		}
	}

	private generateOpenTargetsRecommendations(fieldAnalysis: any): string[] {
		const recommendations = [];
		
		if (this.chunkingRules.length === 0) {
			recommendations.push("Configure Open Targets-specific chunking rules based on your data patterns");
		}
		
		if (fieldAnalysis && fieldAnalysis.length > 0) {
			const uncompressedCount = fieldAnalysis.filter((f: any) => !f.compressed).length;
			if (uncompressedCount > 0) {
				recommendations.push("Enable compression for better storage efficiency of biomedical data");
			}
		}
		
		recommendations.push("Monitor chunk size distribution for target/disease/drug association queries");
		recommendations.push("Consider pagination for large association queries to optimize performance");
		
		return recommendations;
	}

	// Private helper methods

	private generateContentId(): string {
		return 'chunk_' + crypto.randomUUID().replace(/-/g, '');
	}

	private shouldCompress(content: string): boolean {
		// Compress content larger than 8KB (good compression threshold)
		return content.length > 8192;
	}

	private async compress(content: string): Promise<string> {
		try {
			const uint8Array = new TextEncoder().encode(content);
			
			// Check if CompressionStream is available (modern browsers/runtimes)
			if (typeof CompressionStream !== 'undefined') {
				const compressionStream = new CompressionStream('gzip');
				const writer = compressionStream.writable.getWriter();
				const reader = compressionStream.readable.getReader();
				
				writer.write(uint8Array);
				writer.close();
				
				const chunks: Uint8Array[] = [];
				let result = await reader.read();
				while (!result.done) {
					chunks.push(result.value);
					result = await reader.read();
				}
				
				// Combine chunks and encode to base64
				const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
				const combined = new Uint8Array(totalLength);
				let offset = 0;
				for (const chunk of chunks) {
					combined.set(chunk, offset);
					offset += chunk.length;
				}
				
				return btoa(String.fromCharCode(...combined));
			}
			
			// Fallback to simple base64 encoding (not real compression)
			return btoa(content);
		} catch (error) {
			throw new Error(`Compression failed: ${error}`);
		}
	}

	private async decompress(compressedContent: string): Promise<string> {
		try {
			// Check if DecompressionStream is available
			if (typeof DecompressionStream !== 'undefined') {
				const compressedData = Uint8Array.from(atob(compressedContent), c => c.charCodeAt(0));
				
				const decompressionStream = new DecompressionStream('gzip');
				const writer = decompressionStream.writable.getWriter();
				const reader = decompressionStream.readable.getReader();
				
				writer.write(compressedData);
				writer.close();
				
				const chunks: Uint8Array[] = [];
				let result = await reader.read();
				while (!result.done) {
					chunks.push(result.value);
					result = await reader.read();
				}
				
				const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
				const combined = new Uint8Array(totalLength);
				let offset = 0;
				for (const chunk of chunks) {
					combined.set(chunk, offset);
					offset += chunk.length;
				}
				
				return new TextDecoder().decode(combined);
			}
			
			// Fallback from base64
			return atob(compressedContent);
		} catch (error) {
			throw new Error(`Decompression failed: ${error}`);
		}
	}

	private splitIntoChunks(content: string): string[] {
		const chunks: string[] = [];
		for (let i = 0; i < content.length; i += this.CHUNK_SIZE) {
			chunks.push(content.slice(i, i + this.CHUNK_SIZE));
		}
		return chunks;
	}

	private async ensureChunksTable(sql: any): Promise<void> {
		// Create chunks table
		sql.exec(`
			CREATE TABLE IF NOT EXISTS content_chunks (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				content_id TEXT NOT NULL,
				chunk_index INTEGER NOT NULL,
				chunk_data TEXT NOT NULL,
				chunk_size INTEGER NOT NULL,
				created_at TEXT DEFAULT CURRENT_TIMESTAMP,
				UNIQUE(content_id, chunk_index)
			)
		`);

		// Create metadata table
		sql.exec(`
			CREATE TABLE IF NOT EXISTS chunk_metadata (
				content_id TEXT PRIMARY KEY,
				total_chunks INTEGER NOT NULL,
				original_size INTEGER NOT NULL,
				content_type TEXT NOT NULL,
				compressed INTEGER DEFAULT 0,
				encoding TEXT,
				created_at TEXT DEFAULT CURRENT_TIMESTAMP
			)
		`);

		// Create indexes for performance
		sql.exec(`CREATE INDEX IF NOT EXISTS idx_content_chunks_lookup ON content_chunks(content_id, chunk_index)`);
		sql.exec(`CREATE INDEX IF NOT EXISTS idx_chunk_metadata_size ON chunk_metadata(original_size)`);
	}

	private async insertChunk(chunk: ChunkRecord, sql: any): Promise<void> {
		sql.exec(
			`INSERT INTO content_chunks (content_id, chunk_index, chunk_data, chunk_size) 
			 VALUES (?, ?, ?, ?)`,
			chunk.content_id,
			chunk.chunk_index, 
			chunk.chunk_data,
			chunk.chunk_size
		);
	}

	private async storeMetadata(metadata: ChunkMetadata, sql: any): Promise<void> {
		sql.exec(
			`INSERT INTO chunk_metadata (content_id, total_chunks, original_size, content_type, compressed, encoding)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			metadata.contentId,
			metadata.totalChunks,
			metadata.originalSize,
			metadata.contentType,
			metadata.compressed ? 1 : 0,
			metadata.encoding || null
		);
	}

	private async getMetadata(contentId: string, sql: any): Promise<ChunkMetadata | null> {
		const result = sql.exec(
			`SELECT * FROM chunk_metadata WHERE content_id = ?`,
			contentId
		).one();

		if (!result) return null;

		return {
			contentId: result.content_id,
			totalChunks: result.total_chunks,
			originalSize: result.original_size,
			contentType: result.content_type,
			compressed: Boolean(result.compressed),
			encoding: result.encoding
		};
	}

	private async getChunks(contentId: string, expectedCount: number, sql: any): Promise<string[]> {
		const results = sql.exec(
			`SELECT chunk_data FROM content_chunks 
			 WHERE content_id = ? 
			 ORDER BY chunk_index ASC`,
			contentId
		).toArray();

		return results.map((row: any) => row.chunk_data);
	}
} 