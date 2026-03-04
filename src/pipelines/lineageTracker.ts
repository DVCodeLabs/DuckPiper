import * as vscode from 'vscode';
import * as crypto from 'crypto';
import * as path from 'path';
import { Logger } from '../core/logger';
import { HeuristicSqlParser } from './sqlParser';
import { PipelineRepository } from './pipelineRepository';
import { LocalDuckDB } from '../transform/localDuckDB';
import {
    PipelineLineage,
    LineageTableNode,
    LineageTableEdge,
    LineageColumnNode,
    LineageColumnEdge,
    PipelineIndexEntry
} from './types';

interface NotebookDataInput {
    metadata?: { dp?: { pipelineId?: string } };
    cells?: Array<{
        type: string;
        id: string;
        outputName?: string;
        layer?: string;
        materialize?: string;
        sql?: string;
    }>;
}

export class LineageTracker {
    private static instance: LineageTracker;
    private repo: PipelineRepository;
    private parser: HeuristicSqlParser;
    private duckdb: LocalDuckDB;

    private constructor() {
        this.repo = PipelineRepository.getInstance();
        this.parser = new HeuristicSqlParser();
        this.duckdb = LocalDuckDB.getInstance();
    }

    public static getInstance(): LineageTracker {
        if (!LineageTracker.instance) {
            LineageTracker.instance = new LineageTracker();
        }
        return LineageTracker.instance;
    }

    /**
     * Update lineage for a specific notebook.
     * Guaranteed to process only transform cells.
     */
    async updateLineageForNotebook(notebookUri: vscode.Uri, notebookData?: NotebookDataInput): Promise<void> {
        try {
            // 1. Load notebook content if not provided
            if (!notebookData) {
                const content = await vscode.workspace.fs.readFile(notebookUri);
                notebookData = JSON.parse(Buffer.from(content).toString('utf8')) as NotebookDataInput;
            }
            if (!notebookData) return;

            // 2. Ensure pipelineId exists
            let pipelineId = notebookData.metadata?.dp?.pipelineId;
            if (!pipelineId) {
                pipelineId = crypto.randomUUID();
            }

            // 3. Prepare Lineage Data Structures
            const tables: LineageTableNode[] = [];
            const tableEdges: LineageTableEdge[] = [];

            // We use a map to dedupe columns and ensure we don't emit duplicates
            const columnsMap = new Map<string, LineageColumnNode>();
            const columnEdges: LineageColumnEdge[] = [];

            const cells = Array.isArray(notebookData.cells) ? notebookData.cells : [];

            for (const cell of cells) {
                // Must be SQL transform cell with output
                if (cell.type !== 'sql') continue;
                if (!cell.outputName) continue;

                const layer = cell.layer || 'bronze';
                if (!['bronze', 'silver', 'gold'].includes(layer)) continue;

                // Identification
                const outputName = cell.outputName;
                const fqn = `${layer}.${outputName}`;
                // V2 ID Format: pipelineId::fqn
                const tableNodeId = `${pipelineId}::${fqn}`;

                // Add Output Table Node
                tables.push({
                    id: tableNodeId,
                    fqn: fqn,
                    schema: layer,
                    name: outputName,
                    type: cell.materialize === 'view' ? 'view' : 'table',
                    createdBy: { cellId: cell.id }
                });

                const sql = cell.sql || '';

                // --- Step A & B: Introspect Output Columns (Authoritative) ---
                // We use realtime introspection to get the true columns of the materialized table
                const introCols = await this.duckdb.getSchemaColumns(layer, outputName);

                // create column nodes for output
                for (const col of introCols) {
                    const colFqn = `${fqn}.${col.name}`;
                    const colId = `${pipelineId}::${colFqn}`;

                    columnsMap.set(colId, {
                        id: colId,
                        tableId: tableNodeId,
                        fqn: colFqn,
                        name: col.name,
                        type: col.type,
                        createdBy: { cellId: cell.id }
                    });
                }

                // --- Step C: Parse SQL for Edges ---
                // --- Step C: Parse SQL for Edges ---
                try {
                    // Update to match new signature returning { aliases, fromTables, items }
                    const parseResult = this.parser.parse(sql);
                    const aliases = parseResult.aliases;
                    const fromTables = parseResult.fromTables;

                    // 1. Table Edges
                    // We can derive table edges from the aliases used (FROM/JOIN)
                    // The parser extracts them strictly from allowed schemas
                    for (const [alias, sourceFqn] of aliases.entries()) {
                        // Skip internal aliases unless we want to track them as explicit sources
                        // Actually aliases map contains all relevant sources now including those mapped to table names
                        if (alias === '__from__') continue;

                        // sourceFqn is schema.table
                        const parts = sourceFqn.split('.');
                        const sourceLayer = parts[0];
                        const sourceName = parts[1];
                        const sourceNodeId = `${pipelineId}::${sourceFqn}`;

                        if (!tables.find(t => t.id === sourceNodeId)) {
                            tables.push({
                                id: sourceNodeId,
                                fqn: sourceFqn,
                                schema: sourceLayer,
                                name: sourceName,
                                type: 'source'
                            });
                        }

                        // Dedupe identical edges
                        const existingEdge = tableEdges.find(e =>
                            e.from === sourceNodeId && e.to === tableNodeId && e.cellId === cell.id
                        );
                        if (!existingEdge) {
                            tableEdges.push({
                                from: sourceNodeId,
                                to: tableNodeId,
                                cellId: cell.id,
                                reason: 'sql_ref'
                            });
                        }
                    }

                    // 2. Column Edges
                    for (const item of parseResult.items) {
                        const outColName = item.alias;

                        // Handle Wildcard *
                        if (item.isWildcard) {
                            // Step D: Handle SELECT *
                            // Emit wildcard mapping edges for all distinct input tables
                            for (const sourceFqn of fromTables) {
                                const inputWildcardId = `${pipelineId}::${sourceFqn}.*`;
                                const outputWildcardId = `${tableNodeId}.*`;

                                columnEdges.push({
                                    from: inputWildcardId,
                                    to: outputWildcardId,
                                    cellId: cell.id,
                                    transform: '*',
                                    confidence: 0.4,
                                    reason: 'wildcard'
                                });
                            }
                            continue;
                        }

                        // Regular Column
                        // Rule: Only emit edge if output column exists in authoratitive introspection
                        const targetColFqn = `${fqn}.${outColName}`;
                        const targetColId = `${pipelineId}::${targetColFqn}`;

                        if (!columnsMap.has(targetColId)) {
                            // Output column mismatch (parsed alias not in schema). Skip.
                            continue;
                        }

                        // Extract Input Refs (Method A + B)
                        // Pass fromTables for Single-FROM fallback support
                        const refs = this.parser.extractColumnRefs(item.expression, aliases, fromTables);

                        for (const ref of refs) {
                            // ref must have a table source (either explicit or fallback)
                            if (!ref.table) continue;

                            const sourceTableFqn = ref.table;
                            const sourceColName = ref.column;
                            const sourceColFqn = `${sourceTableFqn}.${sourceColName}`;

                            const sourceColId = `${pipelineId}::${sourceColFqn}`;
                            const sourceTableId = `${pipelineId}::${sourceTableFqn}`;

                            // Ensure Source Column Node exists (placeholder if external)
                            if (!columnsMap.has(sourceColId)) {
                                columnsMap.set(sourceColId, {
                                    id: sourceColId,
                                    tableId: sourceTableId,
                                    fqn: sourceColFqn,
                                    name: sourceColName,
                                    type: 'unknown', // source
                                    createdBy: undefined
                                });
                            }

                            columnEdges.push({
                                from: sourceColId,
                                to: targetColId,
                                cellId: cell.id,
                                transform: item.expression,
                                confidence: item.confidence, // Base confidence
                                reason: 'select_expr'
                            });
                        }
                    }

                } catch (e) {
                    Logger.warn(`Lineage parsing failed for cell ${cell.id}`, e);
                }
            }

            // 5. Construct Lineage Object
            const lineage: PipelineLineage = {
                version: "0.2",
                pipelineId: pipelineId,
                generatedAt: new Date().toISOString(),
                notebookPath: vscode.workspace.asRelativePath(notebookUri),
                duckdbPath: "DP/system/piper.duckdb",
                tables,
                tableEdges,
                columns: Array.from(columnsMap.values()),
                columnEdges
            };

            // 6. Save Lineage
            await this.repo.saveLineage(lineage);

            // 7. Update Index
            const lineageUri = await this.repo.getLineageFileUri(notebookUri);
            const entry: PipelineIndexEntry = {
                pipelineId: pipelineId,
                name: path.basename(notebookUri.fsPath, '.dpnb'), // Use actual filename as source of truth
                notebookPath: vscode.workspace.asRelativePath(notebookUri),
                lineagePath: vscode.workspace.asRelativePath(lineageUri),
                duckdbPath: "DP/system/piper.duckdb",
                updatedAt: new Date().toISOString(),
                lastRunAt: new Date().toISOString()
            };
            await this.repo.updatePipelineEntry(entry);

        } catch (e) {
            Logger.error(`Failed to update lineage for ${notebookUri.fsPath}`, e);
        }
    }
}
