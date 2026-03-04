import * as vscode from 'vscode';
import { LocalDuckDB } from '../localDuckDB';
import { convertBigIntForSerialization } from '../../connections/adapters/serializationUtils';
import { getConnection } from '../../connections/connectionStore';
import { performIntrospection } from '../../connections/connectionCommands';
import { Logger } from '../../core/logger';
import { formatTransformError } from '../../core/errorHandler';

export class DPNotebookController {
    readonly controllerId = 'dp-controller';
    readonly notebookType = 'dp-notebook';
    readonly label = 'DuckPiper Local DuckDB';
    readonly supportedLanguages = ['sql'];

    private readonly _controller: vscode.NotebookController;

    constructor() {
        this._controller = vscode.notebooks.createNotebookController(
            this.controllerId,
            this.notebookType,
            this.label
        );

        this._controller.supportedLanguages = this.supportedLanguages;
        this._controller.supportsExecutionOrder = true;
        this._controller.executeHandler = this._execute.bind(this);
    }

    dispose() {
        this._controller.dispose();
    }

    private async _execute(
        cells: vscode.NotebookCell[],
        _notebook: vscode.NotebookDocument,
        _controller: vscode.NotebookController
    ): Promise<void> {
        for (const cell of cells) {
            await this._doExecution(cell);
        }
    }

    private async _annotateBlockedLine(cell: vscode.NotebookCell, blockedRegex: RegExp): Promise<void> {
        // Only annotate if a blocked keyword appears on its own line (ignoring comments)
        const text = cell.document.getText();
        const lines = text.split('\n');
        const edit = new vscode.WorkspaceEdit();
        let modified = false;

        for (let i = 0; i < lines.length; i++) {
            const line = lines[i];
            // Strip comments from this line
            const cleanLine = line.replace(/--.*$/, '').replace(/\/\*[\s\S]*?\*\//g, '');

            if (blockedRegex.test(cleanLine)) {
                // Check if already annotated
                if (line.includes('-- remove this line and set the Destination Layer')) {
                    continue;
                }

                // Annotate
                const newText = line + ' -- remove this line and set the Destination Layer and Output table/view name in the cell footer.';
                const range = new vscode.Range(i, 0, i, line.length);
                edit.replace(cell.document.uri, range, newText);
                modified = true;
                break; // Only annotate the first occurrence to avoid noise
            }
        }

        if (modified) {
            await vscode.workspace.applyEdit(edit);
        }
    }

    private async _doExecution(cell: vscode.NotebookCell): Promise<void> {
        const execution = this._controller.createNotebookCellExecution(cell);
        execution.executionOrder = ++this._executionOrder;
        execution.start(Date.now());

        try {
            let sql = cell.document.getText();
            if (!sql.trim()) {
                execution.end(true, Date.now());
                return;
            }

            // Strip trailing semicolons (common issue in notebooks)
            sql = sql.trim().replace(/;+$/, '');

            // Metadata handling
            const dpMeta = cell.metadata.dp || {};
            // Mode inference: If outputName is set, treat as transform. Else analyze.
            // But if mode is explicitly 'analyze', respect it.
            let mode = dpMeta.mode;
            const outputName = dpMeta.outputName;

            if (!mode) {
                mode = outputName ? 'transform' : 'analyze';
            }

            const layer = dpMeta.layer || 'bronze';

            const dbInstance = LocalDuckDB.getInstance();
            await dbInstance.initialize();

            let resultRows: Record<string, unknown>[] = [];
            let outputMessages: string[] = [];

            if (mode === 'transform') {
                // --- TRANSFORM MODE ---

                // 1. Validation
                // Strip comments to avoid false positives
                const cleanSql = sql
                    .replace(/--.*$/gm, '') // Remove line comments
                    .replace(/\/\*[\s\S]*?\*\//g, ''); // Remove block comments

                // Check for blocked keywords (whole keys, case-insensitive)
                const blockedKeywords = ['CREATE', 'INSERT', 'UPDATE', 'DELETE', 'MERGE', 'DROP', 'TRUNCATE', 'ALTER'];
                // Regex: word boundary + keyword + word boundary
                // We join them: \b(CREATE|INSERT|...)\b
                const blockedRegex = new RegExp(`\\b(${blockedKeywords.join('|')})\\b`, 'i');

                if (blockedRegex.test(cleanSql)) {
                    await this._annotateBlockedLine(cell, blockedRegex);
                    throw new Error(formatTransformError(
                        'SQL validation',
                        'Pipeline SQL must be SELECT-only',
                        'Remove CREATE/INSERT/UPDATE/DELETE/MERGE statements and set Destination Layer and Output table in the cell footer'
                    ));
                }

                const allowedLayers = ['bronze', 'silver', 'gold'];
                if (!allowedLayers.includes(layer)) {
                    // Check if it's a reserved schema to give specific error
                    if (['imports', 'data_cache', 'dp_app'].includes(layer)) {
                        throw new Error(formatTransformError(
                            'Layer validation',
                            `Schema '${layer}' is reserved`,
                            'Write transforms to bronze, silver, or gold layers'
                        ));
                    }
                    throw new Error(formatTransformError(
                        'Layer validation',
                        `Layer '${layer}' is not allowed`,
                        'Use bronze, silver, or gold layers'
                    ));
                }

                if (!outputName) {
                    throw new Error(formatTransformError(
                        'Output name validation',
                        'Output name is required',
                        'Set an output name in snake_case format (e.g., customer_orders)'
                    ));
                }
                if (!/^[a-z0-9_]+$/.test(outputName)) {
                    throw new Error(formatTransformError(
                        'Output name validation',
                        `Output name '${outputName}' must be snake_case`,
                        'Use only lowercase letters, numbers, and underscores'
                    ));
                }

                const mat = dpMeta.materialize || 'table';
                const fq = `${layer}.${outputName}`;

                // 2. Materialize (DDL)
                const transformSql = `CREATE OR REPLACE ${mat === 'view' ? 'VIEW' : 'TABLE'} ${fq} AS ${sql}`;

                try {
                    await dbInstance.exec(transformSql);
                } catch (e: unknown) {
                    // Engine Error Mapping (Fallback)
                    const msg = (e instanceof Error ? e.message : String(e)).toLowerCase();
                    const isSyntaxError = msg.includes('syntax error') || msg.includes('parser error');

                    // Check if any blocked keyword is in the error message
                    const mentionsBlocked = blockedKeywords.some(kw => msg.includes(kw.toLowerCase()));

                    if (isSyntaxError && mentionsBlocked) {
                        await this._annotateBlockedLine(cell, blockedRegex);
                        throw new Error(formatTransformError(
                            'SQL validation',
                            'Pipeline SQL must be SELECT-only',
                            'Remove CREATE/INSERT/UPDATE/DELETE/MERGE statements and set Destination Layer and Output table in the cell footer'
                        ));
                    }
                    throw e; // Rethrow original if not mapped
                }

                // 3. Verify Creation
                const verifySql = `SELECT 1 FROM information_schema.tables WHERE table_schema='${layer}' AND table_name='${outputName}' LIMIT 1`;
                const exists = await dbInstance.query(verifySql);
                if (!exists || exists.length === 0) {
                    throw new Error(formatTransformError(
                        'Table creation',
                        `Table '${fq}' not found after creation`,
                        'Check SQL syntax and permissions'
                    ));
                }

                outputMessages.push(`✅ Materialized: ${fq}`);

                // 4. Get Row Count
                try {
                    const countRes = await dbInstance.query(`SELECT COUNT(*) AS n FROM ${fq}`);
                    const rowCount = countRes[0]?.n;
                    outputMessages.push(`Rows: ${rowCount}`);
                } catch (_e) {
                    outputMessages.push(`Rows: (Unknown)`);
                }

                // 5. Preview Results
                const limit = vscode.workspace.getConfiguration('dp').get<number>('transform.previewRows', 200);
                const previewSql = `SELECT * FROM ${fq} LIMIT ${limit}`;
                resultRows = await dbInstance.query(previewSql);

                // 6. Refresh Schema Views
                vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');

                // Introspect schema for autocomplete - ensures new tables are available immediately
                const profile = await getConnection('duck-piper-local-data-work');
                if (profile) {
                    await performIntrospection(profile, true); // silent
                } else {
                    vscode.commands.executeCommand('dp.view.refreshSchemas');
                }

                // Update Lineage (V2)
                const { LineageTracker } = require('../../pipelines/lineageTracker');
                // We need to serialize the CURRENT notebook state to pass to the tracker
                // Construct pseudo-JSON data similar to what's on disk
                const nb = cell.notebook;
                const notebookJson = {
                    version: "0.1",
                    metadata: nb.metadata || {},
                    name: nb.metadata?.dp?.name || 'Notebook',
                    cells: nb.getCells().map(c => ({
                        id: c.metadata?.dp?.id || c.document.uri.fragment, // fallback
                        type: c.kind === vscode.NotebookCellKind.Code ? 'sql' : 'markdown',
                        sql: c.kind === vscode.NotebookCellKind.Code ? c.document.getText() : undefined,
                        content: c.kind === vscode.NotebookCellKind.Markup ? c.document.getText() : undefined,
                        ...c.metadata?.dp // Access dp metadata directly
                    }))
                };

                LineageTracker.getInstance().updateLineageForNotebook(nb.uri, notebookJson)
                    .catch((e: unknown) => Logger.warn('Lineage update failed', e));

                // Refresh Pipelines View
                vscode.commands.executeCommand('dp.pipelinesView.refresh'); // We need to register this command if used, or just refresh provider if global
                // Actually extension.ts has no registered refresh command for pipelines yet.
                // But LineageTracker writes to JSON, so if there's a file watcher, it might pick it up?
                // For now, let's assume manual refresh or next open is fine, but updating JSON is key.

            } else {
                // --- ANALYZE MODE ---
                // Just run the SQL (read-only intent)
                resultRows = await dbInstance.query(sql);
            }

            // Output Rendering
            const safeRows = convertBigIntForSerialization(resultRows) as Record<string, unknown>[];
            const mdTable = this.markdownTable(safeRows);

            const items: vscode.NotebookCellOutputItem[] = [];

            // If we have status messages (Transform mode), prepend them
            if (outputMessages.length > 0) {
                const statusText = outputMessages.join('\n\n');
                // We can append this to MD or send as separate text/plain
                // Combining feels cleaner for notebook output
                items.push(vscode.NotebookCellOutputItem.text(statusText + '\n\n' + mdTable, 'text/markdown'));
            } else {
                items.push(vscode.NotebookCellOutputItem.text(mdTable, 'text/markdown'));
            }

            items.push(vscode.NotebookCellOutputItem.json(safeRows, 'application/json'));

            execution.replaceOutput([new vscode.NotebookCellOutput(items)]);

            execution.end(true, Date.now());
        } catch (err: unknown) {
            const errMsg = err instanceof Error ? err.message : String(err);
            execution.replaceOutput([
                new vscode.NotebookCellOutput([
                    vscode.NotebookCellOutputItem.text(`❌ ${errMsg}`, 'text/plain')
                ])
            ]);
            execution.end(false, Date.now());
        }
    }

    private _executionOrder = 0;

    private markdownTable(rows: Record<string, unknown>[]): string {
        if (!rows || rows.length === 0) return '*No results*';

        const keys = Object.keys(rows[0]);
        if (keys.length === 0) return '*Empty row result*';

        const header = `| ${keys.join(' | ')} |`;
        const separator = `| ${keys.map(() => '---').join(' | ')} |`;
        const body = rows.map(r => {
            // Basic escaping for pipes
            return `| ${keys.map(k => String(r[k]).replace(/\|/g, '\\|')).join(' | ')} |`;
        }).join('\n');

        return `${header}\n${separator}\n${body}`;
    }
}
