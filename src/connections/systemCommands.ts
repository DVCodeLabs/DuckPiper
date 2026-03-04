import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import { getAdapter } from './adapterFactory';
import { getConnection, getConnectionSecrets } from './connectionStore';
import { LocalDuckDB } from '../transform/localDuckDB';
import { ConnectionProfile } from '../core/types';
import { queryIndex } from '../queryLibrary/queryIndex';
import { quoteLiteral } from '../core/sqlUtils';
import { Logger } from '../core/logger';

/** Shape for loaded results passed from the webview results panel */
interface CacheableResults {
    rows: Record<string, unknown>[];
    connectionId?: string;
}

/** Shape for ExplorerItem fields used by saveTableToDuckDB */
interface CacheTableItem {
    table?: { name: string; primaryKey?: string[] };
    schemaName?: string;
    connectionId?: string;
}

// Reserved schemas as per spec
const RESERVED_SCHEMAS = ['imports', 'data_cache', 'bronze', 'silver', 'gold', 'dp_app'];

// Helper to sanitize names
function slugify(text: string): string {
    return text.toString().toLowerCase()
        .replace(/\s+/g, '_')           // Replace spaces with -
        .replace(/[^\w\-]+/g, '_')      // Remove all non-word chars
        .replace(/\-\-+/g, '_')         // Replace multiple - with single -
        .replace(/^-+/, '')             // Trim - from start of text
        .replace(/-+$/, '');            // Trim - from end of text
}

// Helper: Check Visibility Rules
export function isCacheActionVisible(profile: ConnectionProfile, schemaName?: string): boolean {
    if (profile.isLocalDuckPiperCacheDb) {
        return false; // Hide on local cache itself
    }
    if (schemaName && RESERVED_SCHEMAS.includes(schemaName)) {
        return false; // Hide on reserved schemas
    }
    return true;
}

// Helper: Quote Identifiers based on Dialect
function quoteIdentifier(name: string, dialect: string): string {
    if (dialect === 'mysql' || dialect === 'mariadb') {
        return `\`${name}\``;
    }
    return `"${name}"`;
}

function buildTableFqn(schemaName: string | undefined, tableName: string, dialect: string): string {
    if (!schemaName) {
        return quoteIdentifier(tableName, dialect);
    }

    // Snowflake introspection may represent schema as DATABASE.SCHEMA.
    // Build a 3-part identifier for table-scoped actions in that case.
    if (dialect === 'snowflake' && schemaName.includes('.')) {
        const parts = schemaName.split('.');
        const database = parts.shift() ?? '';
        const schema = parts.join('.');
        if (database && schema) {
            return `${quoteIdentifier(database, dialect)}.${quoteIdentifier(schema, dialect)}.${quoteIdentifier(tableName, dialect)}`;
        }
    }

    return `${quoteIdentifier(schemaName, dialect)}.${quoteIdentifier(tableName, dialect)}`;
}

// --- COMMAND 1: Save System from SQL (CodeLens / Editor)
export async function saveQueryToDuckDB(context: vscode.ExtensionContext) {
    const editor = vscode.window.activeTextEditor;
    if (!editor) {
        vscode.window.showErrorMessage("No active SQL editor found.");
        return;
    }

    // 1. Try resolving connection from Query Index (Specific file context)
    let connId: string | undefined;
    const entry = queryIndex.getEntry(editor.document.uri);
    if (entry && entry.connectionId) {
        connId = entry.connectionId;
    }

    // 2. Fallback to Active Connection
    if (!connId) {
        connId = context.workspaceState.get<string>("dp.activeConnectionId");
    }

    if (!connId) {
        vscode.window.showErrorMessage("No connection selected for this query.");
        return;
    }

    const profile = await getConnection(connId);
    if (!profile) {
        vscode.window.showErrorMessage(`Connection ${connId} not found.`);
        return;
    }

    if (!isCacheActionVisible(profile)) {
        vscode.window.showWarningMessage("System save is not allowed for this connection (it is the local system).");
        return;
    }

    const selection = editor.selection;
    let sql = selection.isEmpty ? editor.document.getText() : editor.document.getText(selection);
    if (!sql.trim()) {
        vscode.window.showWarningMessage("No SQL to run.");
        return;
    }

    // Default name from filename
    const fsPath = editor.document.uri.fsPath;
    const baseName = path.basename(fsPath, path.extname(fsPath));
    const defaultName = baseName === 'Untitled-1' ? 'query_result' : slugify(baseName);

    await executeCachePipeline(context, profile, sql, defaultName, 'query');
}

// --- COMMAND 2: Save System Loaded Results (Results Panel)
// Expects QueryResult object passed from webview
export async function saveLoadedResultsToDuckDB(context: vscode.ExtensionContext, results: CacheableResults | undefined) {
    // If called from command palette without args, we can't do much unless we track active results globally
    if (!results || !results.rows) {
        vscode.window.showErrorMessage("No results provided to cache.");
        return;
    }

    // We need the source connection ID to name it correctly
    // The results object might not have it, so we rely on active or passed metadata
    // Ideally ResultsPanel passes { rows, columns, connectionId, ... }
    const connId = results.connectionId || context.workspaceState.get<string>("dp.activeConnectionId");
    if (!connId) {
        vscode.window.showErrorMessage("Could not determine source connection for these results.");
        return;
    }
    const profile = await getConnection(connId);
    if (!profile) {
        vscode.window.showErrorMessage("Source connection profile not found.");
        return;
    }

    const defaultName = 'loaded_results';
    await executeCachePipeline(context, profile, results, defaultName, 'loaded_rows');
}

// --- COMMAND 3: Save System Table (Schema Tree)
// Expects tree item with table info
export async function saveTableToDuckDB(context: vscode.ExtensionContext, item: CacheTableItem | undefined) {
    // item is ExplorerItem from explorerView
    if (!item || !item.table || !item.schemaName) return;

    const connId = item.connectionId || context.workspaceState.get<string>("dp.activeConnectionId");
    if (!connId) return;
    const profile = await getConnection(connId);
    if (!profile) return;

    if (!isCacheActionVisible(profile, item.schemaName)) {
        vscode.window.showWarningMessage("Caching is not allowed for this table (reserved schema).");
        return;
    }

    // Construct SELECT * query
    const tableFqn = buildTableFqn(item.schemaName, item.table.name, profile.dialect);
    const sql = `SELECT * FROM ${tableFqn}`;

    // Pass primary key info if available
    const primaryKey: string[] | undefined = item.table.primaryKey;

    await executeCachePipeline(context, profile, sql, item.table.name, 'table', primaryKey);
}

// --- CORE PIPELINE ---
// Source can be string (SQL) or object (Rows)
async function executeCachePipeline(
    context: vscode.ExtensionContext,
    profile: ConnectionProfile,
    source: string | CacheableResults,
    defaultName: string,
    mode: 'query' | 'loaded_rows' | 'table',
    primaryKey?: string[] // Optional primary key columns for index creation
) {
    // 1. Prompt for Name
    const inputName = await vscode.window.showInputBox({
        prompt: "Name for cached table",
        value: defaultName,
        placeHolder: "e.g. customers"
    });
    if (!inputName) return;

    const sourceSlug = slugify(profile.name);
    const sanitizedName = slugify(inputName);

    // 2. Check Overwrite
    const dbInstance = LocalDuckDB.getInstance();
    const catalog = dbInstance.getCatalogName(); // e.g. "dp"

    const finalTableName = `${sourceSlug}__${sanitizedName}`; // system.<source>__<name>

    // FIX: Fully qualify with catalog to avoid ambiguity with internal "system" catalog
    const datacacheTable = `"${catalog}"."data_cache".${finalTableName}`;
    const bronzeView = `"${catalog}"."bronze".raw__${finalTableName}`;

    // Check existence via shared instance logic
    const exists = await dbInstance.checkTableExists('data_cache', finalTableName);

    const config = vscode.workspace.getConfiguration('dp');
    const overwriteDefault = config.get<boolean>('system.overwriteByDefault', true);

    if (exists && !overwriteDefault) {
        const choice = await vscode.window.showWarningMessage(
            `Table ${datacacheTable} already exists. Replace it?`,
            { modal: true },
            "Replace",
            "Cancel"
        );
        if (choice !== "Replace") return;
    }

    // 3. Execution
    const maxRows = config.get<number>('system.maxRows', 100000);

    await vscode.window.withProgress({
        location: vscode.ProgressLocation.Notification,
        title: `Caching to ${datacacheTable}...`,
        cancellable: false
    }, async (progress) => {
        try {
            let rows: Record<string, unknown>[] = [];

            // A. Get Data
            if (mode === 'loaded_rows' && typeof source !== 'string') {
                // Buffer already loaded
                rows = source.rows || [];
                if (rows.length === 0) {
                    vscode.window.showWarningMessage("No rows to cache.");
                    return;
                }
            } else {
                // Run Query
                const sql = source as string;
                let finalSql = sql;

                // Wrap limit if not present (simple heuristic)
                if (!/limit\s+\d+/i.test(sql) && !/top\s+\d+/i.test(sql) && !/fetch\s+first/i.test(sql)) {
                    // Remove trailing semicolons to avoid syntax errors when wrapping
                    const cleanSql = sql.replace(/;\s*$/, '').trim();
                    finalSql = `SELECT * FROM (${cleanSql}) AS dp_cache_sub LIMIT ${maxRows}`;
                }

                const secrets = await getConnectionSecrets(profile.id);
                const adapter = getAdapter(profile.dialect);
                progress.report({ message: "Running source query...", increment: 20 });

                const result = await adapter.runQuery(profile, secrets, finalSql, { maxRows });
                rows = result.rows as Record<string, unknown>[];
            }

            if (!rows || rows.length === 0) {
                vscode.window.showInformationMessage("Query returned no rows.");
                return;
            }

            // B. Write to Temp CSV (Type agnostic intermediate)
            progress.report({ message: "Staging data...", increment: 50 });
            const tempFile = path.join(context.globalStorageUri.fsPath, `cache_${finalTableName}_${Date.now()}.csv`);
            if (!fs.existsSync(context.globalStorageUri.fsPath)) {
                fs.mkdirSync(context.globalStorageUri.fsPath, { recursive: true });
            }

            await writeCsv(rows, tempFile);

            // C. Load to DuckDB
            progress.report({ message: "Loading into DuckDB...", increment: 70 });
            const safePathLiteral = quoteLiteral(tempFile);

            // Execute via LocalDuckDB instance to ensure correct path/locking
            // Single transaction script
            const loadSql = `
                CREATE OR REPLACE TABLE ${datacacheTable} AS SELECT * FROM read_csv_auto(${safePathLiteral});
                CREATE OR REPLACE VIEW ${bronzeView} AS SELECT * FROM ${datacacheTable};
            `;

            await dbInstance.runUpdate(loadSql);

            // D. Create index on primary key if available
            if (primaryKey && primaryKey.length > 0) {
                progress.report({ message: "Creating primary key index..." });
                const indexName = `idx_${finalTableName}_pk`;
                const quotedCols = primaryKey.map(c => `"${c}"`).join(', ');
                const indexSql = `CREATE INDEX IF NOT EXISTS "${indexName}" ON ${datacacheTable} (${quotedCols});`;
                try {
                    await dbInstance.runUpdate(indexSql);
                } catch (indexErr: unknown) {
                    // Log but don't fail - index is optional enhancement
                    const msg = indexErr instanceof Error ? indexErr.message : String(indexErr);
                    Logger.warn(`Could not create index: ${msg}`);
                }
            }

            // CRITICAL: Invalidate the adapter's cached connection so it sees the new table
            // The adapter caches connections, and that cached connection has a stale catalog
            const { DuckDBAdapter } = require('./adapters/duckdb');
            const localConnProfile = await getConnection('duck-piper-local-data-work');
            if (localConnProfile) {
                DuckDBAdapter.closeConnection(localConnProfile.id);

            }

            // Cleanup
            fs.unlinkSync(tempFile);

            // D. Open Verification
            await vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');
            await vscode.commands.executeCommand('dp.view.refreshSchemas');

            const verifySql = `SELECT * FROM ${datacacheTable} LIMIT 100;`;
            const doc = await vscode.workspace.openTextDocument({ content: verifySql, language: 'sql' });

            // Set connection BEFORE showing the document to prevent
            // onDidChangeActiveTextEditor from overwriting it with the
            // global active connection.
            const localConnectionId = 'duck-piper-local-data-work';
            await vscode.commands.executeCommand('dp.sql.setConnectionForDoc', doc.uri, localConnectionId);
            await vscode.window.showTextDocument(doc, { preview: false });

            vscode.window.setStatusBarMessage(`Bronze alias updated: ${bronzeView}`, 5000);
            vscode.window.showInformationMessage(`Cached ${rows.length} rows -> ${datacacheTable}`);

        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            vscode.window.showErrorMessage(`Cache failed: ${msg}`);
        }
    });

}

async function writeCsv(rows: Record<string, unknown>[], filePath: string): Promise<void> {
    const headers = Object.keys(rows[0]);
    const csvHeader = headers.join(',') + '\n';
    const fileStream = fs.createWriteStream(filePath);
    fileStream.write(csvHeader);

    rows.forEach(row => {
        const line = headers.map(h => {
            let val = row[h];
            if (val === null || val === undefined) return '';
            const s = String(val).replace(/"/g, '""');
            if (/[",\n\r]/.test(s)) return `"${s}"`;
            return s;
        }).join(',') + '\n';
        fileStream.write(line);
    });
    fileStream.end();
    return new Promise((resolve, reject) => {
        fileStream.on('finish', () => resolve());
        fileStream.on('error', (err) => reject(err));
    });
}
