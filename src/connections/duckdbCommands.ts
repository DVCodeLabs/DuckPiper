
import * as vscode from 'vscode';
import * as path from 'path';
import { getAdapter } from './adapterFactory';
import { getConnection, getConnectionSecrets } from './connectionStore';
import { LocalDuckDB } from '../transform/localDuckDB';

import { ConnectionItem } from './connectionsView';
import { ExplorerItem } from './explorerView';
import { quoteIdentifier, quoteLiteral, sanitizeIdentifierName } from '../core/sqlUtils';
import { ConnectionProfile, DbDialect } from '../core/types';
import { ColumnItem } from '../transform/localDuckDBView';
import { Logger } from '../core/logger';

/** Shape covering both ExplorerItem (schema panel) and TableItem (data work panel) for delete */
interface DeleteTableItem {
    table?: { name: string; type?: string };
    tableName?: string;
    schemaName?: string;
    connectionId?: string;
    type?: string;
}

export function registerDuckDBCommands(context: vscode.ExtensionContext) {
    context.subscriptions.push(
        vscode.commands.registerCommand('dp.duckdb.importFile', async (item?: ConnectionItem) => importFile(context, item)),
        vscode.commands.registerCommand('dp.duckdb.importCsv', async (item?: ConnectionItem) => importFile(context, item)), // Keep alias for backward compat if needed, or remove
        vscode.commands.registerCommand('dp.duckdb.deleteTable', async (item: ExplorerItem) => deleteTable(context, item)),
        vscode.commands.registerCommand('dp.duckdb.createIndex', async (item: ColumnItem) => createColumnIndex(context, item)),
        vscode.commands.registerCommand('dp.duckdb.dropIndex', async (item: ColumnItem) => dropColumnIndex(context, item))
    );
}

async function importFile(context: vscode.ExtensionContext, item?: ConnectionItem | ExplorerItem) {
    let connId: string | undefined;
    let profile: ConnectionProfile | undefined;

    // Handle ConnectionItem (from connections panel)
    if (item && 'profile' in item && item.profile) {
        connId = item.profile.id;
        profile = item.profile;
    }
    // Handle SchemaItem from Local Data Work panel (localDuckDBView)
    else if (item && 'contextValue' in item && (item as ExplorerItem).contextValue === 'dp.duckdb.schema.imports') {
        connId = 'duck-piper-local-data-work';
        profile = await getConnection(connId);
        if (!profile) {
            vscode.window.showErrorMessage("Duck_Piper connection not found.");
            return;
        }
    }
    // Handle SchemaItem (from schemas panel - imports folder)
    else if (item && 'connectionId' in item && item.connectionId) {
        connId = item.connectionId!;
        profile = await getConnection(connId);
        if (!profile) {
            vscode.window.showErrorMessage(`Connection ${connId} not found.`);
            return;
        }
    }
    // Fallback to active connection
    else {
        connId = context.workspaceState.get<string>("dp.activeConnectionId");
        if (!connId) {
            vscode.window.showErrorMessage("No active connection selected. Please select a DuckDB connection first.");
            return;
        }

        profile = await getConnection(connId);
        if (!profile) {
            vscode.window.showErrorMessage("Active connection profile not found.");
            return;
        }
    }

    if (!profile) return;

    if (profile.dialect !== 'duckdb') {
        vscode.window.showErrorMessage(`Current connection '${profile.name}' is not DuckDB. Import is only supported for DuckDB.`);
        return;
    }

    // 2. Select Files
    const fileUris = await vscode.window.showOpenDialog({
        canSelectFiles: true,
        canSelectFolders: false,
        canSelectMany: true,
        filters: {
            'Data Files': ['csv', 'tsv', 'txt', 'parquet'],
            'All Files': ['*']
        },
        openLabel: 'Import'
    });

    if (!fileUris || fileUris.length === 0) {
        return;
    }

    // 3. Execution Logic
    if (!connId) return;
    const secrets = await getConnectionSecrets(connId);
    const adapter = getAdapter('duckdb');

    await vscode.window.withProgress({
        location: vscode.ProgressLocation.Notification,
        title: "Importing files...",
        cancellable: false
    }, async (progress) => {
        let successCount = 0;
        let _failCount = 0;

        for (const uri of fileUris) {
            const fsPath = uri.fsPath;
            const basename = path.basename(fsPath, path.extname(fsPath));
            const ext = path.extname(fsPath).toLowerCase();

            try {
                // Create Naming Convention (Final Spec)
                // Raw: imports.<filename>
                // Bronze: bronze.raw__file__<filename>


                const cleanBaseName = sanitizeIdentifierName(basename);

                // Allow user to override base name only
                let userBaseName = cleanBaseName;
                if (fileUris.length === 1) {
                    const input = await vscode.window.showInputBox({
                        prompt: `Base name for ${basename}`,
                        value: cleanBaseName
                    });
                    if (input === undefined) return;
                    if (input.trim()) userBaseName = sanitizeIdentifierName(input.trim());
                }

                const q = (id: string) => quoteIdentifier('duckdb', id);
                const rawTableName = `${q('imports')}.${q(userBaseName)}`;
                const viewName = `${q('bronze')}.${q('raw__file__' + userBaseName)}`;

                // Check for existence
                const checkSql = `SELECT count(*) as c FROM information_schema.tables WHERE table_schema = 'imports' AND table_name = ${quoteLiteral(userBaseName)}`;
                try {
                    const checkRes = await adapter.runQuery(profile, secrets, checkSql, { maxRows: 1 });
                    const row = checkRes.rows[0] as Record<string, unknown> | undefined;
                    const count = (row?.['c'] ?? row?.['count'] ?? 0);

                    if (Number(count) > 0) {
                        const overwrite = await vscode.window.showWarningMessage(
                            `Table 'imports.${userBaseName}' already exists. Overwrite?`,
                            { modal: true },
                            "Overwrite",
                            "Cancel"
                        );
                        if (overwrite !== "Overwrite") {
                            continue; // Skip this file
                        }
                    }
                } catch (e) {
                    // ignore check errors, assume doesn't exist or will fail later
                    Logger.warn("Failed to check table existence", e);
                }

                // SQL Execution
                const safePathLiteral = quoteLiteral(fsPath);

                // Determine read function based on extension
                let readFunc = 'read_csv_auto';
                if (ext === '.parquet') {
                    readFunc = 'read_parquet';
                }

                const isLocalDp = profile.id === 'duck-piper-local-data-work' || profile.isLocalDuckPiperCacheDb;

                if (isLocalDp) {
                    // Use LocalDuckDB singleton to ensure view consistency and checkpointing
                    const importSql = `
                        CREATE OR REPLACE TABLE ${rawTableName} AS SELECT * FROM ${readFunc}(${safePathLiteral});
                        CREATE OR REPLACE VIEW ${viewName} AS SELECT * FROM ${rawTableName};
                    `;
                    await LocalDuckDB.getInstance().runUpdate(importSql);

                    // Invalidate Adapter's cache for this connection so it sees the changes next time it's used
                    const { DuckDBAdapter } = require('./adapters/duckdb');
                    DuckDBAdapter.closeConnection(profile.id);

                } else {
                    // 1. Create Raw Import Table (Immutable Snapshot)
                    const createRawSql = `CREATE OR REPLACE TABLE ${rawTableName} AS SELECT * FROM ${readFunc}(${safePathLiteral})`;
                    await adapter.runQuery(profile, secrets, createRawSql, { maxRows: 10000 });

                    // 2. Create Bronze View (Canonical Pipeline Entry)
                    const createViewSql = `CREATE OR REPLACE VIEW ${viewName} AS SELECT * FROM ${rawTableName}`;
                    await adapter.runQuery(profile, secrets, createViewSql, { maxRows: 10000 });
                }

                progress.report({ message: `Imported ${basename} -> ${viewName}` });
                successCount++;
            } catch (err: unknown) {
                Logger.error(`Import failed for ${fsPath}`, err);
                const errMsg = err instanceof Error ? err.message : String(err);
                vscode.window.showErrorMessage(`Failed to import ${basename}: ${errMsg}`);
                _failCount++;
            }
        }

        if (successCount > 0) {
            vscode.window.showInformationMessage(`Successfully imported ${successCount} file(s) into DuckDB.`);

            // Re-introspect first
            const { performIntrospection } = require('./connectionCommands');
            await performIntrospection(profile, true);

            // Refreshes
            // Refreshes
            vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');
            vscode.commands.executeCommand('dp.view.refreshSchemas');
        }
    });
}
async function deleteTable(context: vscode.ExtensionContext, item: DeleteTableItem) {
    if (!item) return;

    // Handle both SchemaItem (from Schema panel) and TableItem (from Data Work panel)
    let tableName: string;
    let schemaName: string;
    let connectionId: string | undefined;
    let isView = false; // Track if this is a view vs table

    if (item.table && item.table.name) {
        // SchemaItem from Schema panel
        tableName = item.table.name;
        schemaName = item.schemaName || 'main';
        connectionId = item.connectionId;
        // Check if table type is view
        isView = item.table.type === 'VIEW' || item.table.type === 'view';
    } else if (item.tableName) {
        // TableItem from Data Work panel
        tableName = item.tableName;
        schemaName = item.schemaName || 'main';
        connectionId = undefined; // Will use local DuckDB
        // Check type property on TableItem
        isView = item.type === 'VIEW' || item.type === 'view';
    } else {
        vscode.window.showErrorMessage("Cannot delete: invalid item.");
        return;
    }

    // Safety validation
    if (!tableName || tableName.trim() === '') {
        vscode.window.showErrorMessage("Cannot delete: table name is empty.");
        return;
    }
    if (!schemaName || schemaName.trim() === '') {
        vscode.window.showErrorMessage("Cannot delete: schema name is empty.");
        return;
    }

    // Use quoted identifiers for safety - MUST determine dialect first
    // For local DuckDB, use 'duckdb'. For Schema panel, resolve from profile.
    let dialect: DbDialect = 'duckdb'; // default for local
    if (connectionId) {
        const profile = await getConnection(connectionId);
        if (profile) {
            dialect = profile.dialect;
        }
    }

    const q = (id: string) => quoteIdentifier(dialect, id);
    const quotedTable = `${q(schemaName)}.${q(tableName)}`;

    // Use DROP VIEW for views, DROP TABLE for tables
    const dropType = isView ? 'VIEW' : 'TABLE';
    const dropSql = `DROP ${dropType} IF EXISTS ${quotedTable}`;

    // Check if this is a managed layer that affects lineage
    const isLineageLayer = ['bronze', 'silver', 'gold'].includes(schemaName);
    const typeName = isView ? 'view' : 'table';

    let warningMessage: string;
    let warningDetail: string | undefined;

    if (isLineageLayer) {
        warningMessage = [
            `Delete ${schemaName}.${tableName}?`,
            '',
            'This action will:',
            `• Drop the ${typeName} from DuckDB`,
            `• Mark this ${typeName} as "deleted" in all pipeline lineage files`,
            `• Downstream tables that depend on this may show broken lineage`,
            '',
            'This cannot be undone.'
        ].join('\n');
        warningDetail = `Deleting this ${typeName} will affect any pipelines that reference it.`;
    } else {
        warningMessage = `Delete ${typeName}?\n\nSQL: ${dropSql}`;
    }

    // Show confirmation with appropriate warning
    const check = await vscode.window.showWarningMessage(
        warningMessage,
        { modal: true, detail: warningDetail },
        "Delete"
    );
    if (check !== "Delete") return;

    // Determine if this is a local DuckDB item (from Data Work panel) or Schema panel item
    const isLocalDuckDBItem = !connectionId;

    try {
        const statements: string[] = [];
        statements.push(dropSql);

        let msg = `Deleted ${typeName} ${quotedTable}`;

        // For imports, also drop the associated bronze view
        if (schemaName === 'imports') {
            const bronzeView = `${q('bronze')}.${q('raw__file__' + tableName)}`;
            statements.push(`DROP VIEW IF EXISTS ${bronzeView}`);
            msg += ` and view ${bronzeView}`;
        }
        // For data_cache, also drop the associated bronze view
        if (schemaName === 'data_cache') {
            const bronzeView = `${q('bronze')}.${q('raw__' + tableName)}`;
            statements.push(`DROP VIEW IF EXISTS ${bronzeView}`);
            msg += ` and view ${bronzeView}`;
        }

        if (isLocalDuckDBItem) {
            // Data Work panel item - use LocalDuckDB directly
            const sql = statements.join('; ');
            await LocalDuckDB.getInstance().runUpdate(sql);

            // Invalidate adapter cache
            const { DuckDBAdapter } = require('./adapters/duckdb');
            DuckDBAdapter.closeAllConnections?.();
        } else {
            // Schema panel item - use connection profile
            const connId = connectionId || context.workspaceState.get<string>("dp.activeConnectionId");
            if (!connId) {
                vscode.window.showErrorMessage("No connection context found.");
                return;
            }

            const profile = await getConnection(connId);
            if (!profile) {
                vscode.window.showErrorMessage(`Connection ${connId} not found.`);
                return;
            }

            const secrets = await getConnectionSecrets(connId);
            const adapter = getAdapter(profile.dialect);

            if (profile.isLocalDuckPiperCacheDb) {
                const sql = statements.join('; ');
                await LocalDuckDB.getInstance().runUpdate(sql);
                const { DuckDBAdapter } = require('./adapters/duckdb');
                DuckDBAdapter.closeConnection(profile.id);
            } else {
                for (const stmt of statements) {
                    await adapter.runQuery(profile, secrets, stmt, { maxRows: 1 });
                }
            }
        }

        vscode.window.showInformationMessage(msg);

        // Update lineage for managed layers
        if (isLineageLayer) {
            try {
                const fqn = `${schemaName}.${tableName}`;
                const { PipelineRepository } = require('../pipelines/pipelineRepository');
                await PipelineRepository.getInstance().updateTableStatusInAllPipelines(fqn, 'deleted', 'user');
            } catch (lineageErr) {
                Logger.warn('[DeleteTable] Failed to update lineage:', lineageErr);
            }
        }

        // Refresh views
        await vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');
        await vscode.commands.executeCommand('dp.view.refreshSchemas');
    } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Delete failed: ${msg}`);
    }
}

async function createColumnIndex(context: vscode.ExtensionContext, item: ColumnItem | undefined) {
    // item is a ColumnItem from localDuckDBView
    if (!item || !item.schemaName || !item.tableName || !item.columnName) {
        vscode.window.showWarningMessage("Please right-click on a column in the Data Work panel.");
        return;
    }

    const { schemaName, tableName, columnName } = item;

    try {
        await LocalDuckDB.getInstance().createIndex(schemaName, tableName, columnName);
        vscode.window.showInformationMessage(`Created index on ${schemaName}.${tableName}.${columnName}`);
    } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Failed to create index: ${msg}`);
    }
}

async function dropColumnIndex(context: vscode.ExtensionContext, item: ColumnItem | undefined) {
    // item is a ColumnItem from localDuckDBView
    if (!item || !item.schemaName || !item.tableName || !item.columnName) {
        vscode.window.showWarningMessage("Please right-click on a column in the Data Work panel.");
        return;
    }

    const { schemaName, tableName, columnName } = item;
    const indexName = `idx_${tableName}_${columnName}`;

    const confirm = await vscode.window.showWarningMessage(
        `Drop index "${indexName}" on ${columnName}?`,
        { modal: true },
        "Drop Index"
    );

    if (confirm !== "Drop Index") return;

    try {
        await LocalDuckDB.getInstance().dropIndex(schemaName, tableName, columnName);
        vscode.window.showInformationMessage(`Dropped index on ${schemaName}.${tableName}.${columnName}`);
    } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Failed to drop index: ${msg}`);
    }
}
