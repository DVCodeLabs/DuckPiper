import * as vscode from 'vscode';

import { ConnectionProfile, ConnectionSecrets, ColumnModel, DbDialect, ForeignKeyModel, QueryColumn, RoutineModel, SchemaIntrospection, TableModel } from '../core/types';
import { DbAdapter } from './adapters/adapter';
import { ConnectionsViewProvider } from './connectionsView';
import {
    getConnectionSecrets,
    saveConnectionSecrets,
    deleteConnection
} from './connectionStore';
import { getAdapter } from './adapterFactory';
import { ConnectionItem } from './connectionsView';
import { saveSchema } from '../schema/schemaStore';
import { ConnectionFormView } from '../ui/connectionFormView';
import * as path from 'path';
import { defaultExportTable } from './exportHelper';
import { formatDatabaseConnectionError } from './connectionErrors';
import { Logger } from '../core/logger';
import { ErrorHandler, ErrorSeverity, formatConnectionError, formatGeneralError } from '../core/errorHandler';
import { ProviderRegistry } from './providerRegistry';
import { generateBackupSql, BackupTableInfo, BackupViewInfo, BackupRoutineInfo } from '../core/backupSchemaSql';
import { quoteIdentifier, resolveEffectiveSqlDialect } from '../core/sqlUtils';
import { BackupSchemaView, BackupSchemaContext } from '../ui/backupSchemaView';
import { LocalDuckDB } from '../transform/localDuckDB';
import * as fs from 'fs';

/** Tree item representing a table with schema/connection context (from Schema panel or Data Work panel) */
interface TableExportItem {
    table?: { name: string };
    tableName?: string;
    schemaName?: string;
    connectionId?: string;
}

/** Tree item for backup schema command with optional introspection/schema context */
interface BackupSchemaItem {
    schemaName?: string;
    schemaModel?: { name: string; tables: TableModel[]; views?: TableModel[]; procedures?: RoutineModel[]; functions?: RoutineModel[] };
    connectionId?: string;
    introspection?: SchemaIntrospection;
}

/** A query result row (object form with arbitrary keys) */
type QueryRow = Record<string, unknown>;

/** Tree item for introspect command (ConnectionItem or schema-like item) */
interface IntrospectTreeItem {
    profile?: ConnectionProfile;
    connectionId?: string;
}

export function registerConnectionCommands(context: vscode.ExtensionContext, connectionsProvider: ConnectionsViewProvider | { refresh(): void }) {
    context.subscriptions.push(
        vscode.commands.registerCommand('dp.connection.add', () => addConnection(context.extensionUri)),
        vscode.commands.registerCommand('dp.connection.edit', (item?: ConnectionItem) => editConnectionCommand(item, context.extensionUri)),
        vscode.commands.registerCommand('dp.connection.test', (item?: ConnectionItem) => testConnectionCommand(item)),
        vscode.commands.registerCommand('dp.connection.introspect', (item?: ConnectionItem) => introspectConnectionCommand(item)),
        vscode.commands.registerCommand('dp.connection.remove', (item?: ConnectionItem) => removeConnectionCommand(item, connectionsProvider)),
        vscode.commands.registerCommand('dp.connection.exportToCsv', (item?: TableExportItem) => exportTableCommand(item, context)),
        vscode.commands.registerCommand('dp.schema.backupSchema', (item?: BackupSchemaItem) => backupSchemaCommand(item, context.extensionUri))
    );
}

/**
 * Open the new Connection Form Webview
 */
async function addConnection(extensionUri: vscode.Uri) {
    ConnectionFormView.render(extensionUri);
}

async function editConnectionCommand(item: ConnectionItem | undefined, extensionUri: vscode.Uri) {
    if (!item?.profile) return;
    const secrets = await getConnectionSecrets(item.profile.id);
    ConnectionFormView.render(extensionUri, item.profile, secrets);
}

async function testConnectionCommand(item?: ConnectionItem) {
    let profile: ConnectionProfile | undefined = item?.profile;

    if (!profile) {
        return vscode.window.showWarningMessage("Please select a connection to test.");
    }

    const secrets = await ensureConnectionSecrets(profile);
    if (!secrets) return; // cancelled or failed

    await testConnectionInternal(profile, secrets);
}

async function testConnectionInternal(profile: ConnectionProfile, secrets: ConnectionSecrets) {
    try {
        const adapter = getAdapter(profile.dialect);
        await adapter.testConnection(profile, secrets);
        await vscode.window.showInformationMessage(`Connected to '${profile.name}' successfully!`, { modal: true });
    } catch (e: unknown) {
        const errorMessage = formatConnectionError(
            'Connection test',
            formatDatabaseConnectionError(e)
        );
        await ErrorHandler.handle(e, {
            severity: ErrorSeverity.Error,
            userMessage: errorMessage,
            context: `Test Connection: ${profile.name}`,
            modal: true
        });
    }
}

/**
 * Evaluate a field visibility rule against profile state (server-side equivalent
 * of the form's isFieldVisible).
 */
function isFieldVisibleForProfile(
    field: { visibleWhen?: import('../core/types').DPConnectionFieldVisibility },
    profile: ConnectionProfile
): boolean {
    const rule = field.visibleWhen;
    if (!rule) return true;

    const value = (profile as unknown as Record<string, unknown>)[rule.key];

    let pass = true;
    if (rule.truthy !== undefined) {
        pass = Boolean(value) === rule.truthy;
    } else if (rule.equals !== undefined) {
        pass = value === rule.equals;
    } else if (rule.notEquals !== undefined) {
        pass = value !== rule.notEquals;
    }

    if (pass && rule.and) {
        return rule.and.every((sub) => isFieldVisibleForProfile({ visibleWhen: sub }, profile));
    }

    return pass;
}

/**
 * Helper to ensure we have secrets for a connection.
 * If storage mode is 'session' and secrets are missing, prompts the user
 * for all visible required secret fields.
 */
export async function ensureConnectionSecrets(profile: ConnectionProfile): Promise<ConnectionSecrets | undefined> {
    const secrets = await getConnectionSecrets(profile.id);
    const provider = ProviderRegistry.getInstance().getProvider(profile.dialect);

    if (profile.credentialStorageMode === 'session' && provider) {
        const secretFields = provider.formSchema.fields.filter((field) => {
            const storage = field.storage ?? 'profile';
            return storage === 'secrets' && field.required && isFieldVisibleForProfile(field, profile);
        });

        let changed = false;
        for (const field of secretFields) {
            const existing = (secrets as Record<string, unknown>)[field.key];
            if (existing) continue;

            const value = await vscode.window.showInputBox({
                password: field.type === 'password',
                prompt: `Enter ${field.label} for '${profile.name}'`,
                placeHolder: field.label,
                ignoreFocusOut: true
            });

            if (value === undefined) {
                return undefined; // User cancelled
            }

            if (value) {
                (secrets as Record<string, unknown>)[field.key] = value;
                changed = true;
            }
        }

        if (changed) {
            await saveConnectionSecrets(profile.id, secrets, 'session');
        }
    }
    return secrets;
}

// Exported helper for background introspection
export async function performIntrospection(profile: ConnectionProfile, silent = false): Promise<void> {
    const doIntrospect = async () => {
        const secrets = await ensureConnectionSecrets(profile);
        if (!secrets && !silent) {
            throw new Error(formatGeneralError(
                'Credentials required',
                'Missing credentials',
                'Please provide connection credentials'
            ));
        }
        // If silent and no secrets, we might fail downstream, but usually silent is for auto-refresh where we don't want to prompt.
        // If silent, we typically skip prompting. But ensureConnectionSecrets prompts.
        // We should arguably NOT prompt if silent is true?
        // But if the user triggered it, they expect it. If it's auto-refresh, maybe not.
        // Let's rely on ensureConnectionSecrets for now. If it prompts during auto-refresh that's bad.
        // Refactoring ensureConnectionSecrets to support 'silent' or handle it here?
        // Actually, if silent is true (auto-refresh), we should just try getting secrets without prompting.

        let finalSecrets = secrets;
        if (silent) {
            finalSecrets = await getConnectionSecrets(profile.id);
        }
        if (!finalSecrets) return;

        const adapter = getAdapter(profile.dialect);
        const schema = await adapter.introspectSchema(profile, finalSecrets);
        await saveSchema(schema);
        vscode.commands.executeCommand('dp.view.refreshSchemas', true);
    };

    if (silent) {
        try {
            await doIntrospect();
        } catch (e: unknown) {
            Logger.warn(`Background introspection failed for ${profile.name}:`, e);
            const msg = e instanceof Error ? e.message : 'Unknown error';
            vscode.window.showWarningMessage(
                `Schema introspection failed for '${profile.name}': ${msg}`
            );
        }
    } else {
        await vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: `Introspecting ${profile.name}...`,
            cancellable: false
        }, async () => {
            try {
                await doIntrospect();
                vscode.window.showInformationMessage(`Introspection complete for ${profile.name}`);
            } catch (e: unknown) {
                await ErrorHandler.handle(e, {
                    severity: ErrorSeverity.Error,
                    userMessage: formatConnectionError(
                        'Schema introspection',
                        ErrorHandler.extractErrorMessage(e),
                        'Check connection settings and try again'
                    ),
                    context: `Introspect: ${profile.name}`
                });
            }
        });
    }
}

async function introspectConnectionCommand(item?: IntrospectTreeItem) {
    let profile: ConnectionProfile | undefined;

    if (item?.profile) {
        profile = item.profile;
    } else if (item?.connectionId) {
        // Handle SchemaItem or similar
        const { getConnection } = require('./connectionStore');
        profile = await getConnection(item.connectionId);
    }

    if (!profile) {
        return vscode.window.showWarningMessage("Please select a connection to introspect.");
    }
    await performIntrospection(profile, false);
}

async function removeConnectionCommand(item: ConnectionItem | undefined, provider: { refresh(): void }) {
    if (!item?.profile) return;

    const choice = await vscode.window.showWarningMessage(
        `Are you sure you want to delete '${item.profile.name}'?`,
        { modal: true },
        'Delete'
    );

    if (choice === 'Delete') {
        await deleteConnection(item.profile.id);

        const { deleteSchema } = require('../schema/schemaStore');
        await deleteSchema(item.profile.id, item.profile.name);

        provider.refresh();
        vscode.commands.executeCommand('dp.view.refreshSchemas');
        vscode.window.showInformationMessage("Connection deleted.");
    }
}

async function exportTableCommand(item: TableExportItem | undefined, _context: vscode.ExtensionContext) {
    // 1. Resolve Profile and Table
    let profile: ConnectionProfile | undefined;
    let schemaName: string | undefined;
    let tableName: string | undefined;

    // Handle SchemaItem (from Schema panel)
    if (item && item.table && item.table.name) {
        schemaName = item.schemaName || 'main'; // Default assumption
        tableName = item.table.name;

        // Resolve profile
        if (item.connectionId) {
            const { getConnection } = require('./connectionStore');
            profile = await getConnection(item.connectionId);
        }
    } else if (item && item.tableName) {
        // Handle TableItem (from Data Work / Local DuckDB)
        schemaName = item.schemaName;
        tableName = item.tableName;
        if (item.connectionId) {
            const { getConnection } = require('./connectionStore');
            profile = await getConnection(item.connectionId);
        } else {
            // Local DuckDB Item (Data Work) - Use 'duck-piper-local-data-work' profile
            const { getConnection } = require('./connectionStore');
            profile = await getConnection('duck-piper-local-data-work');
        }
    }

    if (!profile || !tableName) {
        // Fallback: Active Connection?
        // If triggered from command palette without context, we might prompt.
        // For now, require context or show error.
        vscode.window.showErrorMessage("Please select a table to export.");
        return;
    }

    // 2. Prompt for location - default to Downloads folder
    const defaultName = `${tableName}.csv`;
    const homeDir = require('os').homedir();
    const downloadsDir = path.join(homeDir, 'Downloads');
    const fs = require('fs');
    let defaultDir = homeDir;
    if (fs.existsSync(downloadsDir)) {
        defaultDir = downloadsDir;
    }
    const defaultPath = path.join(defaultDir, defaultName);

    const uri = await vscode.window.showSaveDialog({
        saveLabel: 'Export CSV',
        filters: { 'CSV Files': ['csv'] },
        defaultUri: vscode.Uri.file(defaultPath)
    });

    if (!uri) return; // User cancelled

    // 3. Get Secrets
    const secrets = await ensureConnectionSecrets(profile);
    if (!secrets) return;

    // 4. Invoke Export
    const adapter = getAdapter(profile.dialect);

    await vscode.window.withProgress({
        location: vscode.ProgressLocation.Notification,
        title: `Exporting ${profile.name}.${schemaName}.${tableName}...`,
        cancellable: false
    }, async () => {
        try {
            if (adapter.exportTable) {
                // Optimized path
                await adapter.exportTable(profile!, secrets, schemaName!, tableName!, 'csv', uri);
            } else {
                // Generic path
                await defaultExportTable(profile!, secrets, schemaName!, tableName!, 'csv', uri);
            }
            vscode.window.showInformationMessage(`Successfully exported ${tableName} to ${path.basename(uri.fsPath)}`);
        } catch (e: unknown) {
            await ErrorHandler.handle(e, {
                severity: ErrorSeverity.Error,
                userMessage: formatConnectionError(
                    'Table export',
                    ErrorHandler.extractErrorMessage(e),
                    'Check connection and permissions'
                ),
                context: `Export: ${tableName}`
            });
        }
    });

}

// ── View/Routine Definition Fetchers ──

async function fetchViewDefinitions(
    adapter: DbAdapter, profile: ConnectionProfile, secrets: ConnectionSecrets,
    dialect: DbDialect, schemaName: string, views: TableModel[]
): Promise<BackupViewInfo[]> {
    if (views.length === 0) return [];
    const results: BackupViewInfo[] = [];
    const q = (id: string) => quoteIdentifier(dialect, id);

    for (const view of views) {
        try {
            let sql: string;
            switch (dialect) {
                case 'mysql':
                    sql = `SHOW CREATE VIEW ${q(schemaName)}.${q(view.name)}`;
                    break;
                case 'postgres':
                    sql = `SELECT pg_get_viewdef('${schemaName}.${view.name}'::regclass, true) AS definition`;
                    break;
                case 'duckdb':
                    sql = `SELECT sql FROM duckdb_views() WHERE schema_name = '${schemaName}' AND view_name = '${view.name}'`;
                    break;
                default:
                    sql = `SELECT view_definition FROM information_schema.views WHERE table_schema = '${schemaName}' AND table_name = '${view.name}'`;
                    break;
            }
            const qr = await adapter.runQuery(profile, secrets, sql, { maxRows: 1 });
            if (qr.rows.length > 0) {
                const row = qr.rows[0] as QueryRow;
                const def = extractViewDefinition(dialect, schemaName, view.name, row);
                if (def) results.push({ name: view.name, definition: def });
            }
        } catch (e) {
            Logger.warn(`Failed to fetch definition for view ${schemaName}.${view.name}`, e);
        }
    }
    return results;
}

async function fetchLocalDuckDBViewDefinitions(
    db: LocalDuckDB, schemaName: string, viewNames: string[]
): Promise<BackupViewInfo[]> {
    if (viewNames.length === 0) return [];
    const results: BackupViewInfo[] = [];

    for (const viewName of viewNames) {
        try {
            const rows = await db.query(
                `SELECT sql FROM duckdb_views() WHERE schema_name = '${schemaName}' AND view_name = '${viewName}'`
            );
            if (rows.length > 0 && typeof rows[0].sql === 'string') {
                results.push({ name: viewName, definition: rows[0].sql });
            }
        } catch (e) {
            Logger.warn(`Failed to fetch definition for local view ${schemaName}.${viewName}`, e);
        }
    }
    return results;
}

function extractViewDefinition(dialect: DbDialect, schemaName: string, viewName: string, row: QueryRow): string | undefined {
    const q = (id: string) => quoteIdentifier(dialect, id);
    if (dialect === 'mysql') {
        const createStmt = row['Create View'] || row['create view'] || (Array.isArray(row) ? row[1] : undefined);
        return typeof createStmt === 'string' ? createStmt : undefined;
    }
    if (dialect === 'duckdb') {
        const sqlVal = row.sql || row['sql'] || (Array.isArray(row) ? row[0] : undefined);
        return typeof sqlVal === 'string' ? sqlVal : undefined;
    }
    const def = row.definition || row.view_definition || row['pg_get_viewdef'] || (Array.isArray(row) ? row[0] : undefined);
    if (typeof def === 'string' && def.trim()) {
        return `CREATE OR REPLACE VIEW ${q(schemaName)}.${q(viewName)} AS\n${def.trim()}`;
    }
    return undefined;
}

async function fetchRoutineDefinitions(
    adapter: DbAdapter, profile: ConnectionProfile, secrets: ConnectionSecrets,
    dialect: DbDialect, schemaName: string, routines: RoutineModel[]
): Promise<BackupRoutineInfo[]> {
    if (routines.length === 0) return [];
    const results: BackupRoutineInfo[] = [];
    const q = (id: string) => quoteIdentifier(dialect, id);

    for (const routine of routines) {
        try {
            let sql: string;
            switch (dialect) {
                case 'mysql':
                    sql = routine.kind === 'procedure'
                        ? `SHOW CREATE PROCEDURE ${q(schemaName)}.${q(routine.name)}`
                        : `SHOW CREATE FUNCTION ${q(schemaName)}.${q(routine.name)}`;
                    break;
                case 'postgres':
                    sql = `SELECT pg_get_functiondef(p.oid) AS definition FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname = '${schemaName}' AND p.proname = '${routine.name}' LIMIT 1`;
                    break;
                default:
                    continue;
            }
            const qr = await adapter.runQuery(profile, secrets, sql, { maxRows: 1 });
            if (qr.rows.length > 0) {
                const def = extractRoutineDefinition(dialect, routine, qr.rows[0] as QueryRow);
                if (def) results.push({ name: routine.name, kind: routine.kind, definition: def });
            }
        } catch (e) {
            Logger.warn(`Failed to fetch definition for ${routine.kind} ${schemaName}.${routine.name}`, e);
        }
    }
    return results;
}

function extractRoutineDefinition(dialect: DbDialect, routine: RoutineModel, row: QueryRow): string | undefined {
    if (dialect === 'mysql') {
        const key = routine.kind === 'procedure' ? 'Create Procedure' : 'Create Function';
        const createStmt = row[key] || row[key.toLowerCase()] || (Array.isArray(row) ? row[2] : undefined);
        return typeof createStmt === 'string' ? createStmt : undefined;
    }
    const def = row.definition || row['pg_get_functiondef'] || (Array.isArray(row) ? row[0] : undefined);
    return typeof def === 'string' && def.trim() ? def.trim() : undefined;
}

// ── Backup Schema Command ──

async function backupSchemaCommand(item: BackupSchemaItem | undefined, extensionUri: vscode.Uri) {
    // Detect if this is a localDuckDBView item (no introspection/schemaModel, just schemaName)
    const isLocalDuckDBItem = !item?.introspection && !item?.schemaModel && !!item?.schemaName;

    if (isLocalDuckDBItem) {
        await backupLocalDuckDBSchema(item, extensionUri);
    } else {
        await backupRemoteSchema(item, extensionUri);
    }
}

async function backupRemoteSchema(item: BackupSchemaItem | undefined, extensionUri: vscode.Uri) {
    // 1. Extract schema info from tree item
    const schemaName = item?.schemaName || item?.schemaModel?.name;
    const tables = item?.schemaModel?.tables;
    const schemaViews: TableModel[] = item?.schemaModel?.views || [];
    const schemaProcedures: RoutineModel[] = item?.schemaModel?.procedures || [];
    const schemaFunctions: RoutineModel[] = item?.schemaModel?.functions || [];
    const connectionId = item?.connectionId || item?.introspection?.connectionId;

    if (!schemaName || !connectionId) {
        vscode.window.showErrorMessage('Please select a schema to backup.');
        return;
    }

    if (!tables || tables.length === 0) {
        vscode.window.showWarningMessage(`Schema "${schemaName}" has no tables to backup.`);
        return;
    }

    // 2. Resolve connection profile
    const { getConnection } = require('./connectionStore');
    const profile: ConnectionProfile | undefined = await getConnection(connectionId);

    if (!profile) {
        vscode.window.showErrorMessage('Connection not found.');
        return;
    }

    // 3. Check CSV export permission
    if (profile.allowCsvExport === false) {
        vscode.window.showInformationMessage('Schema backup is not available for this connection.');
        return;
    }

    // 4. Compute context for webview
    const dialect = resolveEffectiveSqlDialect(profile);
    const hasViews = schemaViews.length > 0;
    const hasRoutines = schemaProcedures.length > 0 || schemaFunctions.length > 0;

    const defaultName = `${schemaName}_backup.sql`;
    const homeDir = require('os').homedir();
    const downloadsDir = path.join(homeDir, 'Downloads');
    const defaultDir = fs.existsSync(downloadsDir) ? downloadsDir : homeDir;
    const defaultFilePath = path.join(defaultDir, defaultName);

    const panelContext: BackupSchemaContext = {
        connectionId,
        connectionName: profile.name,
        schemaName,
        dialect,
        hasViews,
        hasRoutines,
        defaultFilePath,
    };

    // 5. Open webview panel
    BackupSchemaView.render(extensionUri, panelContext, async (filePath, options) => {
        const secrets = await ensureConnectionSecrets(profile);
        if (!secrets) throw new Error('Missing credentials.');

        const adapter = getAdapter(profile.dialect);

        const backupTables: BackupTableInfo[] = tables.map((t: TableModel) => ({
            name: t.name,
            columns: (t.columns || []).map((c: ColumnModel) => ({
                name: c.name,
                type: c.type,
                nullable: c.nullable
            })),
            primaryKey: t.primaryKey,
            foreignKeys: (t.foreignKeys || []).map((fk: ForeignKeyModel) => ({
                column: fk.column,
                foreignSchema: fk.foreignSchema || schemaName,
                foreignTable: fk.foreignTable,
                foreignColumn: fk.foreignColumn
            }))
        }));

        await vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: `Backing up schema "${schemaName}"...`,
            cancellable: false
        }, async (progress) => {
            let viewDefs: BackupViewInfo[] = [];
            if (options.addCreateView && hasViews) {
                progress.report({ message: 'Fetching view definitions...' });
                viewDefs = await fetchViewDefinitions(adapter, profile!, secrets, dialect, schemaName, schemaViews);
            }

            let routineDefs: BackupRoutineInfo[] = [];
            if (options.addCreateRoutine && hasRoutines) {
                progress.report({ message: 'Fetching routine definitions...' });
                const allRoutines = [...schemaProcedures, ...schemaFunctions];
                routineDefs = await fetchRoutineDefinitions(adapter, profile!, secrets, dialect, schemaName, allRoutines);
            }

            const writeStream = fs.createWriteStream(filePath, { encoding: 'utf8' });
            const result = await generateBackupSql(
                {
                    dialect,
                    schemaName,
                    connectionName: profile!.name,
                    tables: backupTables,
                    options,
                    views: viewDefs,
                    routines: routineDefs,
                    fetchRows: async (tableName: string) => {
                        const q = (id: string) => quoteIdentifier(dialect, id);
                        const sql = `SELECT * FROM ${q(schemaName)}.${q(tableName)}`;
                        const qr = await adapter.runQuery(profile!, secrets, sql, { maxRows: Number.MAX_SAFE_INTEGER });
                        const columns = qr.columns.map((c: QueryColumn) => c.name);
                        const rows = qr.rows.map((row: unknown) => {
                            if (Array.isArray(row)) return row;
                            const r = row as Record<string, unknown>;
                            return columns.map((col: string) => r[col]);
                        });
                        return { columns, rows };
                    }
                },
                (line) => { writeStream.write(line + '\n'); },
                (tableName, tableIndex, tableCount) => {
                    progress.report({
                        message: `Table ${tableIndex + 1}/${tableCount}: ${tableName}`,
                        increment: 100 / tableCount
                    });
                }
            );

            await new Promise<void>((resolve, reject) => {
                writeStream.end(() => resolve());
                writeStream.on('error', reject);
            });

            const parts: string[] = [];
            parts.push(`${result.totalTables} tables`);
            if (result.totalRows > 0) parts.push(`${result.totalRows.toLocaleString()} rows`);
            if (result.totalViews > 0) parts.push(`${result.totalViews} views`);
            if (result.totalRoutines > 0) parts.push(`${result.totalRoutines} routines`);

            const msg = `Backup complete: ${parts.join(', ')}`;
            vscode.window.showInformationMessage(msg);
        });
    });
}

async function backupLocalDuckDBSchema(item: BackupSchemaItem | undefined, extensionUri: vscode.Uri) {
    const schemaName = item?.schemaName;
    if (!schemaName) {
        vscode.window.showErrorMessage('Please select a schema to backup.');
        return;
    }

    const db = LocalDuckDB.getInstance();
    const dialect: DbDialect = 'duckdb';

    // 1. List tables
    let tableInfos: { name: string; type: string }[];
    try {
        tableInfos = await db.listTables(schemaName);
    } catch (e: unknown) {
        const msg = e instanceof Error ? e.message : String(e);
        vscode.window.showErrorMessage(`Failed to list tables in "${schemaName}": ${msg}`);
        return;
    }

    // Separate tables and views
    const tableItems = tableInfos.filter(t => t.type === 'BASE TABLE' || t.type === 'TABLE');
    const viewItems = tableInfos.filter(t => t.type === 'VIEW');

    if (tableItems.length === 0 && viewItems.length === 0) {
        vscode.window.showWarningMessage(`Schema "${schemaName}" has no tables to backup.`);
        return;
    }

    // 2. Compute context for webview
    const hasViews = viewItems.length > 0;

    const defaultName = `${schemaName}_backup.sql`;
    const homeDir = require('os').homedir();
    const downloadsDir = path.join(homeDir, 'Downloads');
    const defaultDir = fs.existsSync(downloadsDir) ? downloadsDir : homeDir;
    const defaultFilePath = path.join(defaultDir, defaultName);

    const panelContext: BackupSchemaContext = {
        connectionId: 'local-duckdb',
        connectionName: 'DuckPiper (Local)',
        schemaName,
        dialect,
        hasViews,
        hasRoutines: false,
        isLocalDuckDB: true,
        defaultFilePath,
    };

    // 3. Open webview panel
    BackupSchemaView.render(extensionUri, panelContext, async (filePath, options) => {
        await vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: `Backing up local schema "${schemaName}"...`,
            cancellable: false
        }, async (progress) => {
            const backupTables: BackupTableInfo[] = [];
            for (const t of tableItems) {
                const columns = await db.getSchemaColumns(schemaName, t.name);
                backupTables.push({
                    name: t.name,
                    columns: columns.map(c => ({ name: c.name, type: c.type }))
                });
            }

            let viewDefs: BackupViewInfo[] = [];
            if (options.addCreateView && hasViews) {
                progress.report({ message: 'Fetching view definitions...' });
                viewDefs = await fetchLocalDuckDBViewDefinitions(db, schemaName, viewItems.map(v => v.name));
            }

            const writeStream = fs.createWriteStream(filePath, { encoding: 'utf8' });
            const result = await generateBackupSql(
                {
                    dialect,
                    schemaName,
                    connectionName: 'DuckPiper (Local)',
                    tables: backupTables,
                    options,
                    views: viewDefs,
                    fetchRows: async (tableName: string) => {
                        const q = (id: string) => quoteIdentifier(dialect, id);
                        const sql = `SELECT * FROM ${q(schemaName)}.${q(tableName)}`;
                        const rows = await db.query(sql);
                        if (rows.length === 0) return { columns: [], rows: [] };
                        const columns = Object.keys(rows[0]);
                        const arrayRows = rows.map((r: Record<string, unknown>) => columns.map(c => r[c]));
                        return { columns, rows: arrayRows };
                    }
                },
                (line) => { writeStream.write(line + '\n'); },
                (tableName, tableIndex, tableCount) => {
                    progress.report({
                        message: `Table ${tableIndex + 1}/${tableCount}: ${tableName}`,
                        increment: 100 / tableCount
                    });
                }
            );

            await new Promise<void>((resolve, reject) => {
                writeStream.end(() => resolve());
                writeStream.on('error', reject);
            });

            const parts: string[] = [];
            parts.push(`${result.totalTables} tables`);
            if (result.totalRows > 0) parts.push(`${result.totalRows.toLocaleString()} rows`);
            if (result.totalViews > 0) parts.push(`${result.totalViews} views`);

            const msg = `Backup complete: ${parts.join(', ')}`;
            vscode.window.showInformationMessage(msg);
        });
    });
}
