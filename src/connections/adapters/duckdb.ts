import { DbAdapter } from './adapter';
import { ConnectionProfile, ConnectionSecrets, QueryRunOptions, QueryResult, SchemaIntrospection, TableModel } from '../../core/types';
import * as duckdb from 'duckdb'; // standard duckdb bindings
import * as fs from 'fs';
import * as path from 'path';
import { convertBigIntForSerialization } from './serializationUtils';
import { quoteIdentifier, quoteLiteral } from '../../core/sqlUtils';
import { Logger } from '../../core/logger';

interface DuckDBTestProfile extends ConnectionProfile {
    _runqlAllowCreateOnTest?: boolean;
}

/** Intermediate shape used during introspection to accumulate schema data */
interface DuckDBSchemaEntry {
    name: string;
    tables: Map<string, { name: string; columns: { name: string; type: string; nullable: boolean }[]; foreignKeys: unknown[]; indexes?: { name: string; columns: string[]; unique: boolean }[] }>;
    views: Map<string, { name: string; columns: { name: string; type: string; nullable: boolean }[]; foreignKeys: unknown[]; indexes?: { name: string; columns: string[]; unique: boolean }[] }>;
    procedures: unknown[];
    functions: unknown[];
    catalog: string;
    originalSchema: string;
}

/** Row shape returned by duckdb_indexes() */
interface IndexRow {
    schema_name: string;
    table_name: string;
    index_name: string;
    is_unique: boolean;
    sql?: string;
}

export class DuckDBAdapter implements DbAdapter {
    readonly dialect = 'duckdb';

    async testConnection(profile: ConnectionProfile, _secrets: ConnectionSecrets): Promise<void> {
        const candidate = profile as DuckDBTestProfile;
        const dbPath = profile.filePath || ':memory:';
        const allowCreateOnTest = candidate._runqlAllowCreateOnTest === true;

        // Test should be non-destructive for local files unless save explicitly allows create.
        if (!allowCreateOnTest && this.isLocalFilePath(dbPath) && !fs.existsSync(dbPath)) {
            throw new Error(`DuckDB file not found: ${dbPath}. Click Save Connection to create it.`);
        }

        const db = new duckdb.Database(dbPath);
        try {
            await new Promise<void>((resolve, reject) => {
                db.all('SELECT 1', (err: Error | null) => {
                    if (err) reject(err);
                    else resolve();
                });
            });
        } finally {
            await this.closeDb(db);
        }
    }

    async runQuery(
        profile: ConnectionProfile,
        _secrets: ConnectionSecrets,
        sql: string,
        _options: QueryRunOptions
    ): Promise<QueryResult> {
        const db = await this.openDb(profile);
        const start = Date.now();

        return new Promise((resolve, reject) => {
            // Note: Row limit wrapping is now handled centrally by extension.ts

            db.all(sql, (err: Error | null, rows: Record<string, unknown>[]) => {
                const elapsedMs = Date.now() - start;
                Logger.info(`[DuckDB] Query completed in ${elapsedMs}ms. Rows returned: ${rows ? rows.length : 0}`);

                if (err) {
                    Logger.error(`[DuckDB] Query error:`, err);
                    reject(err);
                    return;
                }

                // Convert BigInt values to Numbers/strings (DuckDB returns BigInt for integer columns)
                // This prevents JSON serialization errors when sending to webview
                const convertedRows = rows ? rows.map(r => convertBigIntForSerialization(r) as Record<string, unknown>) : [];


                // Extract columns from first row if exists
                // Infer type from actual JavaScript values
                let columns: { name: string; type: string }[] = [];
                if (convertedRows && convertedRows.length > 0) {
                    columns = Object.keys(convertedRows[0]).map(k => {
                        const value = convertedRows[0][k];
                        let inferredType = 'unknown';
                        if (value === null || value === undefined) {
                            // Check next non-null row
                            for (let i = 1; i < convertedRows.length; i++) {
                                const v = convertedRows[i][k];
                                if (v !== null && v !== undefined) {
                                    inferredType = this.inferJsType(v);
                                    break;
                                }
                            }
                        } else {
                            inferredType = this.inferJsType(value);
                        }
                        return { name: k, type: inferredType };
                    });
                }

                resolve({
                    rows: convertedRows,
                    columns,
                    rowCount: convertedRows ? convertedRows.length : 0,
                    elapsedMs
                });
            });
        });
    }

    async executeNonQuery(
        profile: ConnectionProfile,
        _secrets: ConnectionSecrets,
        sql: string
    ): Promise<{ affectedRows: number | null }> {
        const db = await this.openDb(profile);
        return new Promise((resolve, reject) => {
            db.run(sql, function (this: { changes?: number }, err: Error | null) {
                if (err) {
                    reject(err);
                    return;
                }
                const changes = typeof this.changes === 'number'
                    ? Number(this.changes)
                    : null;
                resolve({ affectedRows: changes });
            });
        });
    }

    private inferJsType(value: unknown): string {
        if (typeof value === 'boolean') return 'boolean';
        if (typeof value === 'number') {
            return Number.isInteger(value) ? 'integer' : 'decimal';
        }
        if (typeof value === 'bigint') return 'bigint';
        if (typeof value === 'string') return 'varchar';
        if (value instanceof Date) return 'timestamp';
        if (Array.isArray(value)) return 'array';
        if (typeof value === 'object') return 'json';
        return 'unknown';
    }

    async introspectSchema(
        profile: ConnectionProfile,
        _secrets: ConnectionSecrets
    ): Promise<SchemaIntrospection> {
        const db = await this.openDb(profile);

        return new Promise((resolve, reject) => {
            // 1. Get ALL schemas
            // We need catalog_name to support attached databases.
            db.all('SELECT catalog_name, schema_name FROM information_schema.schemata ORDER BY catalog_name, schema_name', (err: Error | null, schemaRows: Record<string, unknown>[]) => {
                if (err) { reject(err); return; }

                const schemasMap = new Map<string, DuckDBSchemaEntry>();

                // Get primary catalog name to keep standard naming for main DB
                let primaryCatalog = 'memory';
                if (profile.filePath && profile.filePath !== ':memory:') {
                    primaryCatalog = path.basename(profile.filePath, path.extname(profile.filePath));
                }

                const getSchemaKey = (catalog: string, schema: string) => `${catalog}.${schema}`;

                const getDisplayName = (catalog: string, schema: string) => {
                    if (catalog === primaryCatalog || catalog === 'memory') {
                        return schema;
                    }
                    if (schema === 'main') {
                        return catalog;
                    }
                    return `${catalog}.${schema}`;
                };

                // Pre-fill schemas
                if (schemaRows) {
                    for (const s of schemaRows) {
                        const key = getSchemaKey(String(s.catalog_name), String(s.schema_name));
                        const displayName = getDisplayName(String(s.catalog_name), String(s.schema_name));
                        schemasMap.set(key, {
                            name: displayName,
                            tables: new Map(),
                            views: new Map(),
                            procedures: [],
                            functions: [],
                            catalog: String(s.catalog_name),
                            originalSchema: String(s.schema_name)
                        });
                    }
                }

                // 2. Get table types (BASE TABLE vs VIEW)
                const tableTypeSql = `
                    SELECT table_catalog, table_schema, table_name, table_type
                    FROM information_schema.tables
                    ORDER BY table_catalog, table_schema, table_name
                `;

                db.all(tableTypeSql, (errTT: Error | null, tableTypeRows: Record<string, unknown>[]) => {
                    if (errTT) { reject(errTT); return; }

                    const tableTypeMap = new Map<string, string>();
                    if (tableTypeRows) {
                        for (const tt of tableTypeRows) {
                            const ttKey = `${tt.table_catalog}.${tt.table_schema}.${tt.table_name}`;
                            tableTypeMap.set(ttKey, String(tt.table_type));
                        }
                    }

                // 3. Get Tables & Columns
                // Include table_catalog
                const sql = `
                    SELECT table_catalog, table_schema, table_name, column_name, data_type, is_nullable
                    FROM information_schema.columns
                    ORDER BY table_catalog, table_schema, table_name, ordinal_position
                `;

                db.all(sql, (err2: Error | null, rows: Record<string, unknown>[]) => {
                    if (err2) { reject(err2); return; }

                    for (const row of rows) {
                        const cName = String(row.table_catalog);
                        const sName = String(row.table_schema);
                        const tName = String(row.table_name);

                        const key = getSchemaKey(cName, sName);

                        // Just in case a schema exists in columns but not schemata
                        if (!schemasMap.has(key)) {
                            const displayName = getDisplayName(cName, sName);
                            schemasMap.set(key, {
                                name: displayName,
                                tables: new Map(),
                                views: new Map(),
                                procedures: [],
                                functions: [],
                                catalog: String(cName),
                                originalSchema: String(sName)
                            });
                        }
                        const schema = schemasMap.get(key);
                        if (!schema) continue;

                        const typeKey = `${cName}.${sName}.${tName}`;
                        const tableType = tableTypeMap.get(typeKey) || 'BASE TABLE';
                        const targetMap = tableType === 'VIEW' ? schema.views : schema.tables;

                        if (!targetMap.has(tName)) {
                            targetMap.set(tName, { name: tName, columns: [], foreignKeys: [] });
                        }
                        const table = targetMap.get(tName);
                        if (!table) continue;

                        table.columns.push({
                            name: String(row.column_name),
                            type: String(row.data_type),
                            nullable: row.is_nullable === 'YES'
                        });
                    }

                    // 3. Get Indexes (DuckDB exposes indexes via duckdb_indexes())
                    const buildResult = () => {
                        const schemas = Array.from(schemasMap.values())
                            .filter((s) => {
                                // Global filters
                                if (s.originalSchema === 'information_schema' || s.originalSchema === 'pg_catalog') return false;

                                // Filter out system/temp catalogs if desired
                                if (s.catalog === 'data_cache') return false;

                                // Check setting for dp_app
                                const vscodeApi = require('vscode');
                                const showInternal = vscodeApi.workspace.getConfiguration('dp').get('ui.showSystemSchemas', false);
                                if (s.originalSchema === 'dp_app' && !showInternal) return false;

                                // Default Connection (Duck_Piper) strict filters
                                if (profile.id === 'duck-piper-local-data-work') {
                                    if ((s.catalog === primaryCatalog || s.catalog === 'memory') &&
                                        (s.originalSchema === 'main')) {
                                        return false;
                                    }
                                }
                                return true;
                            })
                            .map((s) => ({
                                name: s.name,
                                tables: Array.from(s.tables.values()) as TableModel[],
                                views: Array.from(s.views.values()) as TableModel[],
                                procedures: (s.procedures || []) as SchemaIntrospection['schemas'][number]['procedures'],
                                functions: (s.functions || []) as SchemaIntrospection['schemas'][number]['functions'],
                            }));

                        // For duck-piper-local-data-work, ensure transformation schemas are always present
                        if (profile.id === 'duck-piper-local-data-work') {
                            const requiredSchemas = ['imports', 'data_cache', 'bronze', 'silver', 'gold'];
                            const existingNames = new Set(schemas.map((s) => s.name));
                            for (const name of requiredSchemas) {
                                if (!existingNames.has(name)) {
                                    schemas.push({ name, tables: [], views: [], procedures: [], functions: [] });
                                }
                            }
                            // Sort to maintain consistent order
                            schemas.sort((a, b) => {
                                const order = ['imports', 'data_cache', 'bronze', 'silver', 'gold'];
                                const ia = order.indexOf(a.name);
                                const ib = order.indexOf(b.name);
                                if (ia !== -1 && ib !== -1) return ia - ib;
                                if (ia !== -1) return -1;
                                if (ib !== -1) return 1;
                                return a.name.localeCompare(b.name);
                            });
                        }

                        resolve({
                            version: "0.2",
                            generatedAt: new Date().toISOString(),
                            connectionId: profile.id,
                            connectionName: profile.name,
                            dialect: 'duckdb',
                            schemas
                        });
                    };

                    const idxSql = `
                        SELECT schema_name, table_name, index_name, is_unique, sql
                        FROM duckdb_indexes()
                        ORDER BY schema_name, table_name, index_name
                    `;
                    db.all(idxSql, (err3: Error | null, rawIdxRows: Record<string, unknown>[]) => {
                        const idxRows = rawIdxRows as unknown as IndexRow[];
                        if (!err3 && idxRows) {
                            for (const row of idxRows) {
                                const sName = row.schema_name;
                                const tName = row.table_name;

                                // Find the schema entry (check both keyed variants)
                                let schema: DuckDBSchemaEntry | undefined;
                                for (const [, s] of schemasMap) {
                                    if (s.originalSchema === sName || s.name === sName) {
                                        schema = s;
                                        break;
                                    }
                                }
                                if (!schema) continue;
                                const table = schema.tables.get(tName);
                                if (!table) continue;

                                // Parse columns from CREATE INDEX SQL
                                const colMatch = row.sql?.match(/\(([^)]+)\)/);
                                if (!colMatch) continue;
                                const cols = colMatch[1].split(',').map((c: string) => c.trim().replace(/^"(.*)"$/, '$1'));

                                if (!table.indexes) table.indexes = [];
                                table.indexes.push({
                                    name: row.index_name,
                                    columns: cols,
                                    unique: row.is_unique ?? false,
                                });
                            }
                        }
                        // Not fatal if indexes fail — build result either way
                        buildResult();
                    });
                });
                }); // end tableTypeSql callback
            });
        });
    }

    private static dbCache = new Map<string, duckdb.Database>();

    private async openDb(profile: ConnectionProfile): Promise<duckdb.Database> {
        const cacheKey = profile.id?.trim();

        // Reuse existing connection if available
        if (cacheKey && DuckDBAdapter.dbCache.has(cacheKey)) {

            return DuckDBAdapter.dbCache.get(cacheKey)!;
        }

        // If filePath is set, use it. Else :memory:
        const dbPath = profile.filePath || ':memory:';



        // Create new connection
        const db = new duckdb.Database(dbPath);

        // Only cache persisted connections with stable IDs.
        if (cacheKey) {
            DuckDBAdapter.dbCache.set(cacheKey, db);
        }

        return db;
    }

    private isLocalFilePath(dbPath: string): boolean {
        return dbPath !== ':memory:' && !dbPath.startsWith('md:');
    }

    private async closeDb(db: duckdb.Database): Promise<void> {
        await new Promise<void>((resolve) => {
            try {
                db.close((err) => {
                    if (err) {
                        Logger.warn('[DuckDB] Error closing test connection:', err);
                    }
                    resolve();
                });
            } catch (e) {
                Logger.warn('[DuckDB] close() threw while closing test connection:', e);
                resolve();
            }
        });
    }

    public static closeConnection(profileId: string) {
        const db = DuckDBAdapter.dbCache.get(profileId);
        if (db) {
            DuckDBAdapter.dbCache.delete(profileId);
            try {
                db.close((err) => {
                    if (err) { Logger.warn(`[DuckDB] Error closing connection ${profileId}:`, err); }
                });
            } catch (_e) {
                Logger.warn(`[DuckDB] close() not available for ${profileId}, removed from cache`);
            }
        }
    }

    public static closeAllConnections() {
        for (const [id] of DuckDBAdapter.dbCache) {
            DuckDBAdapter.closeConnection(id);
        }
    }

    async exportTable(
        profile: ConnectionProfile,
        _secrets: ConnectionSecrets,
        schema: string,
        table: string,
        format: 'csv' | 'json',
        outputUri: import('vscode').Uri
    ): Promise<void> {
        // Optimized DuckDB Export using COPY
        const db = await this.openDb(profile);

        let copySql = '';
        const q = (id: string) => quoteIdentifier('duckdb', id);
        const fullTableName = `${q(schema)}.${q(table)}`;

        if (format === 'csv') {
            copySql = `COPY (SELECT * FROM ${fullTableName}) TO ${quoteLiteral(outputUri.fsPath)} (HEADER, DELIMITER ',')`;
        } else {
            // Fallback or JSON support
            throw new Error("JSON export not optimized for DuckDB yet");
        }

        return new Promise((resolve, reject) => {
            db.all(copySql, (err: Error | null) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }
}
