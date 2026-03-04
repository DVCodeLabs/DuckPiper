import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import * as duckdb from 'duckdb';
import { quoteIdentifier, quoteLiteral } from '../core/sqlUtils';
import { Logger } from '../core/logger';

export class LocalDuckDB {
    private static instance: LocalDuckDB;
    private dbPath: string = "";
    private _db: duckdb.Database | null = null;
    private _initialized = false;

    private constructor() { }

    static getInstance(): LocalDuckDB {
        if (!LocalDuckDB.instance) {
            LocalDuckDB.instance = new LocalDuckDB();
        }
        return LocalDuckDB.instance;
    }

    private getConfigurationPath(): string {
        const config = vscode.workspace.getConfiguration('dp');
        const relPath = config.get<string>('duckdb.path', 'DP/system/piper.duckdb');

        if (vscode.workspace.workspaceFolders && vscode.workspace.workspaceFolders.length > 0) {
            return path.join(vscode.workspace.workspaceFolders[0].uri.fsPath, relPath);
        }
        return relPath; // Fallback (might fail if no workspace)
    }

    public getDbPath(): string {
        if (!this.dbPath) {
            this.dbPath = this.getConfigurationPath();
        }
        return this.dbPath;
    }

    public getCatalogName(): string {
        const dbPath = this.getDbPath();
        if (!dbPath || dbPath === ':memory:') {
            return 'memory';
        }
        return path.basename(dbPath, path.extname(dbPath));
    }

    private getDb(): duckdb.Database {
        if (!this._db) {
            this._db = new duckdb.Database(this.getDbPath());
        }
        return this._db;
    }

    public async initialize(): Promise<void> {
        if (this._initialized) return;

        const dbPath = this.getDbPath();
        const dir = path.dirname(dbPath);

        // Ensure directory exists
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        // Initialize DB and Schemas
        const db = this.getDb();

        return new Promise<void>((resolve, reject) => {
            db.serialize(() => {
                // Create Schemas
                const schemas = [
                    'imports',
                    'data_cache',
                    'bronze',
                    'silver',
                    'gold',
                    'dp_app'
                ];
                const statements = schemas.map(s => `CREATE SCHEMA IF NOT EXISTS ${s};`);

                // Optional metadata table
                statements.push(`CREATE TABLE IF NOT EXISTS dp_app.__dp_runs (
                    run_id UUID PRIMARY KEY,
                    notebook_path VARCHAR,
                    cell_id VARCHAR,
                    layer VARCHAR,
                    output_name VARCHAR,
                    started_at TIMESTAMP,
                    user_scan_bytes BIGINT
                );`);

                const runNext = (index: number) => {
                    if (index >= statements.length) {
                        this._initialized = true;
                        resolve();
                        return;
                    }
                    db.run(statements[index], (err: Error | null) => {
                        if (err) {
                            Logger.error(`[LocalDuckDB] Failed to run init SQL: ${statements[index]}`, err);
                            reject(err);
                        } else {
                            runNext(index + 1);
                        }
                    });
                };

                runNext(0);
            });
        });
    }

    public async listTables(schema: string): Promise<{ name: string, type: string }[]> {
        const db = this.getDb();

        return new Promise((resolve, reject) => {
            const catalog = this.getCatalogName();
            const sql = `SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = ${quoteLiteral(catalog)} AND table_schema = ${quoteLiteral(schema)} ORDER BY table_name`;
            db.all(sql, (err: Error | null, rows: Record<string, unknown>[]) => {
                if (err) reject(err);
                else resolve(rows.map(r => ({ name: String(r.table_name), type: String(r.table_type) })));
            });
        });
    }

    public async getSchemaColumns(schema: string, table: string): Promise<{ name: string, type: string }[]> {
        const db = this.getDb();

        return new Promise((resolve) => {
            // Strategy 1: DESCRIBE SELECT * (Authoritative for views & tables)
            const q = (id: string) => quoteIdentifier('duckdb', id);
            const describeSql = `DESCRIBE SELECT * FROM ${q(schema)}.${q(table)}`;

            db.all(describeSql, (err: Error | null, rows: Record<string, unknown>[]) => {
                if (!err && rows && rows.length > 0) {
                    // Success path
                    const columns = rows
                        .filter(r => r.column_name && String(r.column_name).trim() !== '')
                        .map(r => ({
                            name: String(r.column_name),
                            type: String(r.column_type)
                        }));
                    resolve(columns);
                    return;
                }

                // Strategy 2: Fallback to PRAGMA table_info (if DESCRIBE fails)
                Logger.warn(`[LocalDuckDB] DESCRIBE failed for ${schema}.${table}, falling back to PRAGMA.`, err);
                const pragmaSql = `PRAGMA table_info(${quoteLiteral(schema + '.' + table)})`;

                db.all(pragmaSql, (err2: Error | null, rows2: Record<string, unknown>[]) => {
                    const columns = err2 ? [] : rows2.map(r => ({
                        name: String(r.name),
                        type: String(r.type)
                    }));
                    if (err2) {
                        Logger.error(`[LocalDuckDB] Introspection failed for ${schema}.${table}`, err2);
                    }
                    resolve(columns);
                });
            });
        });
    }

    // Alias for getSchemaColumns - used by localDuckDBView
    public async listColumns(schema: string, table: string): Promise<{ name: string, type: string }[]> {
        return this.getSchemaColumns(schema, table);
    }

    public async runUpdate(sql: string): Promise<void> {
        const db = this.getDb();
        return new Promise((resolve, reject) => {
            db.exec(sql, (err: Error | null) => {
                if (err) {
                    reject(err);
                } else {
                    // Force WAL checkpoint to ensure changes are visible to other connections
                    db.exec('CHECKPOINT;', (checkpointErr: Error | null) => {
                        if (checkpointErr) Logger.warn("[LocalDuckDB] Checkpoint warning:", checkpointErr);
                        resolve();
                    });
                }
            });
        });
    }

    public async checkTableExists(schema: string, table: string): Promise<boolean> {
        const db = this.getDb();
        const catalog = this.getCatalogName();
        return new Promise((resolve) => {
            db.all(`SELECT 1 FROM information_schema.tables WHERE table_catalog = ${quoteLiteral(catalog)} AND table_schema = ${quoteLiteral(schema)} AND table_name = ${quoteLiteral(table)}`, (err: Error | null, rows: Record<string, unknown>[]) => {
                resolve(!err && rows && rows.length > 0);
            });
        });
    }

    public getConnectionString(): string {
        return this.getDbPath();
    }

    public async createIndex(schema: string, table: string, column: string): Promise<void> {
        const indexName = `idx_${table}_${column}`;
        const sql = `CREATE INDEX IF NOT EXISTS "${indexName}" ON "${schema}"."${table}" ("${column}");`;
        await this.runUpdate(sql);
    }

    public async dropIndex(schema: string, table: string, column: string): Promise<void> {
        const indexName = `idx_${table}_${column}`;
        const sql = `DROP INDEX IF EXISTS "${schema}"."${indexName}";`;
        await this.runUpdate(sql);
    }

    public async listIndexes(schema: string, table: string): Promise<{ name: string; columns: string[] }[]> {
        const db = this.getDb();
        return new Promise((resolve) => {
            // DuckDB stores index info in duckdb_indexes()
            const sql = `SELECT index_name, sql FROM duckdb_indexes() WHERE schema_name = ${quoteLiteral(schema)} AND table_name = ${quoteLiteral(table)}`;
            db.all(sql, (err: Error | null, rows: Record<string, unknown>[]) => {
                if (err) {
                    resolve([]); // Return empty on error
                    return;
                }
                // Parse column names from index SQL (simplified)
                const indexes = (rows || []).map(r => ({
                    name: String(r.index_name),
                    columns: [] as string[] // Would need SQL parsing to extract columns
                }));
                resolve(indexes);
            });
        });
    }

    public async query(sql: string): Promise<Record<string, unknown>[]> {
        const db = this.getDb();
        return new Promise((resolve, reject) => {
            db.all(sql, (err: Error | null, rows: Record<string, unknown>[]) => {
                if (err) reject(err);
                else resolve(rows);
            });
        });
    }

    public async exec(sql: string): Promise<void> {
        const db = this.getDb();
        return new Promise((resolve, reject) => {
            db.run(sql, (err: Error | null) => {
                if (err) reject(err);
                else resolve();
            });
        });
    }

    public close(): Promise<void> {
        return new Promise((resolve) => {
            if (this._db) {
                this._db.close((err) => {
                    if (err) Logger.warn('[LocalDuckDB] Close error:', err);
                    this._db = null;
                    this._initialized = false;
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }
}
