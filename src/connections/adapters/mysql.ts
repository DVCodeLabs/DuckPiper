
import { DbAdapter } from './adapter';
import { ConnectionProfile, ConnectionSecrets, QueryColumn, QueryResult, QueryRunOptions, SchemaIntrospection, TableModel, ColumnModel, ForeignKeyModel, IndexModel, RoutineModel } from '../../core/types';
import * as mysql from 'mysql2/promise';
import { Logger } from '../../core/logger';
import { openSshTunnel } from './sshTunnel';

/** A row returned from information_schema with rowsAsArray:true is a tuple of values. */
type InfoSchemaRow = unknown[];

interface MySQLSchemaEntry {
    name: string;
    tables: Map<string, { name: string; columns: ColumnModel[]; primaryKey: string[]; foreignKeys: ForeignKeyModel[]; indexes?: IndexModel[] }>;
    views: Map<string, { name: string; columns: ColumnModel[]; primaryKey: string[]; foreignKeys: ForeignKeyModel[]; indexes?: IndexModel[] }>;
    procedures: RoutineModel[];
    functions: RoutineModel[];
}

export class MySQLAdapter implements DbAdapter {
    readonly dialect = 'mysql';

    async testConnection(profile: ConnectionProfile, secrets: ConnectionSecrets): Promise<void> {
        const { conn, cleanup } = await this.createConnectedClient(profile, secrets);
        try {
            await conn.ping();
        } finally {
            await conn.end();
            cleanup();
        }
    }

    async runQuery(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        sql: string,
        _options: QueryRunOptions
    ): Promise<QueryResult> {
        // Force rowsAsArray to true for runQuery to match QueryResult.rows structure (array of arrays)
        const { conn, cleanup } = await this.createConnectedClient(profile, secrets, true);
        const start = Date.now();
        try {
            const [rows, fields] = await conn.execute(sql);
            const elapsedMs = Date.now() - start;

            let columns: QueryColumn[] = [];
            if (fields && Array.isArray(fields)) {
                columns = fields.map(f => ({
                    name: f.name,
                    type: String(f.type) // Just use the ID as string for now
                }));
            }

            // Convert array rows to objects with column names as keys (AG Grid expects objects)
            const rowObjects = Array.isArray(rows) && columns.length > 0
                ? (rows as unknown[][]).map(row =>
                    columns.reduce((obj, col, idx) => {
                        obj[col.name] = row[idx];
                        return obj;
                    }, {} as Record<string, unknown>)
                )
                : [];

            return {
                columns,
                rows: rowObjects,
                rowCount: Array.isArray(rows) ? rows.length : 0,
                elapsedMs
            };
        } catch (err: unknown) {
            Logger.error('[MySQL] Query Error:', err);
            throw err;
        } finally {
            await conn.end();
            cleanup();
        }
    }

    async executeNonQuery(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        sql: string
    ): Promise<{ affectedRows: number | null }> {
        const { conn, cleanup } = await this.createConnectedClient(profile, secrets, false);
        try {
            const [result] = await conn.execute(sql);
            const resultObj = result as unknown as Record<string, unknown>;
            const affectedRows = typeof resultObj?.affectedRows === 'number'
                ? Number(resultObj.affectedRows)
                : null;
            return { affectedRows };
        } finally {
            await conn.end();
            cleanup();
        }
    }

    async introspectSchema(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets
    ): Promise<SchemaIntrospection> {
        // 1. Get Schemas
        // Use rowsAsArray: true for safety against casing issues
        const { conn: connArr, cleanup: cleanupArr } = await this.createConnectedClient(profile, secrets, true);
        let schemaRows: InfoSchemaRow[];
        let columnRows: InfoSchemaRow[];
        let tableTypeRows: InfoSchemaRow[] = [];

        try {
            // Filter by the specific database configured in the profile
            const dbName = profile.database;

            if (dbName) {
                const [sRows] = await connArr.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?', [dbName]);
                schemaRows = sRows as InfoSchemaRow[];

                // 2. Get Tables & Columns
                const sql = `
                    SELECT table_schema, table_name, column_name, data_type, is_nullable, column_comment, column_key
                    FROM information_schema.columns
                    WHERE table_schema = ?
                    ORDER BY table_schema, table_name, ordinal_position
                `;
                const [cRows] = await connArr.execute(sql, [dbName]);
                columnRows = cRows as InfoSchemaRow[];

                // 2b. Get table types (BASE TABLE vs VIEW)
                const [tRows] = await connArr.execute(
                    'SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema = ?',
                    [dbName]
                );
                tableTypeRows = tRows as InfoSchemaRow[];
            } else {
                // Fallback: If no database specified, fetch all non-system schemas (or maybe we should just return empty?)
                // For now, preserving old behavior if no DB is specified, but usually DB is required.
                const [sRows] = await connArr.execute('SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (\'information_schema\', \'mysql\', \'performance_schema\', \'sys\')');
                schemaRows = sRows as InfoSchemaRow[];

                const sql = `
                    SELECT table_schema, table_name, column_name, data_type, is_nullable, column_comment, column_key
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY table_schema, table_name, ordinal_position
                `;
                const [cRows] = await connArr.execute(sql);
                columnRows = cRows as InfoSchemaRow[];

                // 2b. Get table types (BASE TABLE vs VIEW)
                const [tRows] = await connArr.execute(
                    `SELECT table_schema, table_name, table_type FROM information_schema.tables
                     WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')`
                );
                tableTypeRows = tRows as InfoSchemaRow[];
            }

        } finally {
            await connArr.end();
            cleanupArr();
        }

        // Build a map of "schema.table" -> table_type (e.g. 'BASE TABLE' or 'VIEW')
        const tableTypeMap = new Map<string, string>();
        for (const row of tableTypeRows) {
            const tSchema = String(row[0]); // table_schema
            const tName = String(row[1]);   // table_name
            const tType = String(row[2]);   // table_type
            tableTypeMap.set(`${tSchema}.${tName}`, tType);
        }

        const schemasMap = new Map<string, MySQLSchemaEntry>();

        if (Array.isArray(schemaRows)) {
            for (const row of schemaRows) {
                const name = String(row[0]); // schema_name
                schemasMap.set(name, { name, tables: new Map(), views: new Map(), procedures: [], functions: [] });
            }
        }

        if (Array.isArray(columnRows)) {
            for (const row of columnRows) {
                const sName = String(row[0]); // table_schema
                const tName = String(row[1]); // table_name
                const cName = String(row[2]); // column_name
                const dtype = String(row[3]); // data_type
                const isNull = row[4]; // is_nullable
                const comment = row[5]; // column_comment
                const colKey = row[6]; // column_key

                if (!schemasMap.has(sName)) {
                    // Start of safe-guard: if we filtered schemas but found columns for a schema not in map (shouldn't happen with correct filter), add it.
                    schemasMap.set(sName, { name: sName, tables: new Map(), views: new Map(), procedures: [], functions: [] });
                }
                const schema = schemasMap.get(sName);
                if (!schema) continue;

                const tableType = tableTypeMap.get(`${sName}.${tName}`) || 'BASE TABLE';
                const targetMap = tableType === 'VIEW' ? schema.views : schema.tables;

                if (!targetMap.has(tName)) {
                    targetMap.set(tName, { name: tName, columns: [], primaryKey: [], foreignKeys: [] });
                }
                const table = targetMap.get(tName);
                if (!table) continue;

                table.columns.push({
                    name: cName,
                    type: dtype,
                    nullable: isNull === 'YES',
                    comment: (comment as string) || undefined
                });

                if (colKey === 'PRI') {
                    table.primaryKey.push(cName);
                }
            }
        }

        // 3. Get Foreign Keys
        const { conn: connFK, cleanup: cleanupFK } = await this.createConnectedClient(profile, secrets, true);
        try {
            const dbName = profile.database;
            let fkRows: InfoSchemaRow[] = [];

            if (dbName) {
                const fkSql = `
                    SELECT
                        kcu.TABLE_SCHEMA,
                        kcu.TABLE_NAME,
                        kcu.COLUMN_NAME,
                        kcu.CONSTRAINT_NAME,
                        kcu.REFERENCED_TABLE_SCHEMA,
                        kcu.REFERENCED_TABLE_NAME,
                        kcu.REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE kcu
                    WHERE kcu.TABLE_SCHEMA = ?
                      AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
                `;
                const [rows] = await connFK.execute(fkSql, [dbName]);
                fkRows = rows as InfoSchemaRow[];
            } else {
                const fkSql = `
                    SELECT
                        kcu.TABLE_SCHEMA,
                        kcu.TABLE_NAME,
                        kcu.COLUMN_NAME,
                        kcu.CONSTRAINT_NAME,
                        kcu.REFERENCED_TABLE_SCHEMA,
                        kcu.REFERENCED_TABLE_NAME,
                        kcu.REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE kcu
                    WHERE kcu.TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                      AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
                `;
                const [rows] = await connFK.execute(fkSql);
                fkRows = rows as InfoSchemaRow[];
            }

            for (const row of fkRows) {
                const sName = String(row[0]);  // TABLE_SCHEMA
                const tName = String(row[1]);  // TABLE_NAME
                const cName = String(row[2]);  // COLUMN_NAME
                const constraintName = String(row[3]);  // CONSTRAINT_NAME
                const refSchema = String(row[4]);  // REFERENCED_TABLE_SCHEMA
                const refTable = String(row[5]);  // REFERENCED_TABLE_NAME
                const refColumn = String(row[6]);  // REFERENCED_COLUMN_NAME

                const schema = schemasMap.get(sName);
                if (!schema) continue;

                const table = schema.tables.get(tName);
                if (!table) continue;

                table.foreignKeys.push({
                    name: constraintName,
                    column: cName,
                    foreignSchema: refSchema,
                    foreignTable: refTable,
                    foreignColumn: refColumn
                });
            }
        } finally {
            await connFK.end();
            cleanupFK();
        }

        // 4. Get Indexes
        const { conn: connIdx, cleanup: cleanupIdx } = await this.createConnectedClient(profile, secrets, true);
        try {
            const dbName = profile.database;
            let idxRows: InfoSchemaRow[] = [];

            if (dbName) {
                const idxSql = `
                    SELECT
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        INDEX_NAME,
                        NON_UNIQUE,
                        COLUMN_NAME,
                        SEQ_IN_INDEX
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = ?
                      AND INDEX_NAME != 'PRIMARY'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
                `;
                const [rows] = await connIdx.execute(idxSql, [dbName]);
                idxRows = rows as InfoSchemaRow[];
            } else {
                const idxSql = `
                    SELECT
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        INDEX_NAME,
                        NON_UNIQUE,
                        COLUMN_NAME,
                        SEQ_IN_INDEX
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                      AND INDEX_NAME != 'PRIMARY'
                    ORDER BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
                `;
                const [rows] = await connIdx.execute(idxSql);
                idxRows = rows as InfoSchemaRow[];
            }

            // Group by schema/table/index_name
            const indexMap = new Map<string, { name: string; unique: boolean; columns: string[] }>();
            for (const row of idxRows) {
                const sName = String(row[0]);  // TABLE_SCHEMA
                const tName = String(row[1]);  // TABLE_NAME
                const idxName = String(row[2]); // INDEX_NAME
                const nonUnique = row[3]; // NON_UNIQUE (0 = unique, 1 = not unique)
                const colName = String(row[4]); // COLUMN_NAME

                const key = `${sName}.${tName}.${idxName}`;
                if (!indexMap.has(key)) {
                    indexMap.set(key, { name: idxName, unique: nonUnique === 0, columns: [] });
                }
                indexMap.get(key)!.columns.push(colName);
            }

            for (const [key, idx] of indexMap) {
                const parts = key.split('.');
                const sName = parts[0];
                const tName = parts[1];

                const schema = schemasMap.get(sName);
                if (!schema) continue;
                const table = schema.tables.get(tName);
                if (!table) continue;

                if (!table.indexes) table.indexes = [];
                table.indexes.push(idx);
            }
        } finally {
            await connIdx.end();
            cleanupIdx();
        }

        // 5. Get routines (procedures/functions)
        const { conn: connRoutine, cleanup: cleanupRoutine } = await this.createConnectedClient(profile, secrets, true);
        try {
            const dbName = profile.database;
            let routineRows: InfoSchemaRow[] = [];

            if (dbName) {
                const routineSql = `
                    SELECT
                        ROUTINE_SCHEMA,
                        ROUTINE_NAME,
                        ROUTINE_TYPE,
                        ROUTINE_COMMENT,
                        DTD_IDENTIFIER,
                        IS_DETERMINISTIC
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_SCHEMA = ?
                    ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
                `;
                const [rows] = await connRoutine.execute(routineSql, [dbName]);
                routineRows = rows as InfoSchemaRow[];
            } else {
                const routineSql = `
                    SELECT
                        ROUTINE_SCHEMA,
                        ROUTINE_NAME,
                        ROUTINE_TYPE,
                        ROUTINE_COMMENT,
                        DTD_IDENTIFIER,
                        IS_DETERMINISTIC
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY ROUTINE_SCHEMA, ROUTINE_NAME
                `;
                const [rows] = await connRoutine.execute(routineSql);
                routineRows = rows as InfoSchemaRow[];
            }

            for (const row of routineRows) {
                const schemaName = String(row[0]); // ROUTINE_SCHEMA
                const routineName = String(row[1]); // ROUTINE_NAME
                const routineType = String(row[2] || '').toUpperCase(); // ROUTINE_TYPE
                const routineComment = row[3] as string | undefined;
                const returnType = row[4] as string | undefined;
                const deterministicFlag = row[5];

                if (!schemasMap.has(schemaName)) {
                    schemasMap.set(schemaName, { name: schemaName, tables: new Map(), views: new Map(), procedures: [], functions: [] });
                }
                const schema = schemasMap.get(schemaName);
                if (!schema) continue;
                const routine: RoutineModel = {
                    name: routineName,
                    kind: routineType === 'PROCEDURE' ? 'procedure' : 'function',
                    comment: routineComment || undefined,
                    returnType: routineType === 'FUNCTION' ? (returnType || undefined) : undefined,
                    deterministic: deterministicFlag === 'YES' ? true : deterministicFlag === 'NO' ? false : undefined,
                    schemaQualifiedName: `${schemaName}.${routineName}`,
                    signature: `${routineName}()`
                };

                if (routine.kind === 'procedure') {
                    schema.procedures.push(routine);
                } else {
                    schema.functions.push(routine);
                }
            }
        } finally {
            await connRoutine.end();
            cleanupRoutine();
        }

        const schemas = Array.from(schemasMap.values()).map((s) => ({
            name: s.name,
            tables: Array.from(s.tables.values()) as TableModel[],
            views: Array.from(s.views.values()) as TableModel[],
            procedures: s.procedures || [],
            functions: s.functions || [],
        }));

        return {
            version: "0.2",
            generatedAt: new Date().toISOString(),
            connectionId: profile.id,
            connectionName: profile.name,
            dialect: 'mysql',
            schemas
        };
    }

    private async createConnectedClient(
        profile: ConnectionProfile,
        secrets: ConnectionSecrets,
        rowsAsArray = false
    ): Promise<{ conn: mysql.Connection; cleanup: () => void }> {
        const connOpts: mysql.ConnectionOptions = {
            host: profile.host,
            port: profile.port || 3306,
            user: profile.username,
            password: secrets.password,
            database: profile.database,
            ssl: profile.ssl ? {
                rejectUnauthorized: profile.sslMode === 'verify-full' || profile.sslMode === 'verify-ca'
            } : undefined,
            rowsAsArray
        };

        if (profile.sshEnabled) {
            const tunnel = await openSshTunnel(profile, secrets);
            const conn = await mysql.createConnection({
                ...connOpts,
                stream: tunnel.stream as unknown as NodeJS.ReadWriteStream
            });
            return { conn, cleanup: tunnel.close };
        }

        const conn = await mysql.createConnection(connOpts);
        return { conn, cleanup: () => {} };
    }
}
