import {
  ConnectionProfile,
  ConnectionSecrets,
  QueryRunOptions,
  QueryResult,
  SchemaIntrospection,
  DbDialect,
  TableModel,
} from "../../core/types";
import { DbAdapter } from "./adapter";
import { Client } from "pg";
import { convertBigIntForSerialization } from "./serializationUtils";
import { openSshTunnel } from "./sshTunnel";

export class PostgresAdapter implements DbAdapter {
  readonly dialect: DbDialect = "postgres";

  // Normalize Postgres type names to SQL standard names
  private normalizeType(pgType: string): string {
    const typeMap: Record<string, string> = {
      'character varying': 'varchar',
      'character': 'char',
      'timestamp without time zone': 'timestamp',
      'timestamp with time zone': 'timestamptz',
      'time without time zone': 'time',
      'time with time zone': 'timetz',
      'double precision': 'double',
      'real': 'float',
      'smallint': 'int2',
      'integer': 'int',
      'bigint': 'int8',
      'boolean': 'bool'
    };
    return typeMap[pgType.toLowerCase()] || pgType;
  }

  async testConnection(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
  ): Promise<void> {
    const { client, cleanup } = await this.createConnectedClient(profile, secrets);
    try {
      await client.connect();
      await client.query("SELECT 1");
    } finally {
      await client.end();
      cleanup();
    }
  }

  async runQuery(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
    sql: string,
    _options: QueryRunOptions,
  ): Promise<QueryResult> {
    const { client, cleanup } = await this.createConnectedClient(profile, secrets);
    const start = Date.now();
    try {
      await client.connect();

      // Basic implementation without fancy options for now
      const res = await client.query(sql); // pg returns rows, fields
      const elapsedMs = Date.now() - start;

      const columns = res.fields.map((f) => ({
        name: f.name,
        type: String(f.dataTypeID), // pg gives OIDs, need mapping if we want strings. "unknown" OID is fine if we just pass numbers. Or we use raw type name? pg-types might translate.
        // res.fields[i].dataTypeID is number.
      }));

      // Convert any non-JSON-serializable values (e.g., BigInt, Buffer, Date)
      const convertedRows = (res.rows || []).map(convertBigIntForSerialization);

      return {
        rows: convertedRows,
        columns,
        rowCount: res.rowCount || 0,
        elapsedMs,
      };
    } catch (err: unknown) {
      throw err;
    } finally {
      await client.end();
      cleanup();
    }
  }

  async executeNonQuery(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
    sql: string
  ): Promise<{ affectedRows: number | null }> {
    const { client, cleanup } = await this.createConnectedClient(profile, secrets);
    try {
      await client.connect();
      const res = await client.query(sql);
      return { affectedRows: typeof res.rowCount === 'number' ? res.rowCount : null };
    } finally {
      await client.end();
      cleanup();
    }
  }

  async introspectSchema(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
  ): Promise<SchemaIntrospection> {
    const { client, cleanup } = await this.createConnectedClient(profile, secrets);
    try {
      await client.connect();

      // Query information_schema
      const res = await client.query(`
             SELECT table_schema, table_name, column_name, data_type, is_nullable
             FROM information_schema.columns
             WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
             ORDER BY table_schema, table_name, ordinal_position
         `);

      // Query table types to distinguish tables from views
      const tableTypesRes = await client.query(`
             SELECT table_schema, table_name, table_type
             FROM information_schema.tables
             WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
      `);

      const tableTypeMap = new Map<string, string>();
      for (const row of tableTypesRes.rows) {
        tableTypeMap.set(`${row.table_schema}.${row.table_name}`, row.table_type);
      }

      interface PgSchemaEntry {
        name: string;
        tables: Map<string, { name: string; columns: { name: string; type: string; nullable: boolean }[]; foreignKeys: { name: string; column: string; foreignSchema: string; foreignTable: string; foreignColumn: string }[]; primaryKey?: string[]; indexes?: { name: string; columns: string[]; unique: boolean }[] }>;
        views: Map<string, { name: string; columns: { name: string; type: string; nullable: boolean }[]; foreignKeys?: { name: string; column: string; foreignSchema: string; foreignTable: string; foreignColumn: string }[] }>;
        procedures: { name: string; kind: 'procedure' | 'function'; comment?: string; returnType?: string; language?: string; schemaQualifiedName: string; signature: string }[];
        functions: { name: string; kind: 'procedure' | 'function'; comment?: string; returnType?: string; language?: string; schemaQualifiedName: string; signature: string }[];
      }
      const schemasMap = new Map<string, PgSchemaEntry>();

      for (const row of res.rows) {
        const sName = row.table_schema;
        const tName = row.table_name;

        if (!schemasMap.has(sName)) {
          schemasMap.set(sName, { name: sName, tables: new Map(), views: new Map(), procedures: [], functions: [] });
        }
        const schema = schemasMap.get(sName);
        if (!schema) continue;

        const tableType = tableTypeMap.get(`${sName}.${tName}`) || 'BASE TABLE';
        const targetMap = tableType === 'VIEW' ? schema.views : schema.tables;

        if (!targetMap.has(tName)) {
          targetMap.set(tName, { name: tName, columns: [], foreignKeys: [] });
        }
        const table = targetMap.get(tName);
        if (!table) continue;

        table.columns.push({
          name: row.column_name,
          type: this.normalizeType(row.data_type),
          nullable: row.is_nullable === "YES",
        });
      }

      // 2. Query Constraints (PKs and FKs)
      const constraintsRes = await client.query(`
             SELECT
                 tc.table_schema, 
                 tc.table_name, 
                 kcu.column_name, 
                 tc.constraint_name,
                 tc.constraint_type,
                 cc.table_schema AS foreign_schema,
                 cc.table_name AS foreign_table,
                 cc.column_name AS foreign_column
             FROM 
                 information_schema.table_constraints AS tc 
                 JOIN information_schema.key_column_usage AS kcu
                   ON tc.constraint_name = kcu.constraint_name
                   AND tc.table_schema = kcu.table_schema
                 LEFT JOIN information_schema.referential_constraints AS rc
                   ON tc.constraint_name = rc.constraint_name
                   AND tc.constraint_schema = rc.constraint_schema
                 LEFT JOIN information_schema.key_column_usage AS cc
                   ON rc.unique_constraint_name = cc.constraint_name
                   AND rc.unique_constraint_schema = cc.constraint_schema
                   AND kcu.ordinal_position = cc.ordinal_position
             WHERE 
                 tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
                 AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
      `);

      for (const row of constraintsRes.rows) {
        const sName = row.table_schema;
        const tName = row.table_name;
        const schema = schemasMap.get(sName);
        if (!schema) continue;

        const table = schema.tables.get(tName);
        if (!table) continue;

        if (row.constraint_type === 'PRIMARY KEY') {
          if (!table.primaryKey) table.primaryKey = [];
          table.primaryKey.push(row.column_name);
        } else if (row.constraint_type === 'FOREIGN KEY') {
          if (!table.foreignKeys) table.foreignKeys = [];
          table.foreignKeys.push({
            name: row.constraint_name,
            column: row.column_name,
            foreignSchema: row.foreign_schema,
            foreignTable: row.foreign_table,
            foreignColumn: row.foreign_column
          });
        }
      }

      // 3. Query Indexes
      const indexRes = await client.query(`
             SELECT
                 schemaname AS table_schema,
                 tablename AS table_name,
                 indexname AS index_name,
                 indexdef
             FROM pg_indexes
             WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
             ORDER BY schemaname, tablename, indexname
      `);

      for (const row of indexRes.rows) {
        const schema = schemasMap.get(row.table_schema);
        if (!schema) continue;
        const table = schema.tables.get(row.table_name);
        if (!table) continue;

        // Parse column names from indexdef, e.g. "CREATE INDEX idx ON tbl USING btree (col1, col2)"
        const colMatch = row.indexdef?.match(/\(([^)]+)\)/);
        if (!colMatch) continue;
        const cols = colMatch[1].split(',').map((c: string) => c.trim().replace(/\s+(ASC|DESC|NULLS\s+(FIRST|LAST))$/i, '').replace(/^"(.*)"$/, '$1'));

        if (!table.indexes) table.indexes = [];
        table.indexes.push({
          name: row.index_name,
          columns: cols,
          unique: /CREATE UNIQUE/i.test(row.indexdef),
        });
      }

      // 4. Query routines (procedures/functions)
      const routinesRes = await client.query(`
             SELECT
                 n.nspname AS schema_name,
                 p.proname AS routine_name,
                 p.prokind AS routine_kind,
                 pg_get_function_identity_arguments(p.oid) AS identity_args,
                 pg_get_function_result(p.oid) AS return_type,
                 l.lanname AS language_name,
                 obj_description(p.oid, 'pg_proc') AS routine_comment
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             LEFT JOIN pg_language l ON l.oid = p.prolang
             WHERE n.nspname NOT IN ('information_schema', 'pg_catalog')
             ORDER BY n.nspname, p.proname
      `);

      for (const row of routinesRes.rows) {
        const schemaName = row.schema_name;
        if (!schemasMap.has(schemaName)) {
          schemasMap.set(schemaName, { name: schemaName, tables: new Map(), views: new Map(), procedures: [], functions: [] });
        }
        const schema = schemasMap.get(schemaName);
        if (!schema) continue;
        const identityArgs = (row.identity_args || '').trim();
        const signature = `${row.routine_name}(${identityArgs})`;
        const routine = {
          name: row.routine_name,
          kind: (row.routine_kind === 'p' ? 'procedure' : 'function') as 'procedure' | 'function',
          comment: row.routine_comment || undefined,
          returnType: row.routine_kind === 'p' ? undefined : (row.return_type || undefined),
          language: row.language_name || undefined,
          schemaQualifiedName: `${schemaName}.${row.routine_name}`,
          signature
        };

        if (routine.kind === 'procedure') {
          schema.procedures.push(routine);
        } else {
          schema.functions.push(routine);
        }
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
        dialect: "postgres",
        schemas,
      };
    } finally {
      await client.end();
      cleanup();
    }
  }

  private async createConnectedClient(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
  ): Promise<{ client: Client; cleanup: () => void }> {
    if (profile.sshEnabled) {
      const tunnel = await openSshTunnel(profile, secrets);
      const client = new Client({
        host: profile.host,
        port: profile.port || 5432,
        database: profile.database || "postgres",
        user: profile.username,
        password: secrets.password,
        ssl: profile.ssl
          ? {
            rejectUnauthorized:
              profile.sslMode === "verify-full" ||
              profile.sslMode === "verify-ca",
          }
          : undefined,
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        stream: (() => tunnel.stream) as any,
      });
      return { client, cleanup: tunnel.close };
    }

    const client = new Client({
      host: profile.host,
      port: profile.port || 5432,
      database: profile.database || "postgres",
      user: profile.username,
      password: secrets.password,
      ssl: profile.ssl
        ? {
          rejectUnauthorized:
            profile.sslMode === "verify-full" ||
            profile.sslMode === "verify-ca",
        }
        : undefined,
    });
    return { client, cleanup: () => {} };
  }
}
