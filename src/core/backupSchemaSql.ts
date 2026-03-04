import { DbDialect } from './types';
import { quoteIdentifier, toSqlLiteral } from './sqlUtils';

export interface BackupOptions {
    /** Add DROP TABLE IF EXISTS before each CREATE TABLE. Default: false */
    addDropTable: boolean;
    /** Add CREATE TABLE statements. Default: true */
    addCreateTable: boolean;
    /** Add INSERT statements (table data). Default: true */
    addInsertData: boolean;
    /** Add CREATE VIEW statements. Default: false */
    addCreateView: boolean;
    /** Add CREATE PROCEDURE/FUNCTION statements. Default: false */
    addCreateRoutine: boolean;
    /** Wrap everything in BEGIN/COMMIT transaction. Default: false */
    wrapInTransaction: boolean;
    /** Disable FK checks during import. Default: false */
    disableForeignKeyChecks: boolean;
}

export const DEFAULT_BACKUP_OPTIONS: BackupOptions = {
    addDropTable: false,
    addCreateTable: true,
    addInsertData: true,
    addCreateView: false,
    addCreateRoutine: false,
    wrapInTransaction: false,
    disableForeignKeyChecks: false,
};

export interface BackupTableInfo {
    name: string;
    columns: Array<{ name: string; type: string; nullable?: boolean }>;
    primaryKey?: string[];
    foreignKeys?: Array<{
        column: string;
        foreignSchema: string;
        foreignTable: string;
        foreignColumn: string;
    }>;
}

export interface BackupViewInfo {
    name: string;
    /** The full CREATE VIEW statement, or just the SELECT definition body */
    definition: string;
}

export interface BackupRoutineInfo {
    name: string;
    kind: 'procedure' | 'function';
    /** The full CREATE PROCEDURE/FUNCTION statement */
    definition: string;
}

export interface BackupSchemaParams {
    dialect: DbDialect;
    schemaName: string;
    connectionName: string;
    tables: BackupTableInfo[];
    /** Callback to fetch rows for a given table. Returns column names and row arrays. */
    fetchRows: (tableName: string) => Promise<{ columns: string[]; rows: unknown[][] }>;
    /** Max rows per table. 0 = unlimited (default). */
    maxRowsPerTable?: number;
    /** Backup options controlling what to include. */
    options?: BackupOptions;
    /** View definitions to include when addCreateView is true. */
    views?: BackupViewInfo[];
    /** Routine definitions to include when addCreateRoutine is true. */
    routines?: BackupRoutineInfo[];
}

export interface BackupResult {
    totalTables: number;
    totalRows: number;
    totalViews: number;
    totalRoutines: number;
    truncatedTables: string[];
}

const INSERT_BATCH_SIZE = 100;

/** Write dialect-specific FK disable statement */
function writeFkDisable(dialect: DbDialect, writeLine: (line: string) => void): void {
    switch (dialect) {
        case 'mysql':
            writeLine('SET FOREIGN_KEY_CHECKS = 0;');
            break;
        case 'postgres':
            writeLine("SET session_replication_role = 'replica';");
            break;
        case 'duckdb':
        case 'sqlite':
            writeLine('PRAGMA foreign_keys = OFF;');
            break;
        // mssql, snowflake, oracle, databricks: no global FK disable, skip
    }
    writeLine('');
}

/** Write dialect-specific FK re-enable statement */
function writeFkEnable(dialect: DbDialect, writeLine: (line: string) => void): void {
    writeLine('');
    switch (dialect) {
        case 'mysql':
            writeLine('SET FOREIGN_KEY_CHECKS = 1;');
            break;
        case 'postgres':
            writeLine("SET session_replication_role = 'DEFAULT';");
            break;
        case 'duckdb':
        case 'sqlite':
            writeLine('PRAGMA foreign_keys = ON;');
            break;
    }
}

/**
 * Generate a full schema backup as SQL statements.
 * Calls `writeLine` for each line of output and `fetchRows` to retrieve table data.
 */
export async function generateBackupSql(
    params: BackupSchemaParams,
    writeLine: (line: string) => void,
    onProgress?: (tableName: string, tableIndex: number, tableCount: number) => void
): Promise<BackupResult> {
    const {
        dialect,
        schemaName,
        connectionName,
        tables,
        fetchRows,
        maxRowsPerTable = 0,
        options = DEFAULT_BACKUP_OPTIONS,
        views = [],
        routines = [],
    } = params;

    const q = (id: string) => quoteIdentifier(dialect, id);
    let totalRows = 0;
    const truncatedTables: string[] = [];

    // ── Header ──
    writeLine(`-- Schema Backup: ${schemaName}`);
    writeLine(`-- Connection: ${connectionName}`);
    writeLine(`-- Generated: ${new Date().toISOString()}`);
    writeLine(`-- Dialect: ${dialect}`);
    writeLine('--');
    writeLine('');

    // ── Disable FK checks ──
    if (options.disableForeignKeyChecks) {
        writeFkDisable(dialect, writeLine);
    }

    // ── Transaction start ──
    if (options.wrapInTransaction) {
        writeLine('BEGIN;');
        writeLine('');
    }

    // ── Tables ──
    for (let i = 0; i < tables.length; i++) {
        const table = tables[i];
        onProgress?.(table.name, i, tables.length);

        const tablePath = `${q(schemaName)}.${q(table.name)}`;

        // Skip table entirely if neither structure nor data requested
        if (!options.addCreateTable && !options.addInsertData) continue;

        writeLine(`-- Table: ${table.name}`);

        // DROP TABLE
        if (options.addDropTable) {
            writeLine(`DROP TABLE IF EXISTS ${tablePath};`);
        }

        // CREATE TABLE IF NOT EXISTS
        if (options.addCreateTable) {
            const colDefs = table.columns.map(col => {
                let def = `  ${q(col.name)} ${col.type}`;
                if (col.nullable === false) {
                    def += ' NOT NULL';
                }
                return def;
            });

            // Primary key constraint
            if (table.primaryKey && table.primaryKey.length > 0) {
                const pkCols = table.primaryKey.map(c => q(c)).join(', ');
                colDefs.push(`  PRIMARY KEY (${pkCols})`);
            }

            // Foreign key constraints
            if (table.foreignKeys) {
                for (const fk of table.foreignKeys) {
                    const refTable = `${q(fk.foreignSchema)}.${q(fk.foreignTable)}`;
                    colDefs.push(`  FOREIGN KEY (${q(fk.column)}) REFERENCES ${refTable} (${q(fk.foreignColumn)})`);
                }
            }

            writeLine(`CREATE TABLE IF NOT EXISTS ${tablePath} (`);
            writeLine(colDefs.join(',\n'));
            writeLine(');');
            writeLine('');
        }

        // INSERT INTO
        if (options.addInsertData) {
            let data: { columns: string[]; rows: unknown[][] };
            try {
                data = await fetchRows(table.name);
            } catch (err: unknown) {
                const msg = err instanceof Error ? err.message : String(err);
                writeLine(`-- ERROR fetching data for ${table.name}: ${msg}`);
                writeLine('');
                continue;
            }

            if (data.rows.length === 0) {
                writeLine(`-- (no data)`);
                writeLine('');
                continue;
            }

            if (maxRowsPerTable > 0 && data.rows.length >= maxRowsPerTable) {
                truncatedTables.push(table.name);
            }

            totalRows += data.rows.length;

            const colList = data.columns.map(c => q(c)).join(', ');
            const insertPrefix = `INSERT INTO ${tablePath} (${colList}) VALUES`;

            // Write in batches
            for (let batchStart = 0; batchStart < data.rows.length; batchStart += INSERT_BATCH_SIZE) {
                const batchEnd = Math.min(batchStart + INSERT_BATCH_SIZE, data.rows.length);
                const valueRows: string[] = [];

                for (let r = batchStart; r < batchEnd; r++) {
                    const row = data.rows[r];
                    const vals = row.map(v => toSqlLiteral(v, dialect)).join(', ');
                    valueRows.push(`(${vals})`);
                }

                writeLine(`${insertPrefix}`);
                writeLine(valueRows.join(',\n') + ';');
            }

            writeLine('');
        }
    }

    // ── Views ──
    let totalViews = 0;
    if (options.addCreateView && views.length > 0) {
        writeLine('-- ────────────────────────────────');
        writeLine('-- Views');
        writeLine('-- ────────────────────────────────');
        writeLine('');

        for (const view of views) {
            writeLine(`-- View: ${view.name}`);
            // The definition should already be a complete CREATE statement or just the body
            const def = view.definition.trim();
            if (def.toUpperCase().startsWith('CREATE')) {
                // Already a full statement
                writeLine(def.endsWith(';') ? def : def + ';');
            } else {
                // Just the SELECT body — wrap it
                const viewPath = `${q(schemaName)}.${q(view.name)}`;
                writeLine(`CREATE OR REPLACE VIEW ${viewPath} AS`);
                writeLine(def.endsWith(';') ? def : def + ';');
            }
            writeLine('');
            totalViews++;
        }
    }

    // ── Routines ──
    let totalRoutines = 0;
    if (options.addCreateRoutine && routines.length > 0) {
        writeLine('-- ────────────────────────────────');
        writeLine('-- Procedures & Functions');
        writeLine('-- ────────────────────────────────');
        writeLine('');

        // MySQL needs DELIMITER for routines
        if (dialect === 'mysql') {
            writeLine('DELIMITER $$');
            writeLine('');
        }

        for (const routine of routines) {
            const kindLabel = routine.kind === 'procedure' ? 'Procedure' : 'Function';
            writeLine(`-- ${kindLabel}: ${routine.name}`);
            const def = routine.definition.trim();
            if (dialect === 'mysql') {
                writeLine(def.endsWith('$$') ? def : (def.endsWith(';') ? def.slice(0, -1) + '$$' : def + '$$'));
            } else {
                writeLine(def.endsWith(';') ? def : def + ';');
            }
            writeLine('');
            totalRoutines++;
        }

        if (dialect === 'mysql') {
            writeLine('DELIMITER ;');
            writeLine('');
        }
    }

    // ── Transaction end ──
    if (options.wrapInTransaction) {
        writeLine('COMMIT;');
    }

    // ── Re-enable FK checks ──
    if (options.disableForeignKeyChecks) {
        writeFkEnable(dialect, writeLine);
    }

    return {
        totalTables: tables.length,
        totalRows,
        totalViews,
        totalRoutines,
        truncatedTables
    };
}
