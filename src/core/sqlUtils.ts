import { ConnectionProfile, DbDialect } from './types';

function normalizeDialectAlias(dialect: string): DbDialect {
    const normalized = dialect.trim().toLowerCase();
    if (normalized === 'postgresql') {
        return 'postgres';
    }
    if (normalized === 'mariadb') {
        return 'mysql';
    }
    return normalized as DbDialect;
}

/**
 * Resolve the effective SQL dialect for a connection profile.
 * If the profile has a `sqlDialect` hint, that takes precedence over adapter-level `dialect`.
 */
export function resolveEffectiveSqlDialect(profile: ConnectionProfile): DbDialect {
    const candidate = profile.sqlDialect || profile.dialect;
    return normalizeDialectAlias(String(candidate));
}

/**
 * Quote an identifier (table name, schema name, column name) for the given SQL dialect.
 * Handles escaping of the quote character within the identifier.
 */
export function quoteIdentifier(dialect: DbDialect, identifier: string): string {
    switch (dialect) {
        case 'mysql':
            // MySQL uses backticks by default
            return `\`${identifier.replace(/`/g, '``')}\``;
        case 'mssql':
            // MS SQL uses square brackets
            return `[${identifier.replace(/]/g, ']]')}]`;
        default:
            // postgres, duckdb, snowflake, oracle, databricks, sqlite use double quotes (ANSI SQL)
            return `"${identifier.replace(/"/g, '""')}"`;
    }
}

/**
 * Escape a string value for use in a SQL string literal (single-quoted).
 * Doubles single quotes and escapes backslashes.
 */
export function quoteLiteral(value: string): string {
    return `'${value.replace(/\\/g, '\\\\').replace(/'/g, "''")}'`;
}

/**
 * Strip a string down to safe identifier characters only (alphanumeric + underscore).
 */
export function sanitizeIdentifierName(name: string): string {
    return name.replace(/[^a-zA-Z0-9_]/g, '_');
}

/**
 * Convert a JavaScript value to a SQL literal string.
 */
export function toSqlLiteral(value: unknown, dialect: DbDialect): string {
    if (value === null || value === undefined) {
        return 'NULL';
    }

    if (typeof value === 'number') {
        return Number.isFinite(value) ? String(value) : 'NULL';
    }

    if (typeof value === 'bigint') {
        return value.toString();
    }

    if (typeof value === 'boolean') {
        return (dialect === 'mysql' || dialect === 'mariadb') ? (value ? '1' : '0') : (value ? 'TRUE' : 'FALSE');
    }

    if (value instanceof Date) {
        if (isNaN(value.getTime())) {
            return 'NULL';
        }
        return `'${value.toISOString().replace(/'/g, "''")}'`;
    }

    if (typeof value === 'object') {
        return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
    }

    return `'${String(value).replace(/'/g, "''")}'`;
}
