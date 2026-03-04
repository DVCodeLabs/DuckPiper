/**
 * SQL Limit Helper
 * 
 * Applies a hard row cap to result-returning SQL statements.
 */

export interface LimitResult {
    sql: string;
    clamped: boolean;
    effectiveLimit: number;
}

/**
 * Detect if SQL is a result-returning statement that should have LIMIT applied.
 */
export function isResultReturningStatement(sql: string): boolean {
    const upper = sql.trim().toUpperCase();
    // Match SELECT, WITH (CTEs), VALUES, TABLE, SHOW, DESCRIBE, EXPLAIN
    return /^(SELECT|WITH|VALUES|TABLE|SHOW|DESCRIBE|EXPLAIN)\b/.test(upper);
}

/**
 * Parse existing LIMIT value from SQL if present.
 * Returns undefined if no LIMIT clause found.
 */
function parseExistingLimit(sql: string): number | undefined {
    // Match LIMIT <number> at end of statement (with optional semicolon/whitespace)
    // Also handles LIMIT with OFFSET: LIMIT 10 OFFSET 5
    const match = sql.match(/LIMIT\s+(\d+)(?:\s+OFFSET\s+\d+)?\s*;?\s*$/i);
    return match ? parseInt(match[1], 10) : undefined;
}

/**
 * Apply row limit to SQL.
 * 
 * @param sql - The SQL statement to potentially wrap
 * @param maxRows - The hard cap (0 = no limit)
 * @returns Object with wrapped SQL, whether user's limit was clamped, and effective limit
 */
export function applyRowLimit(sql: string, maxRows: number): LimitResult {
    // No limit if maxRows is 0
    if (maxRows === 0) {
        return { sql, clamped: false, effectiveLimit: 0 };
    }

    // Don't wrap non-result statements (CREATE, ALTER, INSERT, etc.)
    if (!isResultReturningStatement(sql)) {
        return { sql, clamped: false, effectiveLimit: 0 };
    }

    const existingLimit = parseExistingLimit(sql);
    let effectiveLimit: number;
    let clamped = false;

    if (existingLimit !== undefined) {
        // User specified a LIMIT - use the smaller of the two
        if (existingLimit > maxRows) {
            effectiveLimit = maxRows;
            clamped = true;
        } else {
            effectiveLimit = existingLimit;
        }
    } else {
        // No existing LIMIT - apply maxRows
        effectiveLimit = maxRows;
    }

    // Wrap the query with an outer LIMIT
    // Strip trailing semicolon and any trailing comments (-- or /* style)
    const cleanSql = sql.trim()
        .replace(/;\s*(--[^\n]*)?$/, '')  // Remove trailing ; and optional -- comment
        .replace(/;\s*(\/\*[\s\S]*?\*\/\s*)?$/, '')  // Remove trailing ; and optional /* */ comment
        .replace(/--[^\n]*$/, '')  // Remove trailing -- comment (no semicolon)
        .trim();
    const wrappedSql = `SELECT * FROM (${cleanSql}) AS dp_limit_sub LIMIT ${effectiveLimit}`;

    return { sql: wrappedSql, clamped, effectiveLimit };
}
