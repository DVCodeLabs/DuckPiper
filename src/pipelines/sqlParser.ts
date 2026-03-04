/**
 * Heuristic SQL parser for column-level lineage extraction (V2)
 * Supports depth-aware splitting, alias resolution, wildcard detection,
 * and Single-FROM fallback for unqualified columns.
 * Designed to be best-effort and fail-safe.
 */

export interface SelectItem {
    expression: string;
    alias: string;
    confidence: number; // 0-1
    isWildcard?: boolean; // true if this item is a * expansion
}

export interface ColumnRef {
    table?: string; // table name or alias, or fqn if resolved
    column: string;
    qualified: string; // "table.column" or "column" (if fallback, it becomes fully qualified logic downstream)
}

// Allowed schemas for lineage tracking
const ALLOWED_SCHEMAS = new Set(['imports', 'data_cache', 'bronze', 'silver', 'gold']);

export class HeuristicSqlParser {

    /**
     * Main entry point: Parse SQL to extract column lineage edges
     */
    parse(sql: string) {
        // 1. Pre-process (strip comments)
        const cleanSql = this.stripComments(sql);

        // 2. Extract Table Aliases and distinct FROM tables
        const { aliases, fromTables } = this.extractTableAliases(cleanSql);

        // 3. Extract SELECT clause
        const selectClause = this.extractSelectClause(cleanSql);
        if (!selectClause) return { aliases, fromTables, items: [] };

        // 4. Parse Select Items
        const items = this.parseSelectItems(selectClause);

        return { aliases, fromTables, items };
    }

    /**
     * Strip block and line comments while preserving strings
     */
    stripComments(sql: string): string {
        // Robust regex to handle strings vs comments
        // Group 1: 'string'
        // Group 2: /* block comment */
        // Group 3: -- line comment (to end of line, but not including newline usually, but replace with space)
        return sql.replace(/('[^']*')|(\/\*[\s\S]*?\*\/)|(--[^\r\n]*)/g, (match, string, _block, _line) => {
            if (string) return string; // Preserve string
            return ' '; // Replace comment with space
        });
    }

    /**
     * Extract table aliases and distinct source tables
     */
    extractTableAliases(sql: string): { aliases: Map<string, string>, fromTables: Set<string> } {
        const aliases = new Map<string, string>();
        const fromTables = new Set<string>();

        // Normalize whitespace for regex
        const normalized = sql.replace(/\s+/g, ' ');

        // Regex for allowed schemas only
        // Capture: schema, table, optional AS alias, optional alias (no AS)
        const regex = /(?:FROM|JOIN)\s+(imports|data_cache|bronze|silver|gold)\.([a-zA-Z0-9_]+)(?:\s+(?:AS\s+)?([a-zA-Z0-9_]+))?/gi;

        let match;
        while ((match = regex.exec(normalized)) !== null) {
            const schema = match[1];
            const table = match[2];
            const potentialAlias = match[3];
            const fqn = `${schema}.${table}`;

            fromTables.add(fqn);

            if (potentialAlias && !this.isSqlKeyword(potentialAlias)) {
                // Aliased
                if (!aliases.has(potentialAlias)) {
                    aliases.set(potentialAlias, fqn);
                }
            } else {
                // No alias provided
                // Map table name to fqn for explicit references like "customers.id"
                if (!aliases.has(table)) {
                    aliases.set(table, fqn);
                }

                // If this is the FROM clause (not JOIN), set __from__ internal alias?
                // Spec says: "If no alias provided for primary FROM table: set aliasMap['__from__']"
                // But we handle it via fromTables logic now for Method B. 
                // Still, adding __from__ doesn't hurt compatibility.
                const fullMatch = match[0];
                if (fullMatch.toUpperCase().startsWith('FROM')) {
                    if (!aliases.has('__from__')) {
                        aliases.set('__from__', fqn);
                    }
                }
            }
        }

        return { aliases, fromTables };
    }

    /**
     * Extract the outermost SELECT list
     * Strategy: Find last SELECT...FROM at depth 0
     */
    extractSelectClause(sql: string): string | null {
        // Find last "SELECT" case-insensitive
        const selectMatches = [...sql.matchAll(/SELECT/gi)];
        if (selectMatches.length === 0) return null;

        const lastSelect = selectMatches[selectMatches.length - 1];
        const startIndex = lastSelect.index! + lastSelect[0].length;

        let depth = 0;
        let fromIndex = -1;

        for (let i = startIndex; i < sql.length; i++) {
            const char = sql[i];

            if (char === "'") {
                const endQuote = sql.indexOf("'", i + 1);
                if (endQuote === -1) break;
                i = endQuote;
                continue;
            }

            if (char === '(') depth++;
            else if (char === ')') depth--;

            // Check for FROM at depth 0
            if (depth === 0) {
                if (sql.substr(i, 4).toUpperCase() === 'FROM') {
                    // Check word boundary
                    const rest = sql.substr(i);
                    if (/^FROM\b/i.test(rest)) {
                        fromIndex = i;
                        break;
                    }
                }
            }
        }

        if (fromIndex !== -1) {
            return sql.substring(startIndex, fromIndex);
        }

        return null;
    }

    /**
     * Parse select list into items
     */
    parseSelectItems(selectClause: string): SelectItem[] {
        const rawItems = this.splitByComma(selectClause);
        const items: SelectItem[] = [];

        rawItems.forEach((raw, idx) => {
            const trimmed = raw.trim();
            if (!trimmed) return;

            // Check for Wildcard
            if (trimmed === '*' || trimmed.endsWith('.*')) {
                items.push({
                    expression: trimmed,
                    alias: '*',
                    confidence: 0.4,
                    isWildcard: true
                });
                return;
            }

            // Determine Alias
            let alias = '';
            let expr = trimmed;

            // check AS alias
            const asMatch = trimmed.match(/^(.*?)\s+AS\s+([a-zA-Z0-9_]+)$/i);
            if (asMatch) {
                expr = asMatch[1].trim();
                alias = asMatch[2].replace(/['"]/g, ''); // strip quotes
            } else {
                // check trailing alias
                const implicitMatch = trimmed.match(/^(.*)\s+([a-zA-Z0-9_]+)$/);
                if (implicitMatch) {
                    const potentialAlias = implicitMatch[2];
                    const potentialExpr = implicitMatch[1].trim();
                    if (potentialExpr.length > 0 && !this.isSqlKeyword(potentialAlias) && !potentialAlias.match(/^\d+$/)) {
                        alias = potentialAlias;
                        expr = potentialExpr;
                    }
                }
            }

            // fallback: alias.col
            if (!alias) {
                const parts = expr.split('.');
                const lastPart = parts[parts.length - 1];
                if (/^[a-zA-Z0-9_]+$/.test(lastPart) && !this.isSqlKeyword(lastPart) && !expr.includes('(')) {
                    alias = lastPart;
                }
            }

            // fallback: unnamed
            if (!alias) {
                alias = `__unnamed_${idx}`;
            }

            items.push({
                expression: expr,
                alias: alias,
                confidence: this.calculateConfidence(expr)
            });
        });

        return items;
    }

    splitByComma(text: string): string[] {
        const parts: string[] = [];
        let current = '';
        let depth = 0;
        let inQuote = false;

        for (let i = 0; i < text.length; i++) {
            const char = text[i];

            if (char === "'") {
                inQuote = !inQuote;
                current += char;
                continue;
            }

            if (inQuote) {
                current += char;
                continue;
            }

            if (char === '(') depth++;
            else if (char === ')') depth--;

            if (char === ',' && depth === 0) {
                parts.push(current);
                current = '';
            } else {
                current += char;
            }
        }
        if (current.trim()) parts.push(current);
        return parts;
    }

    calculateConfidence(expr: string): number {
        if (/^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$/.test(expr)) return 0.9;
        if (/^[a-zA-Z0-9_]+$/.test(expr)) return 0.6; // Will be refined by fallback logic
        if (expr.includes('(')) return 0.8;
        return 0.5;
    }

    /**
     * Extract input column references from an expression
     * Implements Method A (Strict) and Method B (Fallback)
     */
    extractColumnRefs(expression: string, aliases: Map<string, string>, fromTables: Set<string>): ColumnRef[] {
        const refs: ColumnRef[] = [];

        // Fix: Strip string literals before tokenizing to avoid parsing content inside quotes
        const cleanExpr = expression.replace(/'[^']*'/g, ' ');
        const tokenRegex = /([a-zA-Z0-9_]+)(?:\.([a-zA-Z0-9_]+))?(?:\.([a-zA-Z0-9_]+))?/g;

        let match;
        while ((match = tokenRegex.exec(cleanExpr)) !== null) {
            const p1 = match[1];
            const p2 = match[2];
            const p3 = match[3]; // schema.table.col

            if (this.isSqlKeyword(p1) || /^\d+$/.test(p1)) continue;

            // Check for function start (e.g. trim( )
            const tokenEnd = match.index + match[0].length;
            if (expression[tokenEnd] === '(') continue;

            // Method A: Qualified Refs
            if (p3) {
                // schema.table.col check allowed
                if (ALLOWED_SCHEMAS.has(p1)) {
                    refs.push({ table: `${p1}.${p2}`, column: p3, qualified: `${p1}.${p2}.${p3}` });
                }
            } else if (p2) {
                // alias.col or table.col
                if (aliases.has(p1)) {
                    // Fix: Return the resolved table FQN, not the alias itself
                    refs.push({ table: aliases.get(p1), column: p2, qualified: `${aliases.get(p1)}.${p2}` });
                } else if (ALLOWED_SCHEMAS.has(p1)) {
                    refs.push({ table: p1, column: p2, qualified: `${p1}.${p2}` });
                } else {
                    // Unknown reference?
                }
            } else {
                // Unqualified identifier: p1 (Method B)
                // Use fallback if fromTables.size === 1
                if (fromTables.size === 1) {
                    const fallbackTable = fromTables.values().next().value; // fqn
                    refs.push({
                        table: fallbackTable,
                        column: p1,
                        qualified: p1 // Mark as unqualified/bare for tracker awareness? No, treat as legitimate.
                    });
                }
            }
        }

        // Dedupe
        const unique = new Map<string, ColumnRef>();
        refs.forEach(r => unique.set(`${r.table}.${r.column}`, r));
        return Array.from(unique.values());
    }

    private isSqlKeyword(word: string): boolean {
        const keywords = new Set([
            'SELECT', 'FROM', 'WHERE', 'JOIN', 'ON', 'AND', 'OR', 'AS', 'CASE', 'WHEN',
            'THEN', 'ELSE', 'END', 'GROUP', 'BY', 'ORDER', 'HAVING', 'LIMIT', 'OFFSET',
            'INNER', 'LEFT', 'RIGHT', 'OUTER', 'CROSS', 'UNION', 'ALL', 'DISTINCT',
            'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CAST', 'TRUE', 'FALSE', 'NULL', 'IS', 'NOT',
            'IN', 'BETWEEN', 'LIKE', 'ILIKE', 'SIMILAR', 'TO',
            'TRIM', 'UPPER', 'LOWER', 'SUBSTRING', 'CONCAT', 'COALESCE', 'NULLIF', 'INITCAP', 'REGEXP_REPLACE'
        ]);
        return keywords.has(word.toUpperCase());
    }
}
