/**
 * SQL Statement Splitter
 *
 * Splits a SQL script into individual statements using a stateful character
 * scanner.  Only splits on `;` when in "normal" mode — semicolons inside
 * strings, identifiers, comments, and dollar-quoted bodies are preserved.
 */

export interface SplitStatement {
    sql: string;           // trimmed statement text
    startOffset: number;   // char offset in original text (before trim)
    endOffset: number;     // char offset in original text (exclusive)
}

const enum Mode {
    Normal,
    SingleQuote,
    DoubleQuote,
    Backtick,
    Bracket,
    LineComment,
    BlockComment,
    DollarQuote,
}

/**
 * Split a SQL script into individual statements.
 *
 * Rules (v1):
 *  - Splits on `;` only in Normal mode.
 *  - Respects single-quoted strings (with `''` escape).
 *  - Respects double-quoted identifiers.
 *  - Respects backtick-quoted identifiers.
 *  - Respects bracket identifiers (`[name]`).
 *  - Respects line comments (`-- …`).
 *  - Respects block comments (`/* … * /`).
 *  - Respects dollar-quoted bodies (`$$…$$`, `$tag$…$tag$`).
 *  - Discards empty statements after trim.
 */
export function splitStatements(sql: string): SplitStatement[] {
    const results: SplitStatement[] = [];
    const len = sql.length;

    let mode: Mode = Mode.Normal;
    let dollarTag = '';          // current dollar-quote tag (e.g. '' for $$, 'fn' for $fn$)
    let stmtStart = 0;          // start offset of current statement

    let i = 0;
    while (i < len) {
        const ch = sql[i];

        switch (mode) {
            // ── Normal mode ──────────────────────────────────────────
            case Mode.Normal: {
                if (ch === ';') {
                    pushStatement(results, sql, stmtStart, i);
                    stmtStart = i + 1;
                    i++;
                } else if (ch === "'") {
                    mode = Mode.SingleQuote;
                    i++;
                } else if (ch === '"') {
                    mode = Mode.DoubleQuote;
                    i++;
                } else if (ch === '`') {
                    mode = Mode.Backtick;
                    i++;
                } else if (ch === '[') {
                    mode = Mode.Bracket;
                    i++;
                } else if (ch === '-' && i + 1 < len && sql[i + 1] === '-') {
                    mode = Mode.LineComment;
                    i += 2;
                } else if (ch === '#') {
                    mode = Mode.LineComment;
                    i++;
                } else if (ch === '/' && i + 1 < len && sql[i + 1] === '*') {
                    mode = Mode.BlockComment;
                    i += 2;
                } else if (ch === '$') {
                    const tag = tryParseDollarTag(sql, i);
                    if (tag !== null) {
                        dollarTag = tag.tag;
                        mode = Mode.DollarQuote;
                        i = tag.endIndex;  // skip past opening $tag$
                    } else {
                        i++;
                    }
                } else {
                    i++;
                }
                break;
            }

            // ── String / identifier modes ────────────────────────────
            case Mode.SingleQuote: {
                if (ch === "'" && i + 1 < len && sql[i + 1] === "'") {
                    i += 2;  // escaped quote
                } else if (ch === "'") {
                    mode = Mode.Normal;
                    i++;
                } else {
                    i++;
                }
                break;
            }

            case Mode.DoubleQuote: {
                if (ch === '"' && i + 1 < len && sql[i + 1] === '"') {
                    i += 2;  // escaped quote
                } else if (ch === '"') {
                    mode = Mode.Normal;
                    i++;
                } else {
                    i++;
                }
                break;
            }

            case Mode.Backtick: {
                if (ch === '`') {
                    mode = Mode.Normal;
                    i++;
                } else {
                    i++;
                }
                break;
            }

            case Mode.Bracket: {
                if (ch === ']') {
                    mode = Mode.Normal;
                    i++;
                } else {
                    i++;
                }
                break;
            }

            // ── Comment modes ────────────────────────────────────────
            case Mode.LineComment: {
                if (ch === '\n') {
                    mode = Mode.Normal;
                }
                i++;
                break;
            }

            case Mode.BlockComment: {
                if (ch === '*' && i + 1 < len && sql[i + 1] === '/') {
                    mode = Mode.Normal;
                    i += 2;
                } else {
                    i++;
                }
                break;
            }

            // ── Dollar-quoted body ───────────────────────────────────
            case Mode.DollarQuote: {
                if (ch === '$') {
                    const closing = `$${dollarTag}$`;
                    if (sql.startsWith(closing, i)) {
                        mode = Mode.Normal;
                        i += closing.length;
                    } else {
                        i++;
                    }
                } else {
                    i++;
                }
                break;
            }
        }
    }

    // Remaining text after last semicolon
    pushStatement(results, sql, stmtStart, len);

    return results;
}

/**
 * Find the statement that contains the given character offset.
 * Used for "Run Current Statement" — pass the cursor's char offset
 * within the full document text.
 */
export function findStatementAtOffset(sql: string, offset: number): SplitStatement | null {
    const stmts = splitStatements(sql);
    if (stmts.length === 0) return null;

    // Direct match — cursor is within a statement's text
    for (const s of stmts) {
        if (offset >= s.startOffset && offset <= s.endOffset) {
            return s;
        }
    }

    // Cursor is in a gap (on `;`, whitespace between statements, or after the last one).
    // Resolve to the nearest preceding statement.
    let best: SplitStatement | null = null;
    for (const s of stmts) {
        if (s.endOffset <= offset) {
            best = s;
        }
    }
    return best;
}

/**
 * Returns true if the given SQL fragment contains executable SQL
 * after stripping leading/trailing comments and whitespace.
 */
export function hasExecutableSQL(sql: string): boolean {
    let s = sql.trimStart();
    while (s.length > 0) {
        if (s.startsWith('--') || s.startsWith('#')) {
            const nl = s.indexOf('\n');
            s = nl === -1 ? '' : s.slice(nl + 1).trimStart();
        } else if (s.startsWith('/*')) {
            const end = s.indexOf('*/', 2);
            s = end === -1 ? '' : s.slice(end + 2).trimStart();
        } else {
            return true;
        }
    }
    return false;
}

// ── helpers ──────────────────────────────────────────────────────────

function pushStatement(
    results: SplitStatement[],
    sql: string,
    start: number,
    end: number,
) {
    const raw = sql.slice(start, end);
    const trimmed = raw.trim();
    if (trimmed.length > 0 && hasExecutableSQL(trimmed)) {
        // Compute trimmed offsets
        const leadingWs = raw.indexOf(trimmed[0]);
        const trimStart = start + leadingWs;
        const trimEnd = trimStart + trimmed.length;
        results.push({ sql: trimmed, startOffset: trimStart, endOffset: trimEnd });
    }
}

/**
 * Try to parse a dollar-quote tag starting at position `i`.
 * Returns `{ tag, endIndex }` if successful, where `endIndex` is the
 * position immediately after the closing `$` of the opening tag.
 * Tag may be empty (for `$$`) or contain identifier chars (for `$fn$`).
 */
function tryParseDollarTag(
    sql: string,
    i: number,
): { tag: string; endIndex: number } | null {
    // Must start with $
    if (sql[i] !== '$') return null;

    let j = i + 1;
    const len = sql.length;

    // Collect tag name characters (alphanumeric + underscore, must not start with digit)
    const tagStart = j;
    while (j < len && isTagChar(sql[j])) {
        j++;
    }

    // Must end with $
    if (j >= len || sql[j] !== '$') return null;

    const tag = sql.slice(tagStart, j);

    // Tag must not start with a digit (PostgreSQL rule)
    if (tag.length > 0 && /^\d/.test(tag)) return null;

    return { tag, endIndex: j + 1 };
}

function isTagChar(ch: string): boolean {
    return /[a-zA-Z0-9_]/.test(ch);
}
