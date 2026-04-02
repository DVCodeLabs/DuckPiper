import { splitStatements, findStatementAtOffset, hasExecutableSQL } from '../sqlSplitter';

describe('sqlSplitter', () => {
    describe('splitStatements', () => {
        it('should split two simple statements', () => {
            const stmts = splitStatements('SELECT 1; SELECT 2');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT 1');
            expect(stmts[1].sql).toBe('SELECT 2');
        });

        it('should handle trailing semicolon without producing empty statement', () => {
            const stmts = splitStatements('SELECT 1; SELECT 2;');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT 1');
            expect(stmts[1].sql).toBe('SELECT 2');
        });

        it('should handle multiple trailing semicolons', () => {
            const stmts = splitStatements('SELECT 1;;;');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toBe('SELECT 1');
        });

        it('should return single statement when no semicolons', () => {
            const stmts = splitStatements('SELECT * FROM users');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toBe('SELECT * FROM users');
        });

        it('should discard empty statements', () => {
            const stmts = splitStatements('; ; SELECT 1; ;');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toBe('SELECT 1');
        });

        it('should return empty array for whitespace-only input', () => {
            const stmts = splitStatements('   \n\t  ');
            expect(stmts).toHaveLength(0);
        });

        it('should return empty array for empty string', () => {
            const stmts = splitStatements('');
            expect(stmts).toHaveLength(0);
        });

        // ── Strings ──────────────────────────────────────────────

        it('should not split on semicolons inside single-quoted strings', () => {
            const stmts = splitStatements("SELECT 'a;b'; SELECT 2");
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe("SELECT 'a;b'");
            expect(stmts[1].sql).toBe('SELECT 2');
        });

        it('should handle escaped single quotes', () => {
            const stmts = splitStatements("SELECT 'it''s;here'; SELECT 2");
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe("SELECT 'it''s;here'");
        });

        it('should not split on semicolons inside double-quoted identifiers', () => {
            const stmts = splitStatements('SELECT "col;name" FROM t; SELECT 2');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT "col;name" FROM t');
        });

        it('should handle escaped double quotes', () => {
            const stmts = splitStatements('SELECT "a""b;c" FROM t; SELECT 2');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT "a""b;c" FROM t');
        });

        // ── Backtick / Bracket identifiers ───────────────────────

        it('should not split on semicolons inside backtick identifiers', () => {
            const stmts = splitStatements('SELECT `col;name` FROM t; SELECT 2');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT `col;name` FROM t');
        });

        it('should not split on semicolons inside bracket identifiers', () => {
            const stmts = splitStatements('SELECT [col;name] FROM t; SELECT 2');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT [col;name] FROM t');
        });

        // ── Comments ─────────────────────────────────────────────

        it('should not split on semicolons inside line comments', () => {
            const stmts = splitStatements('SELECT 1 -- comment;still comment\n; SELECT 2');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toContain('SELECT 1');
            expect(stmts[0].sql).toContain('comment;still comment');
        });

        it('should not split on semicolons inside block comments', () => {
            const stmts = splitStatements('SELECT 1 /* ;not a split; */ ; SELECT 2');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT 1 /* ;not a split; */');
        });

        it('should handle multi-line block comments', () => {
            const sql = `SELECT 1 /* multi
line ; comment
*/; SELECT 2`;
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(2);
            expect(stmts[1].sql).toBe('SELECT 2');
        });

        // ── Dollar quoting ───────────────────────────────────────

        it('should not split on semicolons inside $$ dollar quotes', () => {
            const sql = "CREATE FUNCTION f() RETURNS void AS $$ BEGIN; END; $$ LANGUAGE plpgsql; SELECT 1";
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toContain('BEGIN; END;');
            expect(stmts[1].sql).toBe('SELECT 1');
        });

        it('should not split on semicolons inside $tag$ dollar quotes', () => {
            const sql = "CREATE FUNCTION f() AS $fn$ BEGIN; END; $fn$ LANGUAGE plpgsql; SELECT 1";
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toContain('BEGIN; END;');
        });

        it('should not confuse different dollar-quote tags', () => {
            // $fn$ body contains $$ which should NOT close the $fn$ block
            const sql = "SELECT $fn$ hello $$ world; $fn$; SELECT 2";
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toContain('hello $$ world;');
        });

        it('should handle empty dollar-quoted body', () => {
            const sql = "SELECT $$$$; SELECT 2";
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(2);
        });

        // ── Offsets ──────────────────────────────────────────────

        it('should track correct offsets', () => {
            const sql = '  SELECT 1;  SELECT 2  ';
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(2);
            expect(stmts[0].startOffset).toBe(2);  // 'S' in 'SELECT 1'
            expect(stmts[0].endOffset).toBe(10);    // after '1'
            expect(stmts[1].startOffset).toBe(13);  // 'S' in 'SELECT 2'
            expect(stmts[1].endOffset).toBe(21);    // after '2'
        });

        // ── Multi-statement scripts ──────────────────────────────

        it('should handle a real migration script', () => {
            const sql = `
CREATE TABLE users (id INT, name TEXT);
INSERT INTO users VALUES (1, 'Alice');
INSERT INTO users VALUES (2, 'Bob');
SELECT * FROM users;
`;
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(4);
            expect(stmts[0].sql).toBe('CREATE TABLE users (id INT, name TEXT)');
            expect(stmts[1].sql).toBe("INSERT INTO users VALUES (1, 'Alice')");
            expect(stmts[2].sql).toBe("INSERT INTO users VALUES (2, 'Bob')");
            expect(stmts[3].sql).toBe('SELECT * FROM users');
        });

        it('should handle script with session setup and comments', () => {
            const sql = `
-- Set search path
SET search_path TO myschema;
-- Create temp table
CREATE TEMP TABLE t AS SELECT 1 AS id;
-- Query it
SELECT * FROM t;
`;
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(3);
            expect(stmts[0].sql).toContain('SET search_path');
            expect(stmts[1].sql).toContain('CREATE TEMP TABLE');
            expect(stmts[2].sql).toContain('SELECT * FROM t');
        });
    });

    describe('findStatementAtOffset', () => {
        const sql = 'SELECT 1; SELECT 2; SELECT 3';

        it('should find first statement when cursor is at start', () => {
            const stmt = findStatementAtOffset(sql, 0);
            expect(stmt).not.toBeNull();
            expect(stmt!.sql).toBe('SELECT 1');
        });

        it('should find second statement when cursor is in middle', () => {
            // 'SELECT 1; SELECT 2; SELECT 3'
            //            ^-- offset 10
            const stmt = findStatementAtOffset(sql, 13);
            expect(stmt).not.toBeNull();
            expect(stmt!.sql).toBe('SELECT 2');
        });

        it('should find third statement when cursor is near end', () => {
            const stmt = findStatementAtOffset(sql, 25);
            expect(stmt).not.toBeNull();
            expect(stmt!.sql).toBe('SELECT 3');
        });

        it('should find the last statement when cursor is past all text', () => {
            const stmt = findStatementAtOffset(sql, 100);
            expect(stmt).not.toBeNull();
            expect(stmt!.sql).toBe('SELECT 3');
        });

        it('should find statement when cursor is on the semicolon boundary', () => {
            // offset 8 is ';', offset 9 is ' ' between statements
            // Both are gaps — resolve to the preceding statement
            const stmt = findStatementAtOffset('SELECT 1; SELECT 2', 8);
            expect(stmt).not.toBeNull();
            expect(stmt!.sql).toBe('SELECT 1');

            const stmt2 = findStatementAtOffset('SELECT 1; SELECT 2', 9);
            expect(stmt2).not.toBeNull();
            expect(stmt2!.sql).toBe('SELECT 1');
        });

        it('should return null for empty SQL', () => {
            const stmt = findStatementAtOffset('', 0);
            expect(stmt).toBeNull();
        });

        it('should handle cursor in whitespace-only area before first statement', () => {
            const stmt = findStatementAtOffset('   SELECT 1', 0);
            expect(stmt === null || stmt!.sql === 'SELECT 1').toBe(true);
        });
    });

    describe('comment-only fragment filtering', () => {
        it('should not produce a statement from leading line comments before a query', () => {
            const stmts = splitStatements('-- note\n-- more\nSELECT 1;');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toContain('SELECT 1');
        });

        it('should not produce a statement from leading block comments before a query', () => {
            const stmts = splitStatements('/* note */\nSELECT 1;');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toContain('SELECT 1');
        });

        it('should not produce an extra statement from trailing line comment after final semicolon', () => {
            const stmts = splitStatements('SELECT 1;\n-- trailing note');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toBe('SELECT 1');
        });

        it('should not produce an extra statement from trailing block comment after final semicolon', () => {
            const stmts = splitStatements('SELECT 1;\n/* trailing note */');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toBe('SELECT 1');
        });

        it('should return zero statements for comment-only input', () => {
            expect(splitStatements('-- just a comment')).toHaveLength(0);
            expect(splitStatements('/* block comment */')).toHaveLength(0);
            expect(splitStatements('-- line\n/* block */')).toHaveLength(0);
        });

        it('should not create standalone statement from comments between two statements', () => {
            const stmts = splitStatements('SELECT 1;\n-- middle comment\nSELECT 2;');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT 1');
            expect(stmts[1].sql).toContain('SELECT 2');
        });

        it('should produce two statements for SELECT 1; -- note\\nSELECT 2;', () => {
            const stmts = splitStatements('SELECT 1; -- note\nSELECT 2;');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT 1');
            expect(stmts[1].sql).toContain('SELECT 2');
        });

        it('should produce two statements for leading comment + DELETE + comment + SELECT', () => {
            const sql = '-- Script\nDELETE FROM customers WHERE order_total = 0;\n-- delete all\nSELECT * FROM customers WHERE order_total = 0;\n-- verify';
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toContain('DELETE FROM customers');
            expect(stmts[1].sql).toContain('SELECT * FROM customers');
        });

        it('should produce one statement for the full spec example', () => {
            const sql = `-- Returns the top 10 customers by total spend
SELECT
    c.customer_id,
    c.customer_name, -- Customer display name
    SUM(o.order_total) AS total_spent -- Sum all order amounts per customer
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id -- Match orders to their customer
GROUP BY c.customer_id, c.customer_name       -- Aggregate one row per customer
ORDER BY total_spent DESC            -- Highest spenders first
LIMIT 10;                            -- Return only the top 10`;
            const stmts = splitStatements(sql);
            expect(stmts).toHaveLength(1);
        });

        it('should return zero statements for # comment-only input', () => {
            expect(splitStatements('# MySQL comment')).toHaveLength(0);
        });

        it('should produce one statement for # comment before query', () => {
            const stmts = splitStatements('# comment\nSELECT 1;');
            expect(stmts).toHaveLength(1);
            expect(stmts[0].sql).toContain('SELECT 1');
        });

        it('should not mis-split on semicolons inside # comments', () => {
            const stmts = splitStatements('SELECT 1; # comment with ; in it\nSELECT 2;');
            expect(stmts).toHaveLength(2);
            expect(stmts[0].sql).toBe('SELECT 1');
            expect(stmts[1].sql).toContain('SELECT 2');
        });
    });

    describe('findStatementAtOffset with comments', () => {
        it('should resolve cursor in leading comment to the real statement', () => {
            const sql = '-- note\nSELECT 1;';
            const stmt = findStatementAtOffset(sql, 3); // inside "-- note"
            expect(stmt).not.toBeNull();
            expect(stmt!.sql).toContain('SELECT 1');
        });

        it('should resolve cursor in trailing comment to preceding statement', () => {
            const sql = 'SELECT 1;\n-- trailing';
            const stmt = findStatementAtOffset(sql, 15); // inside "-- trailing"
            expect(stmt).not.toBeNull();
            expect(stmt!.sql).toBe('SELECT 1');
        });
    });

    describe('hasExecutableSQL', () => {
        it('should return false for pure -- line comment', () => {
            expect(hasExecutableSQL('-- just a comment')).toBe(false);
        });

        it('should return false for pure # line comment', () => {
            expect(hasExecutableSQL('# just a comment')).toBe(false);
        });

        it('should return false for pure block comment', () => {
            expect(hasExecutableSQL('/* block comment */')).toBe(false);
        });

        it('should return false for mixed comments', () => {
            expect(hasExecutableSQL('-- line\n/* block */\n# hash')).toBe(false);
        });

        it('should return true for comment followed by SQL', () => {
            expect(hasExecutableSQL('-- comment\nSELECT 1')).toBe(true);
        });

        it('should return true for plain SQL', () => {
            expect(hasExecutableSQL('SELECT 1')).toBe(true);
        });
    });
});
