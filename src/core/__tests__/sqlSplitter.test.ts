import { splitStatements, findStatementAtOffset } from '../sqlSplitter';

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
});
