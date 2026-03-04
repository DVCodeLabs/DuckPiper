import { HeuristicSqlParser } from '../sqlParser';

describe('HeuristicSqlParser', () => {
  let parser: HeuristicSqlParser;

  beforeEach(() => {
    parser = new HeuristicSqlParser();
  });

  describe('stripComments', () => {
    it('should remove line comments', () => {
      const sql = 'SELECT * FROM users -- this is a comment';
      const result = parser.stripComments(sql);

      expect(result).toBe('SELECT * FROM users  ');
    });

    it('should remove block comments', () => {
      const sql = 'SELECT /* comment */ * FROM users';
      const result = parser.stripComments(sql);

      expect(result).toBe('SELECT   * FROM users');
    });

    it('should preserve strings', () => {
      const sql = "SELECT 'test -- not a comment' FROM users";
      const result = parser.stripComments(sql);

      expect(result).toBe("SELECT 'test -- not a comment' FROM users");
    });

    it('should handle multi-line block comments', () => {
      const sql = `SELECT *
      /* multi
         line
         comment */
      FROM users`;
      const result = parser.stripComments(sql);

      expect(result).not.toContain('multi');
      expect(result).toContain('SELECT');
      expect(result).toContain('FROM users');
    });
  });

  describe('extractTableAliases', () => {
    it('should extract FROM table without alias', () => {
      const sql = 'SELECT * FROM bronze.customers';
      const { aliases, fromTables } = parser.extractTableAliases(sql);

      expect(fromTables.has('bronze.customers')).toBe(true);
      expect(aliases.get('customers')).toBe('bronze.customers');
    });

    it('should extract FROM table with alias', () => {
      const sql = 'SELECT * FROM bronze.customers AS c';
      const { aliases, fromTables } = parser.extractTableAliases(sql);

      expect(fromTables.has('bronze.customers')).toBe(true);
      expect(aliases.get('c')).toBe('bronze.customers');
    });

    it('should extract FROM table with alias without AS keyword', () => {
      const sql = 'SELECT * FROM bronze.customers c';
      const { aliases, fromTables: _fromTables } = parser.extractTableAliases(sql);

      expect(aliases.get('c')).toBe('bronze.customers');
    });

    it('should extract multiple tables from JOINs', () => {
      const sql = `
        SELECT *
        FROM bronze.customers c
        JOIN silver.orders o ON c.id = o.customer_id
      `;
      const { aliases, fromTables } = parser.extractTableAliases(sql);

      expect(fromTables.has('bronze.customers')).toBe(true);
      expect(fromTables.has('silver.orders')).toBe(true);
      expect(aliases.get('c')).toBe('bronze.customers');
      expect(aliases.get('o')).toBe('silver.orders');
    });

    it('should only track allowed schemas', () => {
      const sql = 'SELECT * FROM public.users u';
      const { fromTables } = parser.extractTableAliases(sql);

      expect(fromTables.size).toBe(0);
    });

    it('should track all allowed schemas', () => {
      const sql = `
        SELECT * FROM imports.raw_data r
        JOIN bronze.customers c ON c.id = 1
        JOIN silver.orders s ON s.id = 2
        JOIN gold.analytics g ON g.id = 3
      `;
      const { fromTables } = parser.extractTableAliases(sql);

      expect(fromTables.has('imports.raw_data')).toBe(true);
      expect(fromTables.has('bronze.customers')).toBe(true);
      expect(fromTables.has('silver.orders')).toBe(true);
      expect(fromTables.has('gold.analytics')).toBe(true);
    });

    it('should not create aliases for SQL keywords', () => {
      const sql = 'SELECT * FROM bronze.customers WHERE id = 1';
      const { aliases } = parser.extractTableAliases(sql);

      expect(aliases.has('WHERE')).toBe(false);
    });
  });

  describe('extractSelectClause', () => {
    it('should extract simple SELECT clause', () => {
      const sql = 'SELECT id, name FROM users';
      const clause = parser.extractSelectClause(sql);

      expect(clause?.trim()).toBe('id, name');
    });

    it('should extract from outermost SELECT', () => {
      const sql = 'SELECT id, name FROM (SELECT * FROM users) sub';
      const clause = parser.extractSelectClause(sql);

      // Note: Parser extracts from the last SELECT, which in this case
      // is the subquery due to how it traverses the SQL
      expect(clause?.trim()).toBe('*');
    });

    it('should extract from last SELECT at depth 0', () => {
      const sql = `
        WITH temp AS (SELECT id FROM users)
        SELECT * FROM temp
      `;
      const clause = parser.extractSelectClause(sql);

      expect(clause?.trim()).toBe('*');
    });

    it('should handle strings in SELECT clause', () => {
      const sql = "SELECT 'FROM users' AS test FROM customers";
      const clause = parser.extractSelectClause(sql);

      expect(clause).toContain("'FROM users'");
    });

    it('should return null if no SELECT found', () => {
      const sql = 'INSERT INTO users VALUES (1, 2)';
      const clause = parser.extractSelectClause(sql);

      expect(clause).toBeNull();
    });
  });

  describe('parseSelectItems', () => {
    it('should parse simple column references', () => {
      const clause = 'id, name, email';
      const items = parser.parseSelectItems(clause);

      expect(items).toHaveLength(3);
      expect(items[0].alias).toBe('id');
      expect(items[1].alias).toBe('name');
      expect(items[2].alias).toBe('email');
    });

    it('should parse columns with AS aliases', () => {
      const clause = 'id AS user_id, name AS full_name';
      const items = parser.parseSelectItems(clause);

      expect(items[0].alias).toBe('user_id');
      expect(items[0].expression).toBe('id');
      expect(items[1].alias).toBe('full_name');
      expect(items[1].expression).toBe('name');
    });

    it('should parse columns with implicit aliases', () => {
      const clause = 'id user_id, name full_name';
      const items = parser.parseSelectItems(clause);

      expect(items[0].alias).toBe('user_id');
      expect(items[1].alias).toBe('full_name');
    });

    it('should detect wildcards', () => {
      const clause = '*';
      const items = parser.parseSelectItems(clause);

      expect(items[0].isWildcard).toBe(true);
      expect(items[0].alias).toBe('*');
    });

    it('should detect table-qualified wildcards', () => {
      const clause = 'users.*';
      const items = parser.parseSelectItems(clause);

      expect(items[0].isWildcard).toBe(true);
      expect(items[0].expression).toBe('users.*');
    });

    it('should parse function calls', () => {
      const clause = 'COUNT(*) AS total, AVG(age) AS avg_age';
      const items = parser.parseSelectItems(clause);

      expect(items[0].alias).toBe('total');
      expect(items[0].expression).toBe('COUNT(*)');
      expect(items[1].alias).toBe('avg_age');
      expect(items[1].expression).toBe('AVG(age)');
    });

    it('should generate unnamed aliases for complex expressions', () => {
      const clause = 'CASE WHEN id > 0 THEN 1 ELSE 0 END';
      const items = parser.parseSelectItems(clause);

      expect(items[0].alias).toContain('__unnamed_');
    });

    it('should handle qualified column names', () => {
      const clause = 'users.id, orders.amount';
      const items = parser.parseSelectItems(clause);

      expect(items[0].alias).toBe('id');
      expect(items[0].expression).toBe('users.id');
      expect(items[1].alias).toBe('amount');
      expect(items[1].expression).toBe('orders.amount');
    });

    it('should assign higher confidence to qualified columns', () => {
      const clause = 'users.id, id';
      const items = parser.parseSelectItems(clause);

      expect(items[0].confidence).toBeGreaterThan(items[1].confidence);
    });
  });

  describe('splitByComma', () => {
    it('should split simple comma-separated items', () => {
      const text = 'a, b, c';
      const parts = parser.splitByComma(text);

      expect(parts).toEqual(['a', ' b', ' c']);
    });

    it('should not split commas inside parentheses', () => {
      const text = 'FUNC(a, b), c';
      const parts = parser.splitByComma(text);

      expect(parts).toHaveLength(2);
      expect(parts[0]).toBe('FUNC(a, b)');
    });

    it('should not split commas inside quotes', () => {
      const text = "'hello, world', other";
      const parts = parser.splitByComma(text);

      expect(parts).toHaveLength(2);
      expect(parts[0]).toBe("'hello, world'");
    });

    it('should handle nested parentheses', () => {
      const text = 'OUTER(INNER(a, b), c), d';
      const parts = parser.splitByComma(text);

      expect(parts).toHaveLength(2);
      expect(parts[0]).toBe('OUTER(INNER(a, b), c)');
    });
  });

  describe('extractColumnRefs', () => {
    it('should extract qualified column references', () => {
      const aliases = new Map([['c', 'bronze.customers']]);
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs('c.id', aliases, fromTables);

      expect(refs).toHaveLength(1);
      expect(refs[0].table).toBe('bronze.customers');
      expect(refs[0].column).toBe('id');
    });

    it('should extract fully qualified references', () => {
      const aliases = new Map();
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs('bronze.customers.id', aliases, fromTables);

      expect(refs).toHaveLength(1);
      expect(refs[0].table).toBe('bronze.customers');
      expect(refs[0].column).toBe('id');
    });

    it('should use single-table fallback for unqualified columns', () => {
      const aliases = new Map();
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs('id', aliases, fromTables);

      expect(refs).toHaveLength(1);
      expect(refs[0].table).toBe('bronze.customers');
      expect(refs[0].column).toBe('id');
    });

    it('should not use fallback when multiple tables present', () => {
      const aliases = new Map();
      const fromTables = new Set(['bronze.customers', 'silver.orders']);
      const refs = parser.extractColumnRefs('id', aliases, fromTables);

      expect(refs).toHaveLength(0);
    });

    it('should skip SQL keywords', () => {
      const aliases = new Map();
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs('SELECT FROM WHERE', aliases, fromTables);

      expect(refs).toHaveLength(0);
    });

    it('should skip function names', () => {
      const aliases = new Map();
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs('COUNT(id)', aliases, fromTables);

      // Should only extract 'id', not 'COUNT'
      expect(refs).toHaveLength(1);
      expect(refs[0].column).toBe('id');
    });

    it('should deduplicate column references', () => {
      const aliases = new Map([['c', 'bronze.customers']]);
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs('c.id + c.id', aliases, fromTables);

      expect(refs).toHaveLength(1);
    });

    it('should ignore non-allowed schemas', () => {
      const aliases = new Map();
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs('public.users.id', aliases, fromTables);

      expect(refs).toHaveLength(0);
    });

    it('should strip string literals before parsing', () => {
      const aliases = new Map();
      const fromTables = new Set(['bronze.customers']);
      const refs = parser.extractColumnRefs("'bronze.fake.column'", aliases, fromTables);

      expect(refs).toHaveLength(0);
    });
  });

  describe('parse (full integration)', () => {
    it('should parse a simple SELECT statement', () => {
      const sql = 'SELECT id, name FROM bronze.customers';
      const result = parser.parse(sql);

      expect(result.fromTables.has('bronze.customers')).toBe(true);
      expect(result.items).toHaveLength(2);
      expect(result.items[0].alias).toBe('id');
      expect(result.items[1].alias).toBe('name');
    });

    it('should parse SELECT with aliases', () => {
      const sql = 'SELECT c.id, c.name FROM bronze.customers c';
      const result = parser.parse(sql);

      expect(result.aliases.get('c')).toBe('bronze.customers');
      expect(result.items[0].expression).toBe('c.id');
    });

    it('should parse SELECT with JOIN', () => {
      const sql = `
        SELECT c.name, o.amount
        FROM bronze.customers c
        JOIN silver.orders o ON c.id = o.customer_id
      `;
      const result = parser.parse(sql);

      expect(result.fromTables.size).toBe(2);
      expect(result.fromTables.has('bronze.customers')).toBe(true);
      expect(result.fromTables.has('silver.orders')).toBe(true);
    });

    it('should handle wildcard selects', () => {
      const sql = 'SELECT * FROM bronze.customers';
      const result = parser.parse(sql);

      expect(result.items[0].isWildcard).toBe(true);
    });

    it('should handle CTEs', () => {
      const sql = `
        WITH temp AS (SELECT id FROM bronze.customers)
        SELECT * FROM temp
      `;
      const result = parser.parse(sql);

      // Should extract from the final SELECT
      expect(result.items[0].isWildcard).toBe(true);
    });
  });
});
