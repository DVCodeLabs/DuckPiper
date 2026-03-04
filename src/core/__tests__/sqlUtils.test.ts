import { quoteIdentifier, quoteLiteral, resolveEffectiveSqlDialect, sanitizeIdentifierName } from '../sqlUtils';
import { ConnectionProfile } from '../types';

const createProfile = (dialect: string, sqlDialect?: string): ConnectionProfile => ({
  id: 'test-connection-id',
  name: 'Test Connection',
  dialect,
  sqlDialect,
  createdAt: '2026-01-01T00:00:00.000Z',
  updatedAt: '2026-01-01T00:00:00.000Z',
});

describe('sqlUtils', () => {
  describe('quoteIdentifier', () => {
    it('should quote identifiers with backticks for MySQL', () => {
      const result = quoteIdentifier('mysql', 'table_name');
      expect(result).toBe('`table_name`');
    });

    it('should quote identifiers with backticks for MariaDB', () => {
      const result = quoteIdentifier(resolveEffectiveSqlDialect(createProfile('mysql', 'mariadb')), 'table_name');
      expect(result).toBe('`table_name`');
    });

    it('should escape backticks in MySQL identifiers', () => {
      const result = quoteIdentifier('mysql', 'table`name');
      expect(result).toBe('`table``name`');
    });

    it('should quote identifiers with square brackets for MSSQL', () => {
      const result = quoteIdentifier('mssql', 'table_name');
      expect(result).toBe('[table_name]');
    });

    it('should escape square brackets in MSSQL identifiers', () => {
      const result = quoteIdentifier('mssql', 'table]name');
      expect(result).toBe('[table]]name]');
    });

    it('should quote identifiers with double quotes for PostgreSQL', () => {
      const result = quoteIdentifier('postgres', 'table_name');
      expect(result).toBe('"table_name"');
    });

    it('should escape double quotes in PostgreSQL identifiers', () => {
      const result = quoteIdentifier('postgres', 'table"name');
      expect(result).toBe('"table""name"');
    });

    it('should quote identifiers with double quotes for DuckDB', () => {
      const result = quoteIdentifier('duckdb', 'table_name');
      expect(result).toBe('"table_name"');
    });

    it('should quote identifiers with double quotes for Snowflake', () => {
      const result = quoteIdentifier('snowflake', 'table_name');
      expect(result).toBe('"table_name"');
    });

    it('should handle identifiers with spaces', () => {
      const result = quoteIdentifier('postgres', 'my table');
      expect(result).toBe('"my table"');
    });

    it('should handle identifiers with special characters', () => {
      const result = quoteIdentifier('postgres', 'table-name.test');
      expect(result).toBe('"table-name.test"');
    });
  });

  describe('resolveEffectiveSqlDialect', () => {
    it('should normalize postgresql to postgres', () => {
      const result = resolveEffectiveSqlDialect(createProfile('postgres', 'postgresql'));
      expect(result).toBe('postgres');
    });

    it('should normalize mariadb to mysql', () => {
      const result = resolveEffectiveSqlDialect(createProfile('mysql', 'mariadb'));
      expect(result).toBe('mysql');
    });
  });

  describe('quoteLiteral', () => {
    it('should quote string literals with single quotes', () => {
      const result = quoteLiteral('Hello World');
      expect(result).toBe("'Hello World'");
    });

    it('should escape single quotes by doubling them', () => {
      const result = quoteLiteral("O'Brien");
      expect(result).toBe("'O''Brien'");
    });

    it('should escape backslashes', () => {
      const result = quoteLiteral('C:\\path\\to\\file');
      expect(result).toBe("'C:\\\\path\\\\to\\\\file'");
    });

    it('should handle empty strings', () => {
      const result = quoteLiteral('');
      expect(result).toBe("''");
    });

    it('should handle strings with both backslashes and quotes', () => {
      const result = quoteLiteral("It's a \\test\\");
      expect(result).toBe("'It''s a \\\\test\\\\'");
    });

    it('should handle multi-line strings', () => {
      const result = quoteLiteral('Line 1\nLine 2');
      expect(result).toBe("'Line 1\nLine 2'");
    });

    it('should handle strings with special SQL characters', () => {
      const result = quoteLiteral('SELECT * FROM users; DROP TABLE users;--');
      expect(result).toBe("'SELECT * FROM users; DROP TABLE users;--'");
    });
  });

  describe('sanitizeIdentifierName', () => {
    it('should keep alphanumeric characters and underscores', () => {
      const result = sanitizeIdentifierName('table_name_123');
      expect(result).toBe('table_name_123');
    });

    it('should replace spaces with underscores', () => {
      const result = sanitizeIdentifierName('my table name');
      expect(result).toBe('my_table_name');
    });

    it('should replace hyphens with underscores', () => {
      const result = sanitizeIdentifierName('my-table-name');
      expect(result).toBe('my_table_name');
    });

    it('should replace dots with underscores', () => {
      const result = sanitizeIdentifierName('schema.table.name');
      expect(result).toBe('schema_table_name');
    });

    it('should replace all special characters', () => {
      const result = sanitizeIdentifierName('table@name!#$');
      expect(result).toBe('table_name___');
    });

    it('should handle empty string', () => {
      const result = sanitizeIdentifierName('');
      expect(result).toBe('');
    });

    it('should preserve uppercase letters', () => {
      const result = sanitizeIdentifierName('MyTableName');
      expect(result).toBe('MyTableName');
    });

    it('should handle unicode characters by replacing them', () => {
      const result = sanitizeIdentifierName('table_name_🚀');
      expect(result).toBe('table_name___');
    });

    it('should handle path separators for security', () => {
      const result = sanitizeIdentifierName('../../../etc/passwd');
      expect(result).toBe('_________etc_passwd');
    });
  });
});
