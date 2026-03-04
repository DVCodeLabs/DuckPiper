import { canonicalizeSql } from '../hashing';

describe('canonicalizeSql', () => {
  it('should remove block comments', () => {
    const sql = 'SELECT /* comment */ * FROM users';
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe('select * from users');
  });

  it('should remove line comments', () => {
    const sql = `SELECT * FROM users -- this is a comment
    WHERE id = 1`;
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe('select * from users where id = 1');
  });

  it('should normalize whitespace to single spaces', () => {
    const sql = 'SELECT    *\n\nFROM\t\tusers   WHERE  id=1';
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe('select * from users where id=1');
  });

  it('should remove trailing semicolons', () => {
    const sql = 'SELECT * FROM users;';
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe('select * from users');
  });

  it('should remove multiple trailing semicolons', () => {
    const sql = 'SELECT * FROM users;;;';
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe('select * from users');
  });

  it('should convert to lowercase', () => {
    const sql = 'SELECT * FROM USERS WHERE ID = 1';
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe('select * from users where id = 1');
  });

  it('should generate consistent hash for identical queries', () => {
    const sql1 = 'SELECT * FROM users WHERE id = 1';
    const sql2 = 'SELECT    *\nFROM users\nWHERE id = 1;';

    const result1 = canonicalizeSql(sql1);
    const result2 = canonicalizeSql(sql2);

    expect(result1.canonicalText).toBe(result2.canonicalText);
    expect(result1.sqlHash).toBe(result2.sqlHash);
  });

  it('should generate different hashes for different queries', () => {
    const sql1 = 'SELECT * FROM users WHERE id = 1';
    const sql2 = 'SELECT * FROM orders WHERE id = 1';

    const result1 = canonicalizeSql(sql1);
    const result2 = canonicalizeSql(sql2);

    expect(result1.sqlHash).not.toBe(result2.sqlHash);
  });

  it('should handle multi-line block comments', () => {
    const sql = `SELECT *
    /* This is a
       multi-line
       comment */
    FROM users`;
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe('select * from users');
  });

  it('should handle empty string', () => {
    const result = canonicalizeSql('');

    expect(result.canonicalText).toBe('');
    expect(result.sqlHash).toBeDefined();
  });

  it('should handle complex SQL with multiple comment types', () => {
    const sql = `
      -- Header comment
      SELECT /* inline */ *
      FROM users
      WHERE id = 1 -- trailing comment
      /* Another block comment */
      AND status = 'active';
    `;
    const result = canonicalizeSql(sql);

    expect(result.canonicalText).toBe("select * from users where id = 1 and status = 'active'");
  });

  it('should return sqlHash as a valid SHA256 hex string', () => {
    const sql = 'SELECT * FROM users';
    const result = canonicalizeSql(sql);

    expect(result.sqlHash).toMatch(/^[a-f0-9]{64}$/);
  });
});
