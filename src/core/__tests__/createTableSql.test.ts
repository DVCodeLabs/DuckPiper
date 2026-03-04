import { buildCreateTableSql, CreateTableDraft } from '../createTableSql';

function buildDraft(overrides: Partial<CreateTableDraft> = {}): CreateTableDraft {
  return {
    tableName: 'users',
    columns: [
      { name: 'id', type: 'BIGINT', nullable: false },
      { name: 'email', type: 'VARCHAR(255)', nullable: false },
      { name: 'org_id', type: 'BIGINT', nullable: false }
    ],
    ...overrides
  };
}

describe('buildCreateTableSql', () => {
  it('builds create table with single-column primary key', () => {
    const result = buildCreateTableSql({
      dialect: 'postgres',
      schemaName: 'public',
      draft: buildDraft({
        primaryKey: { columns: ['id'] }
      })
    });

    expect(result.statements[0]).toContain('CREATE TABLE "public"."users"');
    expect(result.statements[0]).toContain('PRIMARY KEY ("id")');
  });

  it('builds create table with multi-column primary key', () => {
    const result = buildCreateTableSql({
      dialect: 'postgres',
      schemaName: 'public',
      draft: buildDraft({
        primaryKey: { name: 'pk_users', columns: ['id', 'org_id'] }
      })
    });

    expect(result.statements[0]).toContain('CONSTRAINT "pk_users" PRIMARY KEY ("id", "org_id")');
  });

  it('builds unique constraints', () => {
    const result = buildCreateTableSql({
      dialect: 'postgres',
      schemaName: 'public',
      draft: buildDraft({
        uniques: [
          { name: 'uq_users_email', columns: ['email'] }
        ]
      })
    });

    expect(result.statements[0]).toContain('CONSTRAINT "uq_users_email" UNIQUE ("email")');
  });

  it('builds foreign key with on update and on delete', () => {
    const result = buildCreateTableSql({
      dialect: 'postgres',
      schemaName: 'public',
      draft: buildDraft({
        foreignKeys: [
          {
            name: 'fk_users_org',
            columns: ['org_id'],
            referencedSchema: 'public',
            referencedTable: 'orgs',
            referencedColumns: ['id'],
            onUpdate: 'CASCADE',
            onDelete: 'RESTRICT'
          }
        ]
      })
    });

    expect(result.statements[0]).toContain('CONSTRAINT "fk_users_org" FOREIGN KEY ("org_id") REFERENCES "public"."orgs" ("id") ON UPDATE CASCADE ON DELETE RESTRICT');
  });

  it('builds check constraint', () => {
    const result = buildCreateTableSql({
      dialect: 'postgres',
      schemaName: 'public',
      draft: buildDraft({
        checks: [
          { name: 'chk_email_len', expression: 'char_length(email) > 3' }
        ]
      })
    });

    expect(result.statements[0]).toContain('CONSTRAINT "chk_email_len" CHECK (char_length(email) > 3)');
  });

  it('builds index statements', () => {
    const result = buildCreateTableSql({
      dialect: 'postgres',
      schemaName: 'public',
      draft: buildDraft({
        indexes: [
          { name: 'idx_users_email', columns: ['email'] },
          { columns: ['org_id', 'email'], unique: true }
        ]
      })
    });

    expect(result.statements).toHaveLength(3);
    expect(result.statements[1]).toContain('CREATE INDEX "idx_users_email" ON "public"."users" ("email");');
    expect(result.statements[2]).toContain('CREATE UNIQUE INDEX');
  });

  it('quotes identifiers for mysql', () => {
    const result = buildCreateTableSql({
      dialect: 'mysql',
      schemaName: 'analytics',
      draft: buildDraft({
        tableName: 'table`name',
        columns: [
          { name: 'col`1', type: 'INT' }
        ]
      })
    });

    expect(result.statements[0]).toContain('CREATE TABLE `analytics`.`table``name`');
    expect(result.statements[0]).toContain('`col``1` INT');
  });

  it('supports snowflake database.schema names', () => {
    const result = buildCreateTableSql({
      dialect: 'snowflake',
      schemaName: 'APP.PUBLIC',
      draft: buildDraft({ tableName: 'events' })
    });

    expect(result.targetLabel).toBe('"APP"."PUBLIC"."events"');
  });

  it('throws validation errors for missing table name', () => {
    expect(() =>
      buildCreateTableSql({
        dialect: 'postgres',
        schemaName: 'public',
        draft: buildDraft({ tableName: '' })
      })
    ).toThrow('Table name is required.');
  });

  it('throws validation errors for missing columns', () => {
    expect(() =>
      buildCreateTableSql({
        dialect: 'postgres',
        schemaName: 'public',
        draft: buildDraft({ columns: [] })
      })
    ).toThrow('At least one column is required.');
  });

  it('throws validation errors for bad foreign key column references', () => {
    expect(() =>
      buildCreateTableSql({
        dialect: 'postgres',
        schemaName: 'public',
        draft: buildDraft({
          foreignKeys: [
            {
              columns: ['missing_col'],
              referencedTable: 'orgs',
              referencedColumns: ['id']
            }
          ]
        })
      })
    ).toThrow("Foreign key column 'missing_col' is not defined in table columns.");
  });
});
