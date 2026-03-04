import { DbDialect } from './types';
import { quoteIdentifier, sanitizeIdentifierName } from './sqlUtils';

export type ReferentialAction = 'NO ACTION' | 'RESTRICT' | 'CASCADE' | 'SET NULL' | 'SET DEFAULT';

export interface ColumnDraft {
  name: string;
  type: string;
  nullable?: boolean;
  defaultExpression?: string;
}

export interface PrimaryKeyDraft {
  name?: string;
  columns: string[];
}

export interface UniqueConstraintDraft {
  name?: string;
  columns: string[];
}

export interface ForeignKeyDraft {
  name?: string;
  columns: string[];
  referencedSchema?: string;
  referencedTable: string;
  referencedColumns: string[];
  onUpdate?: ReferentialAction;
  onDelete?: ReferentialAction;
}

export interface CheckConstraintDraft {
  name?: string;
  expression: string;
}

export interface IndexDraft {
  name?: string;
  columns: string[];
  unique?: boolean;
}

export interface CreateTableDraft {
  tableName: string;
  schemaName?: string;
  columns: ColumnDraft[];
  primaryKey?: PrimaryKeyDraft | null;
  uniques?: UniqueConstraintDraft[];
  foreignKeys?: ForeignKeyDraft[];
  checks?: CheckConstraintDraft[];
  indexes?: IndexDraft[];
}

export interface BuildCreateTableSqlParams {
  dialect: DbDialect;
  schemaName?: string;
  draft: CreateTableDraft;
}

export interface BuildCreateTableSqlResult {
  targetLabel: string;
  statements: string[];
}

function _isBlank(value: unknown): boolean {
  return String(value ?? '').trim().length === 0;
}

function normalizeIdentifierList(values: string[] | undefined): string[] {
  return (values ?? []).map((value) => value.trim()).filter((value) => value.length > 0);
}

function validateReferentialAction(action: string | undefined): action is ReferentialAction {
  if (!action) return true;
  return ['NO ACTION', 'RESTRICT', 'CASCADE', 'SET NULL', 'SET DEFAULT'].includes(action);
}

function buildQualifiedName(dialect: DbDialect, schemaName: string | undefined, objectName: string): string {
  if (!schemaName) {
    return quoteIdentifier(dialect, objectName);
  }

  if (dialect === 'snowflake' && schemaName.includes('.')) {
    const parts = schemaName.split('.').map((part) => part.trim()).filter((part) => part.length > 0);
    if (parts.length >= 2) {
      const database = parts[0];
      const schema = parts.slice(1).join('.');
      return `${quoteIdentifier(dialect, database)}.${quoteIdentifier(dialect, schema)}.${quoteIdentifier(dialect, objectName)}`;
    }
  }

  return `${quoteIdentifier(dialect, schemaName)}.${quoteIdentifier(dialect, objectName)}`;
}

function quoteConstraintName(dialect: DbDialect, name: string | undefined): string {
  if (!name || name.trim().length === 0) return '';
  return `CONSTRAINT ${quoteIdentifier(dialect, name.trim())} `;
}

export function buildCreateTableSql(params: BuildCreateTableSqlParams): BuildCreateTableSqlResult {
  const { dialect } = params;
  const effectiveSchemaName = params.draft.schemaName ?? params.schemaName;

  const tableName = (params.draft.tableName ?? '').trim();
  if (tableName.length === 0) {
    throw new Error('Table name is required.');
  }

  const columns = params.draft.columns ?? [];
  if (columns.length === 0) {
    throw new Error('At least one column is required.');
  }

  const seenColumns = new Set<string>();
  const columnDefinitions: string[] = [];

  for (const column of columns) {
    const columnName = (column.name ?? '').trim();
    const columnType = (column.type ?? '').trim();

    if (columnName.length === 0) {
      throw new Error('Column name is required for every column.');
    }
    if (columnType.length === 0) {
      throw new Error(`Column type is required for column '${columnName}'.`);
    }

    const normalized = columnName.toLowerCase();
    if (seenColumns.has(normalized)) {
      throw new Error(`Duplicate column name '${columnName}'.`);
    }
    seenColumns.add(normalized);

    const nullableSql = column.nullable === false ? ' NOT NULL' : '';
    const defaultExpression = (column.defaultExpression ?? '').trim();
    const defaultSql = defaultExpression.length > 0 ? ` DEFAULT ${defaultExpression}` : '';

    columnDefinitions.push(`${quoteIdentifier(dialect, columnName)} ${columnType}${defaultSql}${nullableSql}`);
  }

  const tableConstraints: string[] = [];

  const primaryKey = params.draft.primaryKey;
  if (primaryKey && normalizeIdentifierList(primaryKey.columns).length > 0) {
    const pkColumns = normalizeIdentifierList(primaryKey.columns);
    for (const columnName of pkColumns) {
      if (!seenColumns.has(columnName.toLowerCase())) {
        throw new Error(`Primary key column '${columnName}' is not defined in table columns.`);
      }
    }
    tableConstraints.push(`${quoteConstraintName(dialect, primaryKey.name)}PRIMARY KEY (${pkColumns.map((name) => quoteIdentifier(dialect, name)).join(', ')})`);
  }

  for (const unique of params.draft.uniques ?? []) {
    const uniqueColumns = normalizeIdentifierList(unique.columns);
    if (uniqueColumns.length === 0) {
      continue;
    }

    for (const columnName of uniqueColumns) {
      if (!seenColumns.has(columnName.toLowerCase())) {
        throw new Error(`Unique constraint column '${columnName}' is not defined in table columns.`);
      }
    }

    tableConstraints.push(`${quoteConstraintName(dialect, unique.name)}UNIQUE (${uniqueColumns.map((name) => quoteIdentifier(dialect, name)).join(', ')})`);
  }

  for (const check of params.draft.checks ?? []) {
    const expression = (check.expression ?? '').trim();
    if (expression.length === 0) {
      continue;
    }
    tableConstraints.push(`${quoteConstraintName(dialect, check.name)}CHECK (${expression})`);
  }

  for (const fk of params.draft.foreignKeys ?? []) {
    const localColumns = normalizeIdentifierList(fk.columns);
    const referencedColumns = normalizeIdentifierList(fk.referencedColumns);
    const referencedTable = (fk.referencedTable ?? '').trim();

    if (localColumns.length === 0 && referencedColumns.length === 0 && referencedTable.length === 0) {
      continue;
    }

    if (localColumns.length === 0) {
      throw new Error('Foreign key requires at least one local column.');
    }
    if (referencedColumns.length === 0) {
      throw new Error('Foreign key requires at least one referenced column.');
    }
    if (localColumns.length !== referencedColumns.length) {
      throw new Error('Foreign key local/referenced column counts must match.');
    }
    if (referencedTable.length === 0) {
      throw new Error('Foreign key referenced table is required.');
    }

    for (const columnName of localColumns) {
      if (!seenColumns.has(columnName.toLowerCase())) {
        throw new Error(`Foreign key column '${columnName}' is not defined in table columns.`);
      }
    }

    const onUpdate = fk.onUpdate?.trim().toUpperCase();
    const onDelete = fk.onDelete?.trim().toUpperCase();

    if (!validateReferentialAction(onUpdate)) {
      throw new Error(`Invalid ON UPDATE action '${fk.onUpdate}'.`);
    }
    if (!validateReferentialAction(onDelete)) {
      throw new Error(`Invalid ON DELETE action '${fk.onDelete}'.`);
    }

    const referencedSchema = (fk.referencedSchema ?? '').trim() || effectiveSchemaName;
    const referencedTableName = buildQualifiedName(dialect, referencedSchema, referencedTable);

    const onUpdateSql = onUpdate ? ` ON UPDATE ${onUpdate}` : '';
    const onDeleteSql = onDelete ? ` ON DELETE ${onDelete}` : '';

    tableConstraints.push(
      `${quoteConstraintName(dialect, fk.name)}FOREIGN KEY (${localColumns.map((name) => quoteIdentifier(dialect, name)).join(', ')}) REFERENCES ${referencedTableName} (${referencedColumns.map((name) => quoteIdentifier(dialect, name)).join(', ')})${onUpdateSql}${onDeleteSql}`
    );
  }

  const tableNameSql = buildQualifiedName(dialect, effectiveSchemaName, tableName);
  const allDefinitions = [...columnDefinitions, ...tableConstraints];
  const createTableSql = `CREATE TABLE ${tableNameSql} (\n  ${allDefinitions.join(',\n  ')}\n);`;

  const statements = [createTableSql];

  for (const [index, idx] of (params.draft.indexes ?? []).entries()) {
    const indexColumns = normalizeIdentifierList(idx.columns);
    if (indexColumns.length === 0) {
      continue;
    }

    for (const columnName of indexColumns) {
      if (!seenColumns.has(columnName.toLowerCase())) {
        throw new Error(`Index column '${columnName}' is not defined in table columns.`);
      }
    }

    const fallbackName = sanitizeIdentifierName(`idx_${tableName}_${indexColumns.join('_')}_${index + 1}`);
    const indexName = (idx.name ?? '').trim() || fallbackName;
    const uniqueSql = idx.unique ? 'UNIQUE ' : '';
    const indexSql = `CREATE ${uniqueSql}INDEX ${quoteIdentifier(dialect, indexName)} ON ${tableNameSql} (${indexColumns.map((name) => quoteIdentifier(dialect, name)).join(', ')});`;
    statements.push(indexSql);
  }

  return {
    targetLabel: tableNameSql,
    statements
  };
}

/* ── ALTER TABLE / DROP TABLE ───────────────────────────── */

export interface BuildAlterTableSqlParams {
  dialect: DbDialect;
  schemaName?: string;
  tableName: string;
  original: CreateTableDraft;
  current: CreateTableDraft;
}

export interface BuildDropTableSqlParams {
  dialect: DbDialect;
  schemaName?: string;
  tableName: string;
}

function isMySqlDialectSql(dialect: DbDialect): boolean {
  const d = String(dialect).toLowerCase();
  return d.includes('mysql') || d.includes('mariadb');
}

function columnsEqual(a: ColumnDraft, b: ColumnDraft): boolean {
  return (
    a.type.trim() === b.type.trim() &&
    Boolean(a.nullable) === Boolean(b.nullable) &&
    (a.defaultExpression ?? '').trim() === (b.defaultExpression ?? '').trim()
  );
}

function pkColumnsEqual(a: PrimaryKeyDraft | null | undefined, b: PrimaryKeyDraft | null | undefined): boolean {
  const aCols = normalizeIdentifierList(a?.columns).map(c => c.toLowerCase()).sort();
  const bCols = normalizeIdentifierList(b?.columns).map(c => c.toLowerCase()).sort();
  if (aCols.length !== bCols.length) return false;
  return aCols.every((col, i) => col === bCols[i]);
}

export function buildAlterTableSql(params: BuildAlterTableSqlParams): BuildCreateTableSqlResult {
  const { dialect, tableName } = params;
  const effectiveSchemaName = params.schemaName;
  const tableNameSql = buildQualifiedName(dialect, effectiveSchemaName, tableName);
  const isMySQL = isMySqlDialectSql(dialect);

  const statements: string[] = [];

  const originalCols = params.original.columns ?? [];
  const currentCols = params.current.columns ?? [];

  const originalByName = new Map(originalCols.map(c => [c.name.trim().toLowerCase(), c]));
  const currentByName = new Map(currentCols.map(c => [c.name.trim().toLowerCase(), c]));

  // Dropped columns (in original but not current)
  for (const origCol of originalCols) {
    const key = origCol.name.trim().toLowerCase();
    if (!currentByName.has(key)) {
      statements.push(`ALTER TABLE ${tableNameSql} DROP COLUMN ${quoteIdentifier(dialect, origCol.name.trim())};`);
    }
  }

  // Added columns (in current but not original)
  for (const curCol of currentCols) {
    const key = curCol.name.trim().toLowerCase();
    if (key.length === 0 || curCol.type.trim().length === 0) continue;
    if (!originalByName.has(key)) {
      const colType = curCol.type.trim();
      const nullSql = curCol.nullable === false ? ' NOT NULL' : '';
      const defaultExpr = (curCol.defaultExpression ?? '').trim();
      const defaultSql = defaultExpr.length > 0 ? ` DEFAULT ${defaultExpr}` : '';
      statements.push(`ALTER TABLE ${tableNameSql} ADD COLUMN ${quoteIdentifier(dialect, curCol.name.trim())} ${colType}${defaultSql}${nullSql};`);
    }
  }

  // Modified columns (exist in both, but changed)
  for (const curCol of currentCols) {
    const key = curCol.name.trim().toLowerCase();
    if (key.length === 0 || curCol.type.trim().length === 0) continue;
    const origCol = originalByName.get(key);
    if (!origCol) continue;
    if (columnsEqual(origCol, curCol)) continue;

    if (isMySQL) {
      // MySQL uses MODIFY COLUMN with full definition
      const colType = curCol.type.trim();
      const nullSql = curCol.nullable === false ? ' NOT NULL' : ' NULL';
      const defaultExpr = (curCol.defaultExpression ?? '').trim();
      const defaultSql = defaultExpr.length > 0 ? ` DEFAULT ${defaultExpr}` : '';
      statements.push(`ALTER TABLE ${tableNameSql} MODIFY COLUMN ${quoteIdentifier(dialect, curCol.name.trim())} ${colType}${defaultSql}${nullSql};`);
    } else {
      // Postgres / DuckDB / others: separate ALTER COLUMN statements
      const colName = quoteIdentifier(dialect, curCol.name.trim());

      if (origCol.type.trim() !== curCol.type.trim()) {
        statements.push(`ALTER TABLE ${tableNameSql} ALTER COLUMN ${colName} TYPE ${curCol.type.trim()};`);
      }

      if (Boolean(origCol.nullable) !== Boolean(curCol.nullable)) {
        if (curCol.nullable === false) {
          statements.push(`ALTER TABLE ${tableNameSql} ALTER COLUMN ${colName} SET NOT NULL;`);
        } else {
          statements.push(`ALTER TABLE ${tableNameSql} ALTER COLUMN ${colName} DROP NOT NULL;`);
        }
      }

      const origDefault = (origCol.defaultExpression ?? '').trim();
      const curDefault = (curCol.defaultExpression ?? '').trim();
      if (origDefault !== curDefault) {
        if (curDefault.length > 0) {
          statements.push(`ALTER TABLE ${tableNameSql} ALTER COLUMN ${colName} SET DEFAULT ${curDefault};`);
        } else {
          statements.push(`ALTER TABLE ${tableNameSql} ALTER COLUMN ${colName} DROP DEFAULT;`);
        }
      }
    }
  }

  // Primary key changes
  const origPK = params.original.primaryKey;
  const curPK = params.current.primaryKey;
  if (!pkColumnsEqual(origPK, curPK)) {
    const origPKCols = normalizeIdentifierList(origPK?.columns);
    const curPKCols = normalizeIdentifierList(curPK?.columns);

    if (origPKCols.length > 0) {
      if (origPK?.name) {
        statements.push(`ALTER TABLE ${tableNameSql} DROP CONSTRAINT ${quoteIdentifier(dialect, origPK.name)};`);
      } else if (isMySQL) {
        statements.push(`ALTER TABLE ${tableNameSql} DROP PRIMARY KEY;`);
      } else {
        statements.push(`ALTER TABLE ${tableNameSql} DROP CONSTRAINT IF EXISTS ${quoteIdentifier(dialect, `${tableName}_pkey`)};`);
      }
    }

    if (curPKCols.length > 0) {
      const pkColsSql = curPKCols.map(n => quoteIdentifier(dialect, n)).join(', ');
      statements.push(`ALTER TABLE ${tableNameSql} ADD PRIMARY KEY (${pkColsSql});`);
    }
  }

  // Unique constraint changes
  const origUniques = params.original.uniques ?? [];
  const curUniques = params.current.uniques ?? [];

  for (const orig of origUniques) {
    const origCols = normalizeIdentifierList(orig.columns).map(c => c.toLowerCase()).sort().join(',');
    const stillExists = curUniques.some(cur => {
      const curCols = normalizeIdentifierList(cur.columns).map(c => c.toLowerCase()).sort().join(',');
      return curCols === origCols;
    });
    if (!stillExists && orig.name) {
      statements.push(`ALTER TABLE ${tableNameSql} DROP CONSTRAINT ${quoteIdentifier(dialect, orig.name)};`);
    }
  }

  for (const cur of curUniques) {
    const curCols = normalizeIdentifierList(cur.columns);
    if (curCols.length === 0) continue;
    const curKey = curCols.map(c => c.toLowerCase()).sort().join(',');
    const alreadyExists = origUniques.some(orig => {
      const origKey = normalizeIdentifierList(orig.columns).map(c => c.toLowerCase()).sort().join(',');
      return origKey === curKey;
    });
    if (!alreadyExists) {
      const colsSql = curCols.map(n => quoteIdentifier(dialect, n)).join(', ');
      statements.push(`ALTER TABLE ${tableNameSql} ADD ${quoteConstraintName(dialect, cur.name)}UNIQUE (${colsSql});`);
    }
  }

  // Foreign key changes
  const origFKs = params.original.foreignKeys ?? [];
  const curFKs = params.current.foreignKeys ?? [];

  for (const orig of origFKs) {
    if (!orig.name) continue;
    const stillExists = curFKs.some(cur => cur.name === orig.name);
    if (!stillExists) {
      statements.push(`ALTER TABLE ${tableNameSql} DROP CONSTRAINT ${quoteIdentifier(dialect, orig.name)};`);
    }
  }

  for (const fk of curFKs) {
    const localColumns = normalizeIdentifierList(fk.columns);
    const referencedColumns = normalizeIdentifierList(fk.referencedColumns);
    const referencedTable = (fk.referencedTable ?? '').trim();
    if (localColumns.length === 0 || referencedColumns.length === 0 || referencedTable.length === 0) continue;

    const alreadyExists = origFKs.some(orig => orig.name && orig.name === fk.name);
    if (!alreadyExists) {
      const referencedSchema = (fk.referencedSchema ?? '').trim() || effectiveSchemaName;
      const referencedTableName = buildQualifiedName(dialect, referencedSchema, referencedTable);
      const onUpdate = fk.onUpdate?.trim().toUpperCase();
      const onDelete = fk.onDelete?.trim().toUpperCase();
      const onUpdateSql = onUpdate ? ` ON UPDATE ${onUpdate}` : '';
      const onDeleteSql = onDelete ? ` ON DELETE ${onDelete}` : '';

      statements.push(
        `ALTER TABLE ${tableNameSql} ADD ${quoteConstraintName(dialect, fk.name)}FOREIGN KEY (${localColumns.map(n => quoteIdentifier(dialect, n)).join(', ')}) REFERENCES ${referencedTableName} (${referencedColumns.map(n => quoteIdentifier(dialect, n)).join(', ')})${onUpdateSql}${onDeleteSql};`
      );
    }
  }

  // Check constraint changes
  const origChecks = params.original.checks ?? [];
  const curChecks = params.current.checks ?? [];

  for (const orig of origChecks) {
    if (!orig.name) continue;
    const stillExists = curChecks.some(cur => cur.name === orig.name);
    if (!stillExists) {
      statements.push(`ALTER TABLE ${tableNameSql} DROP CONSTRAINT ${quoteIdentifier(dialect, orig.name)};`);
    }
  }

  for (const cur of curChecks) {
    const expression = (cur.expression ?? '').trim();
    if (expression.length === 0) continue;
    const alreadyExists = origChecks.some(orig => orig.name && orig.name === cur.name);
    if (!alreadyExists) {
      statements.push(`ALTER TABLE ${tableNameSql} ADD ${quoteConstraintName(dialect, cur.name)}CHECK (${expression});`);
    }
  }

  // Index changes
  const origIndexes = params.original.indexes ?? [];
  const curIndexes = params.current.indexes ?? [];

  for (const orig of origIndexes) {
    if (!orig.name) continue;
    const stillExists = curIndexes.some(cur => cur.name === orig.name);
    if (!stillExists) {
      statements.push(`DROP INDEX ${quoteIdentifier(dialect, orig.name)};`);
    }
  }

  for (const [index, idx] of curIndexes.entries()) {
    const indexColumns = normalizeIdentifierList(idx.columns);
    if (indexColumns.length === 0) continue;
    const alreadyExists = origIndexes.some(orig => orig.name && orig.name === idx.name);
    if (!alreadyExists) {
      const fallbackName = sanitizeIdentifierName(`idx_${tableName}_${indexColumns.join('_')}_${index + 1}`);
      const indexName = (idx.name ?? '').trim() || fallbackName;
      const uniqueSql = idx.unique ? 'UNIQUE ' : '';
      statements.push(`CREATE ${uniqueSql}INDEX ${quoteIdentifier(dialect, indexName)} ON ${tableNameSql} (${indexColumns.map(n => quoteIdentifier(dialect, n)).join(', ')});`);
    }
  }

  if (statements.length === 0) {
    throw new Error('No changes detected.');
  }

  return {
    targetLabel: tableNameSql,
    statements
  };
}

export function buildDropTableSql(params: BuildDropTableSqlParams): BuildCreateTableSqlResult {
  const { dialect, schemaName, tableName } = params;
  const tableNameSql = buildQualifiedName(dialect, schemaName, tableName);
  return {
    targetLabel: tableNameSql,
    statements: [`DROP TABLE ${tableNameSql};`]
  };
}
