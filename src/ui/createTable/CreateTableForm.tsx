import React, { useEffect, useMemo, useRef, useState } from 'react';
import type {
  CheckConstraintDraft,
  CreateTableDraft,
  ForeignKeyDraft,
  IndexDraft,
  UniqueConstraintDraft
} from '../../core/createTableSql';

/* ── SQL syntax highlighting ────────────────────── */

const SQL_KEYWORDS = new Set([
  'CREATE', 'TABLE', 'ALTER', 'ADD', 'DROP', 'PRIMARY', 'KEY', 'FOREIGN',
  'REFERENCES', 'UNIQUE', 'INDEX', 'CHECK', 'CONSTRAINT', 'DEFAULT',
  'NOT', 'NULL', 'CASCADE', 'RESTRICT', 'SET', 'NO', 'ACTION',
  'ON', 'DELETE', 'UPDATE', 'INSERT', 'INTO', 'VALUES', 'IF', 'EXISTS',
  'AND', 'OR', 'IN', 'LIKE', 'IS', 'TRUE', 'FALSE', 'SCHEMA',
  'GENERATED', 'BY', 'AS', 'IDENTITY', 'AUTO_INCREMENT', 'COLLATE',
  'ENGINE', 'COMMENT', 'UNSIGNED', 'ZEROFILL', 'BINARY', 'VIRTUAL', 'STORED'
]);

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function highlightSqlNonString(segment: string): string {
  const tokenPattern = /\b[A-Z_]+\b|\b-?\d+(?:\.\d+)?\b|[=<>!(),;]+/gi;
  let html = '';
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = tokenPattern.exec(segment)) !== null) {
    html += escapeHtml(segment.slice(lastIndex, match.index));
    const token = match[0];
    const upper = token.toUpperCase();
    let className = '';

    if (/^-?\d/.test(token)) {
      className = 'sql-token-number';
    } else if (SQL_KEYWORDS.has(upper)) {
      className = 'sql-token-keyword';
    } else if (/^[=<>!(),;]+$/.test(token)) {
      className = 'sql-token-operator';
    }

    if (className) {
      html += `<span class="${className}">${escapeHtml(token)}</span>`;
    } else {
      html += escapeHtml(token);
    }
    lastIndex = match.index + token.length;
  }

  html += escapeHtml(segment.slice(lastIndex));
  return html;
}

function highlightSql(sql: string): string {
  const stringPattern = /'(?:''|[^'])*'/g;
  let result = '';
  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = stringPattern.exec(sql)) !== null) {
    result += highlightSqlNonString(sql.slice(lastIndex, match.index));
    result += `<span class="sql-token-string">${escapeHtml(match[0])}</span>`;
    lastIndex = match.index + match[0].length;
  }

  result += highlightSqlNonString(sql.slice(lastIndex));
  return result;
}

type StatusType = 'info' | 'error' | 'success' | 'warning';

type IndexKind = 'NONE' | 'PRIMARY' | 'UNIQUE' | 'INDEX';
type ConstraintAddType = 'unique' | 'index' | 'foreignKey' | 'check';

interface CreateTableContext {
  connectionId: string;
  connectionName: string;
  schemaName: string;
  dialect: string;
  isLocalDuckDB?: boolean;
}

interface PreviewPayload {
  connectionName: string;
  targetLabel: string;
  statements: string[];
}

interface ResultPayload {
  ok: boolean;
  message: string;
}

interface ColumnRow {
  id: string;
  name: string;
  type: string;
  lengthValues: string;
  defaultExpression: string;
  collation: string;
  attributes: string;
  nullable: boolean;
  indexKind: IndexKind;
  autoIncrement: boolean;
  comments: string;
  virtuality: string;
}

interface UniqueRow {
  name: string;
  columns: string;
}

interface ForeignKeyRow {
  name: string;
  columns: string;
  referencedSchema: string;
  referencedTable: string;
  referencedColumns: string;
  onUpdate: string;
  onDelete: string;
}

interface CheckRow {
  name: string;
  expression: string;
}

interface IndexRow {
  name: string;
  columns: string;
  unique: boolean;
}

interface DraftPreview {
  draft: CreateTableDraft;
  payload: PreviewPayload;
}

interface RowValidation {
  name?: string;
  type?: string;
  lengthValues?: string;
  duplicate?: string;
  autoIncrement?: string;
  indexKind?: string;
}

interface ValidationState {
  tableName?: string;
  general: string[];
  warnings: string[];
  rows: Record<string, RowValidation>;
  foreignKeys: Record<number, string>;
}

const ACTION_OPTIONS = ['', 'NO ACTION', 'RESTRICT', 'CASCADE', 'SET NULL', 'SET DEFAULT'];
const INDEX_OPTIONS: Array<{ value: IndexKind; label: string }> = [
  { value: 'NONE', label: '---' },
  { value: 'PRIMARY', label: 'PRIMARY' },
  { value: 'UNIQUE', label: 'UNIQUE' },
  { value: 'INDEX', label: 'INDEX' }
];
const MYSQL_ATTRIBUTES = ['', 'UNSIGNED', 'UNSIGNED ZEROFILL', 'BINARY'];
const MYSQL_VIRTUALITY = ['', 'VIRTUAL', 'STORED'];
const MYSQL_COLLATIONS = ['', 'utf8mb4_general_ci', 'utf8mb4_unicode_ci', 'latin1_swedish_ci'];
const MYSQL_STORAGE_ENGINES = ['InnoDB', 'MyISAM', 'MEMORY', 'Aria'];

const TYPE_OPTIONS_BY_DIALECT: Record<string, string[]> = {
  postgres: ['BIGINT', 'INTEGER', 'NUMERIC', 'BOOLEAN', 'TEXT', 'VARCHAR', 'TIMESTAMP', 'TIMESTAMPTZ', 'JSONB', 'UUID'],
  mysql: ['BIGINT', 'INT', 'DECIMAL', 'BOOLEAN', 'TEXT', 'VARCHAR', 'TIMESTAMP', 'DATETIME', 'JSON', 'BLOB'],
  duckdb: ['BIGINT', 'INTEGER', 'DOUBLE', 'BOOLEAN', 'VARCHAR', 'TIMESTAMP', 'DATE', 'JSON', 'BLOB'],
  snowflake: ['NUMBER', 'BOOLEAN', 'VARCHAR', 'TEXT', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'DATE', 'VARIANT', 'BINARY'],
  default: ['BIGINT', 'INTEGER', 'NUMERIC', 'BOOLEAN', 'VARCHAR', 'TEXT', 'TIMESTAMP', 'DATE']
};

let rowCounter = 0;

function parseIdentifierList(csv: string): string[] {
  return csv
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

function nextRowId(): string {
  rowCounter += 1;
  return `column-row-${rowCounter}`;
}

function createColumnRow(patch?: Partial<ColumnRow>): ColumnRow {
  return {
    id: patch?.id || nextRowId(),
    name: patch?.name || '',
    type: patch?.type || '',
    lengthValues: patch?.lengthValues || '',
    defaultExpression: patch?.defaultExpression || '',
    collation: patch?.collation || '',
    attributes: patch?.attributes || '',
    nullable: patch?.nullable ?? true,
    indexKind: patch?.indexKind || 'NONE',
    autoIncrement: patch?.autoIncrement ?? false,
    comments: patch?.comments || '',
    virtuality: patch?.virtuality || ''
  };
}

function buildInitialColumns(): ColumnRow[] {
  return [
    createColumnRow({
      name: 'id',
      type: 'BIGINT',
      nullable: false,
      indexKind: 'PRIMARY'
    })
  ];
}

function emptyValidation(): ValidationState {
  return {
    general: [],
    warnings: [],
    rows: {},
    foreignKeys: {}
  };
}

function rowHasValues(row: ColumnRow): boolean {
  return (
    row.name.trim().length > 0 ||
    row.type.trim().length > 0 ||
    row.lengthValues.trim().length > 0 ||
    row.defaultExpression.trim().length > 0 ||
    row.collation.trim().length > 0 ||
    row.attributes.trim().length > 0 ||
    row.indexKind !== 'NONE' ||
    row.autoIncrement ||
    row.comments.trim().length > 0 ||
    row.virtuality.trim().length > 0 ||
    row.nullable === false
  );
}

function toDialectKey(value: string | undefined): string {
  return (value || '').trim().toLowerCase();
}

function isMySqlDialect(value: string | undefined): boolean {
  const dialect = toDialectKey(value);
  return dialect.includes('mysql') || dialect.includes('mariadb');
}

function isPostgresDialect(value: string | undefined): boolean {
  const dialect = toDialectKey(value);
  return dialect === 'postgres' || dialect === 'postgresql';
}

function parseAddCount(raw: number): number {
  if (!Number.isFinite(raw)) return 1;
  return Math.max(1, Math.min(50, Math.floor(raw)));
}

/* ── Edit mode helpers ──────────────────────────── */

interface EditModeData {
  tableName: string;
  columns: Array<{ name: string; type: string; nullable?: boolean; comment?: string }>;
  primaryKey?: string[];
  foreignKeys?: Array<{ name?: string; column: string; foreignSchema: string; foreignTable: string; foreignColumn: string }>;
  indexes?: Array<{ name: string; columns: string[]; unique?: boolean }>;
}

function parseTypeString(fullType: string): { baseType: string; lengthValues: string } {
  const match = fullType.match(/^([^(]+)\((.+)\)$/);
  if (match) {
    return { baseType: match[1].trim(), lengthValues: match[2].trim() };
  }
  return { baseType: fullType.trim(), lengthValues: '' };
}

function buildColumnsFromEditData(
  data: EditModeData
): ColumnRow[] {
  const primarySet = new Set((data.primaryKey ?? []).map(c => c.toLowerCase()));

  return data.columns.map((col) => {
    const parsed = parseTypeString(col.type);
    const isPrimary = primarySet.has(col.name.toLowerCase());

    return createColumnRow({
      name: col.name,
      type: parsed.baseType,
      lengthValues: parsed.lengthValues,
      nullable: col.nullable ?? !isPrimary,
      indexKind: isPrimary ? 'PRIMARY' : 'NONE',
      comments: col.comment ?? ''
    });
  });
}

function buildForeignKeysFromEditData(
  data: EditModeData
): ForeignKeyRow[] {
  if (!data.foreignKeys || data.foreignKeys.length === 0) return [];

  // Group by constraint name (foreign keys can span multiple columns)
  const grouped = new Map<string, typeof data.foreignKeys>();
  for (const fk of data.foreignKeys) {
    const key = fk.name || `__unnamed_${fk.column}_${fk.foreignTable}`;
    const existing = grouped.get(key) || [];
    existing.push(fk);
    grouped.set(key, existing);
  }

  return Array.from(grouped.values()).map((fks) => ({
    name: fks[0].name || '',
    columns: fks.map(fk => fk.column).join(', '),
    referencedSchema: fks[0].foreignSchema || '',
    referencedTable: fks[0].foreignTable || '',
    referencedColumns: fks.map(fk => fk.foreignColumn).join(', '),
    onUpdate: '',
    onDelete: ''
  }));
}

function buildIndexesFromEditData(
  data: EditModeData
): IndexRow[] {
  if (!data.indexes || data.indexes.length === 0) return [];
  return data.indexes.map((idx) => ({
    name: idx.name || '',
    columns: idx.columns.join(', '),
    unique: idx.unique ?? false
  }));
}

function hasFatalIssues(validation: ValidationState): boolean {
  if (validation.tableName) {
    return true;
  }
  if (validation.general.length > 0) {
    return true;
  }
  if (Object.keys(validation.foreignKeys).length > 0) {
    return true;
  }
  return Object.values(validation.rows).some((row) =>
    Boolean(row.name || row.type || row.lengthValues || row.duplicate || row.autoIncrement || row.indexKind)
  );
}

function countFatalIssues(validation: ValidationState): number {
  let count = 0;
  if (validation.tableName) count += 1;
  count += validation.general.length;
  count += Object.keys(validation.foreignKeys).length;
  for (const row of Object.values(validation.rows)) {
    if (row.name) count += 1;
    if (row.type) count += 1;
    if (row.lengthValues) count += 1;
    if (row.duplicate) count += 1;
    if (row.autoIncrement) count += 1;
    if (row.indexKind) count += 1;
  }
  return count;
}

function countWarnings(validation: ValidationState): number {
  return validation.warnings.length;
}

function composeTypeExpression(
  row: ColumnRow,
  options: {
    isMySql: boolean;
    isPostgres: boolean;
    supportsCollation: boolean;
    supportsAutoIncrement: boolean;
  }
): string {
  const baseType = row.type.trim();
  if (!baseType) {
    return '';
  }

  const lengthValues = row.lengthValues.trim();
  let expression = baseType;

  if (lengthValues && !baseType.includes('(')) {
    expression = `${expression}(${lengthValues})`;
  }

  if (options.isMySql && row.attributes.trim().length > 0) {
    expression = `${expression} ${row.attributes.trim()}`;
  }

  if (options.supportsCollation && row.collation.trim().length > 0) {
    expression = `${expression} COLLATE ${row.collation.trim()}`;
  }

  if (row.autoIncrement && options.supportsAutoIncrement) {
    if (options.isMySql) {
      expression = `${expression} AUTO_INCREMENT`;
    } else if (options.isPostgres) {
      expression = `${expression} GENERATED BY DEFAULT AS IDENTITY`;
    }
  }

  return expression;
}

export const CreateTableForm: React.FC<{ vscode: any }> = ({ vscode }) => {
  const [context, setContext] = useState<CreateTableContext | null>(null);
  const [tableName, setTableName] = useState('');
  const [addCount, setAddCount] = useState(1);
  const [columns, setColumns] = useState<ColumnRow[]>(buildInitialColumns);

  const [tableComments, setTableComments] = useState('');
  const [tableCollation, setTableCollation] = useState('');
  const [storageEngine, setStorageEngine] = useState('InnoDB');
  const [constraintAddType, setConstraintAddType] = useState<ConstraintAddType>('unique');

  const [uniques, setUniques] = useState<UniqueRow[]>([]);
  const [foreignKeys, setForeignKeys] = useState<ForeignKeyRow[]>([]);
  const [checks, setChecks] = useState<CheckRow[]>([]);
  const [indexes, setIndexes] = useState<IndexRow[]>([]);

  const [status, setStatus] = useState<{ type: StatusType; text: string } | null>(null);
  const [preview, setPreview] = useState<DraftPreview | null>(null);
  const [executing, setExecuting] = useState(false);
  const [showValidation, setShowValidation] = useState(false);

  const [isEditMode, setIsEditMode] = useState(false);
  const [showDropConfirm, setShowDropConfirm] = useState(false);
  const originalDraftRef = useRef<CreateTableDraft | null>(null);

  const firstColumnInputRef = useRef<HTMLInputElement | null>(null);
  const modalRef = useRef<HTMLDivElement | null>(null);

  const dialectKey = toDialectKey(context?.dialect);
  const isMySql = isMySqlDialect(context?.dialect);
  const isPostgres = isPostgresDialect(context?.dialect);
  const supportsCollation = isMySql;
  const supportsAttributes = isMySql;
  const supportsVirtuality = isMySql;
  const supportsStorageEngine = isMySql;
  const supportsAutoIncrement = isMySql || isPostgres;

  const dialectTypeOptions = useMemo(() => {
    return TYPE_OPTIONS_BY_DIALECT[dialectKey] || TYPE_OPTIONS_BY_DIALECT.default;
  }, [dialectKey]);

  useEffect(() => {
    if (supportsStorageEngine && !storageEngine) {
      setStorageEngine('InnoDB');
    }
    if (!supportsCollation) {
      setTableCollation('');
    }
  }, [storageEngine, supportsStorageEngine, supportsCollation]);

  useEffect(() => {
    const handleMessage = (event: MessageEvent) => {
      const message = event.data;
      switch (message.command) {
        case 'setContext': {
          const ctx = message.data as CreateTableContext & { editMode?: EditModeData };
          setContext(ctx);

          if (ctx.editMode) {
            setIsEditMode(true);
            setTableName(ctx.editMode.tableName);

            const editColumns = buildColumnsFromEditData(ctx.editMode);
            setColumns(editColumns.length > 0 ? editColumns : buildInitialColumns());

            setForeignKeys(buildForeignKeysFromEditData(ctx.editMode));
            setIndexes(buildIndexesFromEditData(ctx.editMode));

            // Defer building the original draft so state is settled
            setTimeout(() => {
              const pkColumns = (ctx.editMode!.primaryKey ?? []);
              const origColumns = (ctx.editMode!.columns ?? []).filter(c => c.name.trim().length > 0).map((col) => {
                const parsed = parseTypeString(col.type);
                const fullType = parsed.lengthValues ? `${parsed.baseType}(${parsed.lengthValues})` : parsed.baseType;
                return {
                  name: col.name.trim(),
                  type: fullType,
                  nullable: col.nullable ?? !pkColumns.map(c => c.toLowerCase()).includes(col.name.toLowerCase()),
                  defaultExpression: ''
                };
              });

              const origFKs = buildForeignKeysFromEditData(ctx.editMode!).map(fk => ({
                name: fk.name || undefined,
                columns: fk.columns.split(',').map(c => c.trim()).filter(c => c.length > 0),
                referencedSchema: fk.referencedSchema || undefined,
                referencedTable: fk.referencedTable,
                referencedColumns: fk.referencedColumns.split(',').map(c => c.trim()).filter(c => c.length > 0)
              }));

              const origIndexes = buildIndexesFromEditData(ctx.editMode!).map(idx => ({
                name: idx.name || undefined,
                columns: idx.columns.split(',').map(c => c.trim()).filter(c => c.length > 0),
                unique: idx.unique
              }));

              originalDraftRef.current = {
                tableName: ctx.editMode!.tableName,
                schemaName: ctx.schemaName,
                columns: origColumns,
                primaryKey: pkColumns.length > 0 ? { columns: pkColumns } : null,
                uniques: [],
                foreignKeys: origFKs,
                checks: [],
                indexes: origIndexes
              };
            }, 0);
          }
          return;
        }
        case 'createTablePreview':
          if (message.data) {
            setPreview((current) => (current ? { ...current, payload: message.data as PreviewPayload } : current));
            setStatus(null);
          }
          return;
        case 'createTableResult': {
          const result = message.data as ResultPayload;
          setExecuting(false);
          if (!result) return;
          setStatus({ type: result.ok ? 'success' : 'error', text: result.message });
          if (result.ok) {
            setPreview(null);
          }
          return;
        }
      }
    };

    window.addEventListener('message', handleMessage);
    vscode.postMessage({ command: 'ready' });
    return () => window.removeEventListener('message', handleMessage);
  }, [vscode]);

  useEffect(() => {
    const previewOpen = Boolean(preview?.payload.statements.length);
    if (!previewOpen) {
      return;
    }

    const previous = document.activeElement as HTMLElement | null;
    const modal = modalRef.current;
    const selector = [
      'button:not([disabled])',
      'input:not([disabled])',
      'select:not([disabled])',
      'textarea:not([disabled])',
      '[tabindex]:not([tabindex="-1"])'
    ].join(',');

    const getFocusable = () => {
      if (!modal) return [] as HTMLElement[];
      return Array.from(modal.querySelectorAll<HTMLElement>(selector));
    };

    const focusable = getFocusable();
    if (focusable.length > 0) {
      focusable[0].focus();
    }

    const onKeyDown = (event: KeyboardEvent) => {
      if (!modal) {
        return;
      }

      if (event.key === 'Escape') {
        if (!executing) {
          event.preventDefault();
          setPreview(null);
        }
        return;
      }

      if (event.key !== 'Tab') {
        return;
      }

      const elements = getFocusable();
      if (elements.length === 0) {
        return;
      }

      const active = document.activeElement as HTMLElement | null;
      const currentIndex = active ? elements.indexOf(active) : -1;
      const first = elements[0];
      const last = elements[elements.length - 1];

      if (event.shiftKey) {
        if (!active || currentIndex <= 0) {
          event.preventDefault();
          last.focus();
        }
        return;
      }

      if (!active || currentIndex === -1 || active === last) {
        event.preventDefault();
        first.focus();
      }
    };

    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('keydown', onKeyDown);
      previous?.focus();
    };
  }, [preview?.payload.statements.length, executing]);

  const knownColumnNames = useMemo(
    () => columns.map((column) => column.name.trim()).filter((name) => name.length > 0),
    [columns]
  );

  const validation = useMemo<ValidationState>(() => {
    const nextValidation = emptyValidation();

    if (!tableName.trim()) {
      nextValidation.tableName = 'Table name is required.';
    }

    let completeRows = 0;
    const columnNameMap = new Map<string, string[]>();
    let autoIncrementCount = 0;

    for (const row of columns) {
      if (!rowHasValues(row)) {
        continue;
      }

      const rowErrors: RowValidation = {};
      const name = row.name.trim();
      const type = row.type.trim();
      const lengthValues = row.lengthValues.trim();

      if (!name) {
        rowErrors.name = 'Name is required.';
      }
      if (!type) {
        rowErrors.type = 'Type is required.';
      }

      if (type && lengthValues) {
        const upperType = type.toUpperCase();
        const isEnumLike = upperType === 'ENUM' || upperType === 'SET';
        const hasTypeParams = type.includes('(');
        if (!hasTypeParams && !isEnumLike && !/^\d+(\s*,\s*\d+)?$/.test(lengthValues)) {
          rowErrors.lengthValues = 'Use numeric length like 255 or precision like 10,2.';
        }
      }

      if (name && type) {
        completeRows += 1;
        const normalized = name.toLowerCase();
        const ids = columnNameMap.get(normalized) || [];
        ids.push(row.id);
        columnNameMap.set(normalized, ids);
      }

      if (row.autoIncrement) {
        autoIncrementCount += 1;
        if (!supportsAutoIncrement) {
          nextValidation.warnings.push(`Auto increment is not supported for ${context?.dialect || 'this dialect'} and will be ignored.`);
        } else if (row.indexKind === 'NONE') {
          rowErrors.indexKind = 'Auto increment columns must be indexed (PRIMARY, UNIQUE, or INDEX).';
        }
      }

      if (Object.keys(rowErrors).length > 0) {
        nextValidation.rows[row.id] = rowErrors;
      }
    }

    if (completeRows === 0) {
      nextValidation.general.push('At least one row with both Name and Type is required.');
    }

    for (const [, rowIds] of columnNameMap) {
      if (rowIds.length > 1) {
        for (const id of rowIds) {
          const rowError = nextValidation.rows[id] || {};
          rowError.duplicate = 'Duplicate column name.';
          nextValidation.rows[id] = rowError;
        }
      }
    }

    if (supportsAutoIncrement && autoIncrementCount > 1) {
      nextValidation.general.push('Only one auto increment column is allowed for this dialect.');
    }

    const normalizedColumnNames = new Set(
      columns
        .filter((row) => row.name.trim().length > 0)
        .map((row) => row.name.trim().toLowerCase())
    );

    foreignKeys.forEach((row, index) => {
      const localColumns = parseIdentifierList(row.columns);
      const referencedColumns = parseIdentifierList(row.referencedColumns);
      const referencedTable = row.referencedTable.trim();
      const touched =
        localColumns.length > 0 ||
        referencedColumns.length > 0 ||
        row.referencedSchema.trim().length > 0 ||
        referencedTable.length > 0;

      if (!touched) {
        return;
      }

      const rowIssues: string[] = [];
      if (localColumns.length === 0) rowIssues.push('Local columns required.');
      if (referencedColumns.length === 0) rowIssues.push('Referenced columns required.');
      if (referencedTable.length === 0) rowIssues.push('Referenced table required.');
      if (localColumns.length !== referencedColumns.length) {
        rowIssues.push('Local and referenced column counts must match.');
      }

      const unknownLocal = localColumns.filter((column) => !normalizedColumnNames.has(column.toLowerCase()));
      if (unknownLocal.length > 0) {
        rowIssues.push(`Unknown local columns: ${unknownLocal.join(', ')}`);
      }

      if (rowIssues.length > 0) {
        nextValidation.foreignKeys[index] = rowIssues.join(' ');
      }
    });

    for (const unique of uniques) {
      const uniqueColumns = parseIdentifierList(unique.columns);
      if (uniqueColumns.length === 0) {
        continue;
      }
      const unknown = uniqueColumns.filter((column) => !normalizedColumnNames.has(column.toLowerCase()));
      if (unknown.length > 0) {
        nextValidation.general.push(`Unique constraint references unknown columns: ${unknown.join(', ')}.`);
      }
    }

    for (const indexRow of indexes) {
      const indexColumns = parseIdentifierList(indexRow.columns);
      if (indexColumns.length === 0) {
        continue;
      }
      const unknown = indexColumns.filter((column) => !normalizedColumnNames.has(column.toLowerCase()));
      if (unknown.length > 0) {
        nextValidation.general.push(`Index references unknown columns: ${unknown.join(', ')}.`);
      }
    }

    return nextValidation;
  }, [
    tableName,
    columns,
    supportsAutoIncrement,
    context?.dialect,
    foreignKeys,
    uniques,
    indexes
  ]);

  const issueCount = countFatalIssues(validation);
  const warningCount = countWarnings(validation);
  const previewDisabled = executing;

  const buildDraft = (): CreateTableDraft => {
    const draftColumns = columns
      .filter((row) => row.name.trim().length > 0 && row.type.trim().length > 0)
      .map((row) => ({
        name: row.name.trim(),
        type: composeTypeExpression(row, {
          isMySql,
          isPostgres,
          supportsCollation,
          supportsAutoIncrement
        }),
        nullable: row.nullable,
        defaultExpression: row.defaultExpression.trim()
      }));

    const primaryColumns = columns
      .filter((row) => row.indexKind === 'PRIMARY' && row.name.trim().length > 0)
      .map((row) => row.name.trim());

    const rowUniques: UniqueConstraintDraft[] = columns
      .filter((row) => row.indexKind === 'UNIQUE' && row.name.trim().length > 0)
      .map((row) => ({ columns: [row.name.trim()] }));

    const rowIndexes: IndexDraft[] = columns
      .filter((row) => row.indexKind === 'INDEX' && row.name.trim().length > 0)
      .map((row) => ({ columns: [row.name.trim()], unique: false }));

    const advancedUniques: UniqueConstraintDraft[] = uniques
      .map((row) => ({
        name: row.name.trim() || undefined,
        columns: parseIdentifierList(row.columns)
      }))
      .filter((row) => row.columns.length > 0);

    const nextForeignKeys: ForeignKeyDraft[] = foreignKeys
      .map((row) => ({
        name: row.name.trim() || undefined,
        columns: parseIdentifierList(row.columns),
        referencedSchema: row.referencedSchema.trim() || undefined,
        referencedTable: row.referencedTable.trim(),
        referencedColumns: parseIdentifierList(row.referencedColumns),
        onUpdate: (row.onUpdate || undefined) as ForeignKeyDraft['onUpdate'],
        onDelete: (row.onDelete || undefined) as ForeignKeyDraft['onDelete']
      }))
      .filter((row) => row.columns.length > 0 || row.referencedColumns.length > 0 || row.referencedTable.length > 0);

    const nextChecks: CheckConstraintDraft[] = checks
      .map((row) => ({
        name: row.name.trim() || undefined,
        expression: row.expression.trim()
      }))
      .filter((row) => row.expression.length > 0);

    const advancedIndexes: IndexDraft[] = indexes
      .map((row) => ({
        name: row.name.trim() || undefined,
        columns: parseIdentifierList(row.columns),
        unique: row.unique
      }))
      .filter((row) => row.columns.length > 0);

    const draft: CreateTableDraft = {
      tableName: tableName.trim(),
      schemaName: context?.schemaName,
      columns: draftColumns,
      uniques: [...rowUniques, ...advancedUniques],
      foreignKeys: nextForeignKeys,
      checks: nextChecks,
      indexes: [...rowIndexes, ...advancedIndexes]
    };

    if (primaryColumns.length > 0) {
      draft.primaryKey = {
        columns: primaryColumns
      };
    }

    return draft;
  };

  const addRows = (count: number) => {
    const safeCount = parseAddCount(count);
    const rows = Array.from({ length: safeCount }, () => createColumnRow());
    setColumns((prev) => [...prev, ...rows]);
  };

  const updateColumn = (index: number, patch: Partial<ColumnRow>) => {
    setColumns((prev) => prev.map((row, i) => (i === index ? { ...row, ...patch } : row)));
  };

  const removeColumn = (index: number) => {
    setColumns((prev) => {
      if (prev.length <= 1) {
        return prev;
      }
      return prev.filter((_, i) => i !== index);
    });
  };

  const moveColumn = (index: number, delta: number) => {
    setColumns((prev) => {
      const target = index + delta;
      if (target < 0 || target >= prev.length) {
        return prev;
      }
      const next = [...prev];
      const [row] = next.splice(index, 1);
      next.splice(target, 0, row);
      return next;
    });
  };

  const focusFirstGridCell = () => {
    firstColumnInputRef.current?.focus();
  };

  const requestPreview = () => {
    setShowValidation(true);

    if (hasFatalIssues(validation)) {
      setStatus({
        type: 'error',
        text: `Fix ${issueCount} validation issue${issueCount === 1 ? '' : 's'} before generating SQL preview.`
      });
      return;
    }

    const draft = buildDraft();

    if (isEditMode && originalDraftRef.current) {
      setStatus({ type: 'info', text: 'Generating ALTER TABLE preview...' });
      setPreview({
        draft,
        payload: {
          connectionName: context?.connectionName || '',
          targetLabel: '',
          statements: []
        }
      });
      vscode.postMessage({
        command: 'previewAlterTable',
        data: { original: originalDraftRef.current, current: draft }
      });
    } else {
      setStatus({ type: 'info', text: 'Generating SQL preview...' });
      setPreview({
        draft,
        payload: {
          connectionName: context?.connectionName || '',
          targetLabel: '',
          statements: []
        }
      });
      vscode.postMessage({
        command: 'previewCreateTable',
        data: { draft }
      });
    }
  };

  const executeSql = () => {
    if (!preview || executing) return;
    setExecuting(true);

    if (isEditMode && originalDraftRef.current) {
      setStatus({ type: 'info', text: 'Executing ALTER TABLE statements...' });
      vscode.postMessage({
        command: 'executeAlterTable',
        data: { original: originalDraftRef.current, current: preview.draft }
      });
    } else {
      setStatus({ type: 'info', text: 'Executing CREATE TABLE statements...' });
      vscode.postMessage({
        command: 'executeCreateTable',
        data: { draft: preview.draft }
      });
    }
  };

  const requestDropTable = () => {
    setShowDropConfirm(true);
  };

  const executeDropTable = () => {
    setShowDropConfirm(false);
    setExecuting(true);
    setStatus({ type: 'info', text: 'Dropping table...' });
    vscode.postMessage({ command: 'dropTable' });
  };

  const resetForm = () => {
    setTableName('');
    setAddCount(1);
    setColumns(buildInitialColumns());
    setTableComments('');
    setTableCollation('');
    setStorageEngine('InnoDB');
    setConstraintAddType('unique');
    setUniques([]);
    setForeignKeys([]);
    setChecks([]);
    setIndexes([]);
    setStatus(null);
    setPreview(null);
    setExecuting(false);
    setShowValidation(false);
  };

  const addAdvancedConstraint = () => {
    if (constraintAddType === 'unique') {
      setUniques((prev) => [...prev, { name: '', columns: '' }]);
      return;
    }

    if (constraintAddType === 'index') {
      setIndexes((prev) => [...prev, { name: '', columns: '', unique: false }]);
      return;
    }

    if (constraintAddType === 'foreignKey') {
      setForeignKeys((prev) => [
        ...prev,
        {
          name: '',
          columns: '',
          referencedSchema: context?.schemaName || '',
          referencedTable: '',
          referencedColumns: '',
          onUpdate: '',
          onDelete: ''
        }
      ]);
      return;
    }

    setChecks((prev) => [...prev, { name: '', expression: '' }]);
  };

  return (
    <div className="create-table-root">
      <header className="create-table-header">
        <h1 className="header-line">
          {isEditMode ? 'Edit' : 'Create'} table in <code>{context?.connectionName || '...'}</code> <code>{context?.schemaName || '...'}</code> <code>{context?.dialect || '...'}</code>
        </h1>

        <div className="top-table-settings">
          <label className="table-name-setting" htmlFor="table-name-input">
            <span>Table name</span>
            <input
              id="table-name-input"
              className={showValidation && validation.tableName ? 'input-error' : ''}
              value={tableName}
              onChange={(event) => setTableName(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === 'Enter') {
                  event.preventDefault();
                  focusFirstGridCell();
                }
              }}
              placeholder="users"
              aria-label="Table name"
              readOnly={isEditMode}
              style={isEditMode ? { opacity: 0.7 } : undefined}
            />
          </label>

          <label>
            <span>Collation</span>
            <select
              disabled={!supportsCollation}
              value={tableCollation}
              title={supportsCollation ? '' : 'Table collation is not supported for this dialect.'}
              onChange={(event) => setTableCollation(event.target.value)}
              aria-label="Table collation"
            >
              {(supportsCollation ? MYSQL_COLLATIONS : ['']).map((option) => (
                <option key={`table-collation-${option || 'none'}`} value={option}>
                  {option || (supportsCollation ? 'Default' : 'n/a')}
                </option>
              ))}
            </select>
          </label>

          <label>
            <span>Storage Engine</span>
            <select
              disabled={!supportsStorageEngine}
              value={supportsStorageEngine ? storageEngine : ''}
              title={supportsStorageEngine ? '' : 'Storage engine is not supported for this dialect.'}
              onChange={(event) => setStorageEngine(event.target.value)}
              aria-label="Storage engine"
            >
              {(supportsStorageEngine ? MYSQL_STORAGE_ENGINES : ['']).map((option) => (
                <option key={`storage-${option || 'none'}`} value={option}>
                  {option || 'n/a'}
                </option>
              ))}
            </select>
          </label>
        </div>

        <div className="table-description-row">
          <label>
            <span>Table description</span>
            <input
              value={tableComments}
              onChange={(event) => setTableComments(event.target.value)}
              placeholder="optional"
              aria-label="Table description"
            />
          </label>
        </div>

        {showValidation && validation.tableName ? <div className="field-error">{validation.tableName}</div> : null}
      </header>

      <section className="designer-content">
        <div className="columns-controls-row" role="group" aria-label="Columns actions">
          <label className="inline-field add-count-field" htmlFor="add-column-count">
            <span>Add</span>
            <input
              id="add-column-count"
              type="number"
              min={1}
              max={50}
              value={addCount}
              onChange={(event) => setAddCount(parseAddCount(Number(event.target.value)))}
              aria-label="Add column count"
            />
          </label>
          <span className="inline-suffix">column(s)</span>
          <button type="button" onClick={() => addRows(addCount)} aria-label="Add columns">Go</button>
          <button type="button" onClick={() => addRows(1)}>Add row</button>
          <button type="button" onClick={resetForm}>Reset</button>
        </div>

        <h2 className="columns-heading">Columns</h2>
        <div className="type-hint">
          Suggested {context?.dialect || 'SQL'} types: {dialectTypeOptions.slice(0, 8).join(', ')}
        </div>

        <div className="columns-scroller">
          <div className={`designer-grid${isMySql ? '' : ' compact-grid'}`} role="table" aria-label="Table columns designer">
            <div className="designer-grid-header" role="row">
              <span>Name</span>
              <span>Type</span>
              <span>Length/Values</span>
              <span>Default</span>
              {isMySql ? <span>Collation</span> : null}
              {isMySql ? <span>Attributes</span> : null}
              <span>Null</span>
              <span>Index</span>
              <span>A_I</span>
              <span>Comments</span>
              {isMySql ? <span>Virtuality</span> : null}
              <span></span>
            </div>

            {columns.map((row, index) => {
              const rowValidation = validation.rows[row.id] || {};
              const rowErrors = [
                rowValidation.name,
                rowValidation.type,
                rowValidation.lengthValues,
                rowValidation.duplicate,
                rowValidation.autoIncrement,
                rowValidation.indexKind
              ].filter(Boolean) as string[];

              return (
                <React.Fragment key={row.id}>
                  <div className="designer-grid-row" role="row">
                    <input
                      ref={index === 0 ? firstColumnInputRef : undefined}
                      className={showValidation && (rowValidation.name || rowValidation.duplicate) ? 'input-error' : ''}
                      value={row.name}
                      onChange={(event) => updateColumn(index, { name: event.target.value })}
                      placeholder="column_name"
                      aria-label={`Column ${index + 1} name`}
                    />

                    <input
                      className={showValidation && rowValidation.type ? 'input-error' : ''}
                      value={row.type}
                      onChange={(event) => updateColumn(index, { type: event.target.value })}
                      list="designer-type-options"
                      placeholder="INT"
                      aria-label={`Column ${index + 1} type`}
                    />

                    <input
                      className={showValidation && rowValidation.lengthValues ? 'input-error' : ''}
                      value={row.lengthValues}
                      onChange={(event) => updateColumn(index, { lengthValues: event.target.value })}
                      placeholder="255 or 10,2"
                      aria-label={`Column ${index + 1} length or values`}
                    />

                    <input
                      value={row.defaultExpression}
                      onChange={(event) => updateColumn(index, { defaultExpression: event.target.value })}
                      placeholder="None"
                      aria-label={`Column ${index + 1} default expression`}
                    />

                    {isMySql ? (
                      <select
                        value={row.collation}
                        onChange={(event) => updateColumn(index, { collation: event.target.value })}
                        aria-label={`Column ${index + 1} collation`}
                      >
                        {MYSQL_COLLATIONS.map((option) => (
                          <option key={`collation-${row.id}-${option || 'none'}`} value={option}>
                            {option || 'Default'}
                          </option>
                        ))}
                      </select>
                    ) : null}

                    {isMySql ? (
                      <select
                        value={row.attributes}
                        onChange={(event) => updateColumn(index, { attributes: event.target.value })}
                        aria-label={`Column ${index + 1} attributes`}
                      >
                        {MYSQL_ATTRIBUTES.map((option) => (
                          <option key={`attributes-${row.id}-${option || 'none'}`} value={option}>
                            {option || '---'}
                          </option>
                        ))}
                      </select>
                    ) : null}

                    <label className="checkbox-cell" aria-label={`Column ${index + 1} nullable`}>
                      <input
                        type="checkbox"
                        checked={row.nullable}
                        onChange={(event) => updateColumn(index, { nullable: event.target.checked })}
                      />
                    </label>

                    <select
                      className={showValidation && rowValidation.indexKind ? 'input-error' : ''}
                      value={row.indexKind}
                      onChange={(event) => updateColumn(index, { indexKind: event.target.value as IndexKind })}
                      aria-label={`Column ${index + 1} index type`}
                    >
                      {INDEX_OPTIONS.map((option) => (
                        <option key={`index-${row.id}-${option.value}`} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>

                    <label className="checkbox-cell" aria-label={`Column ${index + 1} auto increment`}>
                      <input
                        type="checkbox"
                        checked={row.autoIncrement}
                        onChange={(event) => updateColumn(index, { autoIncrement: event.target.checked })}
                      />
                    </label>

                    <input
                      value={row.comments}
                      onChange={(event) => updateColumn(index, { comments: event.target.value })}
                      placeholder=""
                      aria-label={`Column ${index + 1} comments`}
                    />

                    {isMySql ? (
                      <select
                        value={row.virtuality}
                        onChange={(event) => updateColumn(index, { virtuality: event.target.value })}
                        aria-label={`Column ${index + 1} virtuality`}
                      >
                        {MYSQL_VIRTUALITY.map((option) => (
                          <option key={`virtuality-${row.id}-${option || 'none'}`} value={option}>
                            {option || '---'}
                          </option>
                        ))}
                      </select>
                    ) : null}

                    <div className="move-cell">
                      <button
                        type="button"
                        onClick={() => moveColumn(index, -1)}
                        disabled={index === 0}
                        aria-label={`Move column ${index + 1} up`}
                      >
                        Up
                      </button>
                      <button
                        type="button"
                        onClick={() => moveColumn(index, 1)}
                        disabled={index === columns.length - 1}
                        aria-label={`Move column ${index + 1} down`}
                      >
                        Down
                      </button>
                      <button
                        type="button"
                        className="danger"
                        onClick={() => removeColumn(index)}
                        disabled={columns.length <= 1}
                        aria-label={`Remove column ${index + 1}`}
                      >
                        Del
                      </button>
                    </div>
                  </div>

                  {showValidation && rowErrors.length > 0 ? <div className="row-error">{rowErrors.join(' ')}</div> : null}
                </React.Fragment>
              );
            })}
          </div>
        </div>

        <datalist id="designer-type-options">
          {dialectTypeOptions.map((typeOption) => (
            <option key={typeOption} value={typeOption} />
          ))}
        </datalist>

        <div className="table-options-note">
          SQL preview currently emits core DDL and constraints.
        </div>
      </section>

      <details className="section-card constraint-card">
        <summary>
          <span className="summary-title">Advanced Constraints</span>
          <span className="summary-meta">optional grouped uniques, indexes, foreign keys, checks</span>
        </summary>

        <div className="constraint-toolbar">
          <select
            value={constraintAddType}
            onChange={(event) => setConstraintAddType(event.target.value as ConstraintAddType)}
            aria-label="Constraint type to add"
          >
            <option value="unique">Unique constraint</option>
            <option value="index">Index</option>
            <option value="foreignKey">Foreign key</option>
            <option value="check">Check constraint</option>
          </select>
          <button type="button" onClick={addAdvancedConstraint}>Add</button>
        </div>

        {uniques.map((row, index) => (
          <div key={`unique-${index}`} className="constraint-grid three-col">
            <input
              value={row.name}
              onChange={(event) => setUniques((prev) => prev.map((item, i) => (i === index ? { ...item, name: event.target.value } : item)))}
              placeholder="unique name (optional)"
              aria-label={`Composite unique ${index + 1} name`}
            />
            <input
              value={row.columns}
              onChange={(event) => setUniques((prev) => prev.map((item, i) => (i === index ? { ...item, columns: event.target.value } : item)))}
              placeholder="column_a, column_b"
              aria-label={`Composite unique ${index + 1} columns`}
            />
            <button type="button" className="danger" onClick={() => setUniques((prev) => prev.filter((_, i) => i !== index))}>Delete</button>
          </div>
        ))}

        {indexes.map((row, index) => (
          <div key={`index-${index}`} className="constraint-grid four-col">
            <input
              value={row.name}
              onChange={(event) => setIndexes((prev) => prev.map((item, i) => (i === index ? { ...item, name: event.target.value } : item)))}
              placeholder="index name (optional)"
              aria-label={`Advanced index ${index + 1} name`}
            />
            <input
              value={row.columns}
              onChange={(event) => setIndexes((prev) => prev.map((item, i) => (i === index ? { ...item, columns: event.target.value } : item)))}
              placeholder="column_a, column_b"
              aria-label={`Advanced index ${index + 1} columns`}
            />
            <label className="checkbox-cell">
              <input
                type="checkbox"
                checked={row.unique}
                onChange={(event) => setIndexes((prev) => prev.map((item, i) => (i === index ? { ...item, unique: event.target.checked } : item)))}
                aria-label={`Advanced index ${index + 1} unique`}
              />
              <span>Unique</span>
            </label>
            <button type="button" className="danger" onClick={() => setIndexes((prev) => prev.filter((_, i) => i !== index))}>Delete</button>
          </div>
        ))}

        {foreignKeys.length > 0 ? (
          <div className="columns-scroller">
            <div className="fk-table">
              <div className="fk-header">
                <span>Name</span>
                <span>Local Columns</span>
                <span>Ref Schema</span>
                <span>Ref Table</span>
                <span>Ref Columns</span>
                <span>On Update</span>
                <span>On Delete</span>
                <span className="actions-column">Actions</span>
              </div>
              {foreignKeys.map((row, index) => (
                <React.Fragment key={`fk-${index}`}>
                  <div className="fk-row">
                    <input value={row.name} onChange={(event) => setForeignKeys((prev) => prev.map((item, i) => (i === index ? { ...item, name: event.target.value } : item)))} placeholder="fk_name (optional)" />
                    <input value={row.columns} onChange={(event) => setForeignKeys((prev) => prev.map((item, i) => (i === index ? { ...item, columns: event.target.value } : item)))} placeholder="local_col" />
                    <input value={row.referencedSchema} onChange={(event) => setForeignKeys((prev) => prev.map((item, i) => (i === index ? { ...item, referencedSchema: event.target.value } : item)))} placeholder="schema" />
                    <input value={row.referencedTable} onChange={(event) => setForeignKeys((prev) => prev.map((item, i) => (i === index ? { ...item, referencedTable: event.target.value } : item)))} placeholder="table" />
                    <input value={row.referencedColumns} onChange={(event) => setForeignKeys((prev) => prev.map((item, i) => (i === index ? { ...item, referencedColumns: event.target.value } : item)))} placeholder="id" />
                    <select value={row.onUpdate} onChange={(event) => setForeignKeys((prev) => prev.map((item, i) => (i === index ? { ...item, onUpdate: event.target.value } : item)))}>
                      {ACTION_OPTIONS.map((option) => (
                        <option key={`upd-${index}-${option || 'none'}`} value={option}>
                          {option || 'none'}
                        </option>
                      ))}
                    </select>
                    <select value={row.onDelete} onChange={(event) => setForeignKeys((prev) => prev.map((item, i) => (i === index ? { ...item, onDelete: event.target.value } : item)))}>
                      {ACTION_OPTIONS.map((option) => (
                        <option key={`del-${index}-${option || 'none'}`} value={option}>
                          {option || 'none'}
                        </option>
                      ))}
                    </select>
                    <button type="button" className="danger" onClick={() => setForeignKeys((prev) => prev.filter((_, i) => i !== index))}>Delete</button>
                  </div>
                  {showValidation && validation.foreignKeys[index] ? <div className="row-error">{validation.foreignKeys[index]}</div> : null}
                </React.Fragment>
              ))}
            </div>
          </div>
        ) : null}

        {checks.map((row, index) => (
          <div key={`check-${index}`} className="constraint-grid three-col">
            <input value={row.name} onChange={(event) => setChecks((prev) => prev.map((item, i) => (i === index ? { ...item, name: event.target.value } : item)))} placeholder="constraint name (optional)" />
            <input value={row.expression} onChange={(event) => setChecks((prev) => prev.map((item, i) => (i === index ? { ...item, expression: event.target.value } : item)))} placeholder="price > 0" />
            <button type="button" className="danger" onClick={() => setChecks((prev) => prev.filter((_, i) => i !== index))}>Delete</button>
          </div>
        ))}
      </details>

      {showValidation && issueCount > 0 ? (
        <div className="status-banner error">
          {issueCount} validation issue{issueCount === 1 ? '' : 's'} need attention before preview.
        </div>
      ) : null}
      {showValidation && warningCount > 0 ? (
        <div className="status-banner warning">
          {warningCount} warning{warningCount === 1 ? '' : 's'}: {validation.warnings.join(' ')}
        </div>
      ) : null}
      {status && <div className={`status-banner ${status.type}`}>{status.text}</div>}

      <section className="sticky-footer">
        <div className="known-columns">Columns: {knownColumnNames.join(', ') || '(none)'}</div>
        <div className="footer-actions-right">
          {isEditMode && (
            <button type="button" className="danger" onClick={requestDropTable} disabled={executing}>
              Drop Table
            </button>
          )}
          {!isEditMode && <button type="button" onClick={resetForm}>Reset</button>}
          <button type="button" onClick={() => vscode.postMessage({ command: 'cancel' })}>Cancel</button>
          <button type="button" className="primary" onClick={requestPreview} disabled={previewDisabled}>
            {isEditMode ? 'Preview Changes' : 'Preview SQL'}
          </button>
        </div>
      </section>

      {showDropConfirm && (
        <div className="save-confirm-overlay">
          <div className="save-confirm-modal" role="dialog" aria-modal="true" aria-label="Confirm drop table">
            <div className="save-confirm-header">
              <h2>Drop Table</h2>
            </div>
            <div className="save-confirm-meta">
              <div className="save-confirm-meta-item">
                <span>Table</span>
                <code>{context?.schemaName}.{tableName}</code>
              </div>
            </div>
            <div className="status-banner error" style={{ margin: '12px 0' }}>
              This will permanently delete the table and all its data. This action cannot be undone.
            </div>
            <div className="save-confirm-actions">
              <button type="button" onClick={() => setShowDropConfirm(false)} disabled={executing}>Cancel</button>
              <button type="button" className="danger" onClick={executeDropTable} disabled={executing}>
                {executing ? 'Dropping...' : 'Drop Table'}
              </button>
            </div>
          </div>
        </div>
      )}

      {preview?.payload.statements.length ? (
        <div className="save-confirm-overlay">
          <div ref={modalRef} className="save-confirm-modal" role="dialog" aria-modal="true" aria-label="Confirm SQL execution">
            <div className="save-confirm-header">
              <h2>Confirm SQL Execution</h2>
              <span className="save-confirm-chip">
                {preview.payload.statements.length} {preview.payload.statements.length === 1 ? 'statement' : 'statements'}
              </span>
            </div>
            <div className="save-confirm-meta">
              <div className="save-confirm-meta-item">
                <span>Connection</span>
                <code>{preview.payload.connectionName}</code>
              </div>
              <div className="save-confirm-meta-item">
                <span>Target</span>
                <code>{preview.payload.targetLabel}</code>
              </div>
            </div>
            <div className="save-confirm-sql-list">
              {preview.payload.statements.map((statement, index) => (
                <div key={`statement-${index}`} className="save-confirm-sql-card">
                  <div className="save-confirm-sql-title">Statement {index + 1}</div>
                  <pre className="save-confirm-sql-code" dangerouslySetInnerHTML={{ __html: highlightSql(statement) }} />
                </div>
              ))}
            </div>
            <div className="save-confirm-actions">
              <button type="button" onClick={() => setPreview(null)} disabled={executing}>Cancel</button>
              <button type="button" className="primary" onClick={executeSql} disabled={executing}>
                {executing ? 'Executing...' : 'Execute SQL'}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
};
