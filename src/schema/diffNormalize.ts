import { SchemaModel, TableModel, ColumnModel, ForeignKeyModel, IndexModel, RoutineModel } from '../core/types';

/**
 * Normalize a SchemaModel for stable diffing.
 * Strips transient/connection-specific fields and sorts all arrays alphabetically.
 */
export function normalizeSchemaForDiff(schema: SchemaModel): object {
  const tables = (schema.tables || [])
    .slice()
    .sort(byName)
    .map(normalizeTableForDiff);

  const views = (schema.views || [])
    .slice()
    .sort(byName)
    .map(normalizeTableForDiff);

  const procedures = (schema.procedures || [])
    .slice()
    .sort(byName)
    .map(normalizeRoutineForDiff);

  const functions = (schema.functions || [])
    .slice()
    .sort(byName)
    .map(normalizeRoutineForDiff);

  const result: Record<string, unknown> = { name: schema.name };
  if (tables.length > 0) { result.tables = tables; }
  if (views.length > 0) { result.views = views; }
  if (procedures.length > 0) { result.procedures = procedures; }
  if (functions.length > 0) { result.functions = functions; }

  return result;
}

/**
 * Normalize a TableModel for stable diffing.
 * Sorts columns alphabetically, sorts FKs and indexes for deterministic output.
 */
export function normalizeTableForDiff(table: TableModel): object {
  const columns = (table.columns || [])
    .slice()
    .sort(byName)
    .map(normalizeColumnForDiff);

  const result: Record<string, unknown> = {
    name: table.name,
    columns,
  };

  if (table.comment) {
    result.comment = table.comment;
  }

  if (table.primaryKey && table.primaryKey.length > 0) {
    result.primaryKey = table.primaryKey.slice().sort();
  }

  if (table.foreignKeys && table.foreignKeys.length > 0) {
    result.foreignKeys = table.foreignKeys
      .slice()
      .sort(compareForeignKeys)
      .map(normalizeForeignKeyForDiff);
  }

  if (table.indexes && table.indexes.length > 0) {
    result.indexes = table.indexes
      .slice()
      .sort(byName)
      .map(normalizeIndexForDiff);
  }

  return result;
}

function normalizeColumnForDiff(col: ColumnModel): object {
  const result: Record<string, unknown> = {
    name: col.name,
    type: col.type,
  };
  if (col.nullable !== undefined) {
    result.nullable = col.nullable;
  }
  if (col.comment) {
    result.comment = col.comment;
  }
  return result;
}

function normalizeForeignKeyForDiff(fk: ForeignKeyModel): object {
  const result: Record<string, unknown> = {
    column: fk.column,
    foreignSchema: fk.foreignSchema,
    foreignTable: fk.foreignTable,
    foreignColumn: fk.foreignColumn,
  };
  if (fk.name) {
    result.name = fk.name;
  }
  return result;
}

function normalizeIndexForDiff(idx: IndexModel): object {
  const result: Record<string, unknown> = {
    name: idx.name,
    columns: idx.columns.slice().sort(),
  };
  if (idx.unique !== undefined) {
    result.unique = idx.unique;
  }
  return result;
}

function normalizeRoutineForDiff(routine: RoutineModel): object {
  const result: Record<string, unknown> = {
    name: routine.name,
    kind: routine.kind,
  };
  if (routine.returnType) { result.returnType = routine.returnType; }
  if (routine.language) { result.language = routine.language; }
  if (routine.signature) { result.signature = routine.signature; }
  if (routine.parameters && routine.parameters.length > 0) {
    result.parameters = routine.parameters.map(p => {
      const param: Record<string, unknown> = { name: p.name };
      if (p.mode) { param.mode = p.mode; }
      if (p.type) { param.type = p.type; }
      return param;
    });
  }
  return result;
}

// --- Sort helpers ---

function byName(a: { name: string }, b: { name: string }): number {
  return a.name.localeCompare(b.name);
}

function compareForeignKeys(a: ForeignKeyModel, b: ForeignKeyModel): number {
  const colCmp = a.column.localeCompare(b.column);
  if (colCmp !== 0) return colCmp;
  return a.foreignTable.localeCompare(b.foreignTable);
}
