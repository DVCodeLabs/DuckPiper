export interface PipelineIndex {
    version: "0.1";
    generatedAt: string; // ISO-8601
    pipelines: PipelineIndexEntry[];
}

export interface PipelineIndexEntry {
    pipelineId: string; // uuid
    name: string; // name or notebook base name
    notebookPath: string; // workspace relative
    lineagePath: string; // workspace relative
    duckdbPath: string; // likely DP/system/piper.duckdb
    updatedAt: string; // ISO-8601
    lastRunAt?: string | null; // ISO-8601
    createdAt?: string; // ISO-8601
    docPath?: string; // workspace relative path to companion markdown
}

export interface PipelineLineage {
    version: "0.2";
    pipelineId: string; // uuid
    generatedAt: string; // ISO-8601
    notebookPath: string; // workspace relative
    duckdbPath: string; // DP/system/piper.duckdb

    tables: LineageTableNode[];
    tableEdges: LineageTableEdge[];
    columns: LineageColumnNode[];
    columnEdges: LineageColumnEdge[];
}

export interface LineageTableNode {
    id: string; // pipelineId::fqn
    fqn: string; // schema.table
    schema: string;
    name: string;
    type: "table" | "view" | "source";
    createdBy?: { cellId: string };
    sql?: {
        hash: string;
        preview: string;
    };
    metadata?: Record<string, unknown>; // internal usage
    status?: 'present' | 'missing' | 'deleted';
    deletedAt?: string;
    deletedBy?: 'user' | 'system';
}

/**
 * Reason for edge:
 * - sql_ref: explicit FROM/JOIN
 * - implicit: inferred based on flow (deprecated for V2?)
 */
export type TableEdgeReason = "sql_ref" | "implicit";

export interface LineageTableEdge {
    from: string; // pipelineId::fqn
    to: string; // pipelineId::fqn
    cellId?: string;
    reason: TableEdgeReason;
}

export interface LineageColumnNode {
    id: string; // pipelineId::fqn.col
    tableId: string; // pipelineId::fqn
    fqn: string; // schema.table.col
    name: string;
    type?: string;
    createdBy?: { cellId: string };
}

/**
 * Reason for column edge:
 * - select_expr: explicit SELECT .. AS ..
 * - wildcard: SELECT *
 */
export type ColumnEdgeReason = "select_expr" | "wildcard";

export interface LineageColumnEdge {
    from: string; // pipelineId::fqn.col (or .* for wildcard)
    to: string; // pipelineId::fqn.col
    cellId?: string;
    transform?: string;
    confidence: number; // 0..1
    reason: ColumnEdgeReason;
}
