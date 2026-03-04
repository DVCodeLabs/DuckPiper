export type TransformationLayer = 'bronze' | 'silver' | 'gold';
export type MaterializationType = 'table' | 'view';

export interface NotebookCell {
    id: string;
    type: 'sql' | 'markdown';
    // For SQL cells
    layer?: TransformationLayer;
    outputName?: string;
    materialize?: MaterializationType;
    sql?: string; // or content for markdown
    content?: string; // For markdown cells, or unification
}

export interface NotebookData {
    version: string;
    name: string;
    engine?: {
        type: 'duckdb';
        duckdbPath?: string;
    };
    sourceConnectionId?: string;
    cells: NotebookCell[];
}

export interface CellResult {
    cellId: string;
    success: boolean;
    rowCount?: number;
    head?: Record<string, unknown>[]; // Preview rows
    error?: string;
    scanBytes?: number;
    executionTimeMs?: number;
}
