import React, { useState, useEffect, useCallback, useRef } from 'react';
import { createRoot } from 'react-dom/client';
import { Download, Activity, DatabaseZap, RefreshCw, PenBox } from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import { ColDef, ModuleRegistry, AllCommunityModule, themeBalham, colorSchemeDark, colorSchemeLight } from 'ag-grid-community';
import ReactFlow, { Background, Controls, Node, Edge, Handle, Position, applyNodeChanges, applyEdgeChanges, ReactFlowInstance } from 'reactflow';
import ELK from 'elkjs/lib/elk.bundled.js';

// NOTE: Do NOT import 'ag-grid-community/styles/ag-grid.css' when using the new Theming API.
// The legacy CSS conflicts with programmatic theming via themeBalham.withPart().
import 'reactflow/dist/style.css';
import './webviewApp.css';

// Register ALL community modules to ensure Sorting, Filtering, etc. work
ModuleRegistry.registerModules([AllCommunityModule]);

import { ChartBuilder } from './chartBuilder';

const myThemeDark = themeBalham.withPart(colorSchemeDark).withParams({
    backgroundColor: 'transparent',
    foregroundColor: 'var(--vscode-editor-foreground)',
    headerBackgroundColor: 'var(--vscode-editor-groupHeader-tabsBackground)',
    headerTextColor: 'var(--vscode-editor-foreground)',
    oddRowBackgroundColor: 'rgba(255,255,255,0.04)',
    borderColor: 'var(--vscode-widget-border)',
    inputBorderRadius: '6px',
});

const myThemeLight = themeBalham.withPart(colorSchemeLight).withParams({
    backgroundColor: 'transparent',
    foregroundColor: 'var(--vscode-editor-foreground)',
    headerBackgroundColor: 'var(--vscode-editor-groupHeader-tabsBackground)',
    headerTextColor: 'var(--vscode-editor-foreground)',
    oddRowBackgroundColor: 'rgba(0,0,0,0.03)',
    borderColor: 'var(--vscode-widget-border)',
    inputBorderRadius: '6px',
});

// --- Types ---
interface GridData {
    columns: any[];
    rows: any[];
    meta?: {
        resultId?: string;
        source?: {
            catalog?: string;
            schema?: string;
            table?: string;
        };
        editable?: {
            enabled?: boolean;
            reason?: string;
            primaryKeyColumns?: string[];
            editableColumns?: string[];
        };
    };
}

interface MessageData {
    command: string;
    data: any;
}

interface AppNotice {
    title: string;
    message: string;
}

interface ResultsetEditsPreview {
    request: {
        resultId: string;
        source: {
            catalog?: string;
            schema?: string;
            table: string;
        };
        edits: Array<{
            rowKey: Record<string, unknown>;
            changes: Array<{ column: string; oldValue: unknown; newValue: unknown }>;
        }>;
    };
    connectionName: string;
    targetLabel: string;
    statements: string[];
}

type PendingRowEdit = {
    rowKey: Record<string, unknown>;
    changes: Record<string, { oldValue: unknown; newValue: unknown }>;
};

function isEqualValue(a: unknown, b: unknown): boolean {
    if (a === b) return true;
    if (a == null && b == null) return true;
    if (typeof a === 'object' || typeof b === 'object') {
        try {
            return JSON.stringify(a) === JSON.stringify(b);
        } catch {
            return false;
        }
    }
    return false;
}

function escapeHtml(text: string): string {
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function highlightSqlNonString(segment: string): string {
    const tokenPattern = /\b(?:UPDATE|SET|WHERE|AND|OR|NOT|IN|LIKE|IS|NULL|TRUE|FALSE)\b|\b-?\d+(?:\.\d+)?\b|[=<>!]+/gi;
    let html = '';
    let lastIndex = 0;
    let match: RegExpExecArray | null;

    while ((match = tokenPattern.exec(segment)) !== null) {
        html += escapeHtml(segment.slice(lastIndex, match.index));
        const token = match[0];
        const upper = token.toUpperCase();
        let className = 'sql-token-operator';

        if (/^-?\d/.test(token)) {
            className = 'sql-token-number';
        } else if ([
            'UPDATE', 'SET', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE'
        ].includes(upper)) {
            className = 'sql-token-keyword';
        }

        html += `<span class="${className}">${escapeHtml(token)}</span>`;
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

declare var acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    RadialLinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title,
    Tooltip,
    Legend,
    BarController,
    LineController,
    PieController,
    DoughnutController,
    ScatterController,
    RadarController
} from 'chart.js';
import { Chart } from 'react-chartjs-2';

ChartJS.register(
    CategoryScale,
    LinearScale,
    RadialLinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title,
    Tooltip,
    Legend,
    BarController,
    LineController,
    PieController,
    DoughnutController,
    ScatterController,
    RadarController
);

class ErrorBoundary extends React.Component<
    { children: React.ReactNode },
    { hasError: boolean; error: Error | null }
> {
    constructor(props: any) {
        super(props);
        this.state = { hasError: false, error: null };
    }

    static getDerivedStateFromError(error: Error) {
        return { hasError: true, error };
    }

    componentDidCatch(error: Error, errorInfo: any) {
        console.error("ErrorBoundary caught an error", error, errorInfo);
    }

    render() {
        if (this.state.hasError) {
            return (
                <div style={{ padding: 20, color: 'var(--vscode-errorForeground)' }}>
                    <h3>Something went wrong.</h3>
                    <pre>{this.state.error?.message}</pre>
                </div>
            );
        }
        return this.props.children;
    }
}

// --- Components ---

const ChartsTab = ({
    config,
    data,
    onNotify
}: {
    config: any;
    data: GridData | null;
    onNotify: (title: string, message: string) => void;
}) => {
    const [isEditing, setIsEditing] = useState(!config);
    const chartRef = React.useRef<any>(null);

    useEffect(() => {
        if (config) setIsEditing(false);
    }, [config]);

    if (!data) return <div className="placeholder">No data available to chart.</div>;

    const handleBuild = (newConfig: any) => {
        // Send to extension to save
        vscode.postMessage({ command: 'saveChartConfig', data: { config: newConfig } });
        // Optimistically update? The extension handles save and likely relies on file watcher or reload, 
        // but we can just let variable update via message loop if we want.
        // For now, assume extension will send back 'chartConfigSuccess' or we just wait.
        // Actually, better to send message and wait for update.
        // But for UX, let's just assume success or at least close editor if we want.
        // The resultsPanel handler does NOT automatically reload config after save unless we tell it to?
        // _saveChartConfig sends 'chartConfigSaved'. We should handle that.
    };

    // If no config, or editing mode, show Builder
    if (!config || isEditing) {
        return (
            <div style={{ height: '100%', overflow: 'auto' }}>
                <ChartBuilder
                    config={config}
                    columns={data.columns}
                    onBuild={handleBuild}
                    onCancel={config ? () => setIsEditing(false) : undefined}
                    onValidationError={(message) => onNotify('Chart Validation', message)}
                />
            </div>
        );
    }

    // Render Chart

    // Attempt to map data based on config
    // Fallback: if labelColumn not found, use first column
    const labelCol = config.labelColumn && data.rows.length > 0 && config.labelColumn in data.rows[0]
        ? config.labelColumn
        : (data.columns[0]?.name);

    if (!labelCol) return <div className="error">Could not determine label column.</div>;

    const labels = data.rows.map(row => row[labelCol]);

    // Fallback: if no dataset columns, try to find numeric columns
    let datasetCols = config.datasetColumns;
    if (!datasetCols || datasetCols.length === 0) {
        // Find numeric columns
        datasetCols = data.columns
            .filter((c: any) => ['integer', 'number', 'float', 'decimal'].some(t => c.type?.toLowerCase().includes(t)))
            .map((c: any) => c.name);
    }

    // Default colors for datasets
    const defaultColors = [
        'rgba(53, 162, 235, 0.8)',
        'rgba(255, 99, 132, 0.8)',
        'rgba(75, 192, 192, 0.8)',
        'rgba(255, 206, 86, 0.8)',
        'rgba(153, 102, 255, 0.8)',
        'rgba(255, 159, 64, 0.8)'
    ];

    const datasets = (datasetCols || []).map((col: string, idx: number) => {
        // Check if custom color is set in datasetStyles
        const customStyle = config.datasetStyles?.[col];
        const bgColor = customStyle?.backgroundColor || defaultColors[idx % defaultColors.length];
        const borderColor = customStyle?.borderColor || bgColor;

        return {
            label: col,
            data: data.rows.map(row => {
                const val = row[col];
                return isNaN(Number(val)) ? 0 : Number(val);
            }),
            backgroundColor: bgColor,
            borderColor: borderColor,
            borderWidth: 1
        };
    });

    const chartData = {
        labels,
        datasets
    };

    const options = {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'top' as const },
            title: { display: true, text: config.title || 'Query Results Chart' }
        }
    };


    const handleDownload = () => {
        if (chartRef.current) {
            const base64Image = chartRef.current.toBase64Image();
            const link = document.createElement('a');
            link.href = base64Image;
            link.download = (config.title || 'chart') + '.png';
            link.click();
        }
    };

    const handleRefresh = () => {
        // Just re-render by forcing state update or requesting fresh data
        // For now, the chart auto-updates with data prop changes.
        // If user wants to re-fetch, they should re-run the query.
        // But we can force a re-render by toggling a key or similar.
        // Since data is passed in, this is mostly a no-op unless we reload from config.
        vscode.postMessage({ command: 'loadChartConfig' });
    };

    return (
        <div className="chart-container" style={{ height: '100%', width: '100%', maxWidth: '100%', display: 'flex', flexDirection: 'column', boxSizing: 'border-box' }}>
            <div className="toolbar">
                <button onClick={handleDownload} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <Download size={14} /> Download Chart
                </button>
                <div className="separator" style={{ width: 1, background: 'var(--vscode-widget-border)', margin: '0 8px' }}></div>
                <button onClick={() => setIsEditing(true)} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <PenBox size={14} /> Edit Chart
                </button>
                <div className="separator" style={{ width: 1, background: 'var(--vscode-widget-border)', margin: '0 8px' }}></div>
                <button onClick={handleRefresh} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <RefreshCw size={14} /> Refresh
                </button>
            </div>
            {/* Chart */}
            <div style={{ padding: '20px', height: '100%', width: '100%', maxWidth: '100%', display: 'flex', flexDirection: 'column', boxSizing: 'border-box' }}>
                <div style={{ flex: 1, minHeight: 0, maxWidth: '100%' }}>
                    <Chart ref={chartRef} type={config.type || 'bar'} data={chartData} options={options} />
                </div>
            </div>
        </div>
    );
};

const ResultsTab = ({
    data,
    theme,
    onOpenChartBuilder,
    allowCsvExport
}: {
    data: GridData | null;
    theme: any;
    onOpenChartBuilder: () => void;
    allowCsvExport: boolean;
}) => {
    const [rowData, setRowData] = useState<any[]>([]);
    const [baselineRows, setBaselineRows] = useState<any[]>([]);
    const [baselineByKey, setBaselineByKey] = useState<Record<string, any>>({});
    const [pendingEdits, setPendingEdits] = useState<Record<string, PendingRowEdit>>({});

    useEffect(() => {
        if (!data) {
            setRowData([]);
            setBaselineRows([]);
            setBaselineByKey({});
            setPendingEdits({});
            return;
        }
        const clonedRows = (data.rows || []).map((row: any) => ({ ...row }));
        setRowData(clonedRows);
        setBaselineRows(clonedRows.map((row: any) => ({ ...row })));
        setPendingEdits({});
    }, [data]);

    const editMeta = data?.meta?.editable;
    const primaryKeyColumns = editMeta?.primaryKeyColumns || [];
    const editableColumns = editMeta?.editableColumns || [];
    const editableEnabled = !!editMeta?.enabled;
    const editableColumnSet = new Set(editableColumns);
    const primaryKeySet = new Set(primaryKeyColumns);
    const allowExport = allowCsvExport !== false;

    const makeRowKey = (row: any): Record<string, unknown> | null => {
        if (!primaryKeyColumns.length) return null;
        const rowKey: Record<string, unknown> = {};
        for (const key of primaryKeyColumns) {
            if (!(key in row)) {
                return null;
            }
            rowKey[key] = row[key];
        }
        return rowKey;
    };

    const rowKeyString = (rowKey: Record<string, unknown>) =>
        primaryKeyColumns.map((pk) => `${pk}:${JSON.stringify(rowKey[pk])}`).join('|');

    useEffect(() => {
        if (!editableEnabled || !primaryKeyColumns.length) {
            setBaselineByKey({});
            return;
        }
        const next: Record<string, any> = {};
        for (const row of baselineRows) {
            const rk = makeRowKey(row);
            if (!rk) continue;
            next[rowKeyString(rk)] = { ...row };
        }
        setBaselineByKey(next);
    }, [editableEnabled, baselineRows, primaryKeyColumns.join('|')]);

    if (!data) return <div className="placeholder">Run a query to see results.</div>;

    if (data.columns.length === 0 && rowData.length === 0) {
        return (
            <div className="placeholder">
                <div style={{ fontSize: '1.2em', marginBottom: '10px' }}>✅ Query executed successfully.</div>
                <div style={{ opacity: 0.7 }}>No rows returned.</div>
            </div>
        );
    }

    const isDirtyCell = (row: any, colName: string): boolean => {
        if (!editableEnabled) return false;
        const rowKey = makeRowKey(row);
        if (!rowKey) return false;
        const key = rowKeyString(rowKey);
        return !!pendingEdits[key]?.changes[colName];
    };

    const colDefs: ColDef[] = data.columns.map((col: any) => ({
        field: col.name,
        headerName: col.name,
        editable: editableEnabled && editableColumnSet.has(col.name) && !primaryKeySet.has(col.name),
        cellStyle: (params: any) => isDirtyCell(params.data, col.name) ? { backgroundColor: 'rgba(245, 158, 11, 0.2)' } : undefined,
    }));

    const defaultColDef = {
        sortable: true,
        filter: true,
        floatingFilter: true,
        resizable: true,
        unSortIcon: true,
        minWidth: 100,
    };

    const autoSizeStrategy = {
        type: 'fitCellContents' as const,
    };

    const dirtyCount = Object.values(pendingEdits)
        .reduce((sum, rowEdit) => sum + Object.keys(rowEdit.changes).length, 0);

    const handleRevertAll = () => {
        setRowData(baselineRows.map((row) => ({ ...row })));
        setPendingEdits({});
    };

    const handleSaveChanges = () => {
        if (!editableEnabled || !data.meta?.resultId || !data.meta?.source || dirtyCount === 0) {
            return;
        }

        const edits = Object.values(pendingEdits).map((rowEdit) => ({
            rowKey: rowEdit.rowKey,
            changes: Object.entries(rowEdit.changes).map(([column, values]) => ({
                column,
                oldValue: values.oldValue,
                newValue: values.newValue
            }))
        }));

        vscode.postMessage({
            command: 'applyResultsetEdits',
            data: {
                resultId: data.meta.resultId,
                source: data.meta.source,
                edits
            }
        });
    };

    return (
        <div style={{ height: '100%', width: '100%', display: 'flex', flexDirection: 'column' }}>
            <div className="toolbar">
                {allowExport && (
                    <>
                        <button onClick={() => vscode.postMessage({ command: 'exportCsv' })} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <Download size={14} /> Export CSV
                        </button>
                        <div className="separator" style={{ width: 1, background: 'var(--vscode-widget-border)', margin: '0 8px' }}></div>
                    </>
                )}
                <button
                    onClick={onOpenChartBuilder}
                    title="Manually create a chart"
                    style={{ display: 'flex', alignItems: 'center', gap: '4px' }}
                >
                    <Activity size={14} /> Chart Builder
                </button>
                <div className="separator" style={{ width: 1, background: 'var(--vscode-widget-border)', margin: '0 8px' }}></div>
                <button onClick={() => vscode.postMessage({ command: 'cacheRerunQuery' })} title="Cache the full resultset to DuckDB (re-runs query)" style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <DatabaseZap size={14} /> Cache Resultset
                </button>
                <div className="separator" style={{ width: 1, background: 'var(--vscode-widget-border)', margin: '0 8px' }}></div>
                <span
                    title={editableEnabled ? 'Resultset editing is enabled.' : (editMeta?.reason || 'Resultset is read-only.')}
                    style={{
                        fontSize: 12,
                        padding: '4px 8px',
                        borderRadius: 6,
                        border: '1px solid var(--vscode-widget-border)',
                        opacity: 0.95
                    }}
                >
                    {editableEnabled ? `Editable${dirtyCount > 0 ? ` (${dirtyCount} ${dirtyCount === 1 ? 'change' : 'changes'})` : ''}` : 'Read-only results'}
                </span>
                {editableEnabled && (
                    <>
                        <div className="separator" style={{ width: 1, background: 'var(--vscode-widget-border)', margin: '0 8px' }}></div>
                        <button onClick={handleSaveChanges} disabled={dirtyCount === 0}>
                            Save Changes
                        </button>
                        <button onClick={handleRevertAll} disabled={dirtyCount === 0}>
                            Revert All
                        </button>
                    </>
                )}
            </div>
            <div style={{ flex: 1, width: '100%' }}>
                <AgGridReact
                    rowData={rowData}
                    columnDefs={colDefs}
                    defaultColDef={defaultColDef}
                    theme={theme}
                    autoSizeStrategy={autoSizeStrategy}
                    pagination={true}
                    paginationPageSize={100}
                    singleClickEdit={editableEnabled}
                    stopEditingWhenCellsLoseFocus={editableEnabled}
                    undoRedoCellEditing={editableEnabled}
                    onCellValueChanged={(params: any) => {
                        if (!editableEnabled) return;
                        const colName = params.colDef?.field;
                        if (!colName) return;

                        const rowKey = makeRowKey(params.data);
                        if (!rowKey) return;

                        const key = rowKeyString(rowKey);
                        const baselineRow = baselineByKey[key];
                        const oldValue = baselineRow && colName in baselineRow ? baselineRow[colName] : params.oldValue;
                        const newValue = params.newValue;

                        setPendingEdits((prev) => {
                            const next: Record<string, PendingRowEdit> = { ...prev };
                            const existing = next[key] || { rowKey, changes: {} };
                            const changed = !isEqualValue(oldValue, newValue);

                            if (!changed) {
                                const updatedChanges = { ...existing.changes };
                                delete updatedChanges[colName];

                                if (Object.keys(updatedChanges).length === 0) {
                                    delete next[key];
                                } else {
                                    next[key] = { ...existing, changes: updatedChanges };
                                }
                                return next;
                            }

                            next[key] = {
                                rowKey,
                                changes: {
                                    ...existing.changes,
                                    [colName]: { oldValue, newValue }
                                }
                            };
                            return next;
                        });
                    }}
                />
            </div>
        </div>
    );
};

const SaveConfirmModal = ({
    preview,
    executing,
    onCancel,
    onConfirm
}: {
    preview: ResultsetEditsPreview | null;
    executing: boolean;
    onCancel: () => void;
    onConfirm: () => void;
}) => {
    if (!preview) return null;

    return (
        <div className="save-confirm-overlay">
            <div className="save-confirm-modal" role="dialog" aria-modal="true" aria-label="Confirm SQL execution">
                <div className="save-confirm-header">
                    <h2>Confirm SQL Execution</h2>
                    <span className="save-confirm-chip">
                        {preview.statements.length} {preview.statements.length === 1 ? 'statement' : 'statements'}
                    </span>
                </div>
                <div className="save-confirm-meta">
                    <div className="save-confirm-meta-item">
                        <span>Connection</span>
                        <code>{preview.connectionName}</code>
                    </div>
                    <div className="save-confirm-meta-item">
                        <span>Target</span>
                        <code>{preview.targetLabel}</code>
                    </div>
                </div>
                <div className="save-confirm-sql-list">
                    {preview.statements.map((statement, index) => (
                        <div className="save-confirm-sql-card" key={`${index}-${statement.slice(0, 24)}`}>
                            <div className="save-confirm-sql-title">Statement {index + 1}</div>
                            <pre
                                className="save-confirm-sql-code"
                                dangerouslySetInnerHTML={{ __html: highlightSql(statement) }}
                            />
                        </div>
                    ))}
                </div>
                <div className="save-confirm-actions">
                    <button type="button" onClick={onCancel} disabled={executing}>Cancel</button>
                    <button type="button" onClick={onConfirm} disabled={executing}>
                        {executing ? 'Executing...' : 'Execute SQL'}
                    </button>
                </div>
            </div>
        </div>
    );
};

const AppNoticeModal = ({
    notice,
    onClose
}: {
    notice: AppNotice | null;
    onClose: () => void;
}) => {
    if (!notice) return null;

    return (
        <div className="app-notice-overlay">
            <div className="app-notice-modal" role="dialog" aria-modal="true" aria-label={notice.title}>
                <div className="app-notice-header">
                    <h2>{notice.title}</h2>
                </div>
                <div className="app-notice-message">{notice.message}</div>
                <div className="app-notice-actions">
                    <button type="button" onClick={onClose}>OK</button>
                </div>
            </div>
        </div>
    );
};

const ExplainTab = ({ markdown, error }: { markdown: string | null, error: string | null }) => {
    if (error) return <div className="error">Error: {error}</div>;
    if (!markdown) return <div className="placeholder">The explanation will show up here.</div>;

    const html = markdown
        .replace(/^### (.*$)/gim, '<h3>$1</h3>')
        .replace(/^## (.*$)/gim, '<h2>$1</h2>')
        .replace(/^# (.*$)/gim, '<h1>$1</h1>')
        .replace(/\*\*(.*)\*\*/gim, '<b>$1</b>')
        .replace(/\*(.*)\*/gim, '<i>$1</i>')
        .replace(/`(.*?)`/gim, '<code>$1</code>')
        .replace(/\n/gim, '<br>');

    return (
        <div className="explain-content" dangerouslySetInnerHTML={{ __html: html }} />
    );
};

// --- Shared Components ---

// Custom Table Node Component (Reused for ERD and Lineage)
const TableNode = ({ data }: any) => {
    const columns = data.columns || [];

    const handleColumnClick = (colName: string, index: number) => {
        if (data.onColumnClick) {
            data.onColumnClick(data.nodeId, colName, index);
        }
    };

    return (
        <div style={{
            background: 'var(--vscode-editor-background)',
            border: '1px solid var(--vscode-widget-border)',
            borderRadius: '6px',
            minWidth: '220px',
            fontSize: '12px',
            boxShadow: '0 2px 8px rgba(0,0,0,0.3)'
        }}>
            {/* Table Header */}
            <div style={{
                background: 'var(--vscode-editor-groupHeader-tabsBackground)',
                padding: '8px 12px',
                fontWeight: 600,
                borderBottom: '1px solid var(--vscode-widget-border)',
                borderTopLeftRadius: '6px',
                borderTopRightRadius: '6px',
                color: 'var(--vscode-editor-foreground)'
            }}>
                {data.label}
            </div>

            {/* Columns List */}
            <div style={{ padding: '4px 0' }}>
                {columns.map((col: any, index: number) => (
                    <div
                        key={index}
                        onClick={() => handleColumnClick(col.name, index)}
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            padding: '6px 12px',
                            gap: '8px',
                            position: 'relative',
                            borderBottom: index < columns.length - 1 ? '1px solid var(--vscode-widget-shadow)' : 'none',
                            color: 'var(--vscode-editor-foreground)',
                            cursor: data.onColumnClick ? 'pointer' : 'default',
                            background: col.highlight ? 'var(--vscode-list-activeSelectionBackground)' : 'transparent'
                        }}
                    >
                        {/* Connection Handle (Left) */}
                        <Handle
                            type="target"
                            position={Position.Left}
                            id={`${data.nodeId}-col-${index}`}
                            style={{
                                left: -4,
                                width: 8,
                                height: 8,
                                background: col.relationshipColor || (col.isPrimaryKey ? '#60a5fa' : '#9ca3af'),
                                border: 'none'
                            }}
                        />
                        {/* Connection Handle (Right) */}
                        <Handle
                            type="source"
                            position={Position.Right}
                            id={`${data.nodeId}-col-${index}`}
                            style={{
                                right: -4,
                                width: 8,
                                height: 8,
                                background: col.relationshipColor || (col.isForeignKey ? '#f59e0b' : '#9ca3af'),
                                border: 'none'
                            }}
                        />

                        {/* Column Name */}
                        <span style={{
                            flex: 1,
                            fontWeight: col.isPrimaryKey || col.isForeignKey ? 600 : 400,
                            color: col.relationshipColor || (col.isPrimaryKey ? '#60a5fa' : (col.isForeignKey ? '#f59e0b' : 'var(--vscode-editor-foreground)'))
                        }}>
                            {col.name}
                        </span>

                        {/* Badges */}
                        {col.isPrimaryKey && (
                            <span style={{ fontSize: '9px', padding: '1px 4px', background: col.relationshipColor || '#60a5fa', color: '#000', borderRadius: '3px', fontWeight: 600 }}>PK</span>
                        )}
                        {col.isForeignKey && !col.isPrimaryKey && (
                            <span style={{ fontSize: '9px', padding: '1px 4px', background: col.relationshipColor || '#f59e0b', color: '#000', borderRadius: '3px', fontWeight: 600 }}>FK</span>
                        )}

                        {/* Data Type */}
                        <span style={{ fontSize: '10px', color: 'var(--vscode-descriptionForeground)', fontFamily: 'monospace' }}>
                            {col.type}
                        </span>
                    </div>
                ))}
            </div>
        </div>
    );
};

const nodeTypes = {
    tableNode: TableNode
};

// --- ELK Auto-Layout ---
const elk = new ELK();

async function computeElkLayout(
    nodes: Node[],
    edges: Edge[]
): Promise<Record<string, { x: number; y: number }>> {
    const NODE_WIDTH = 250;
    const ROW_HEIGHT = 24;
    const HEADER_HEIGHT = 50;

    const elkGraph = {
        id: 'root',
        layoutOptions: {
            'elk.algorithm': 'layered',
            'elk.direction': 'RIGHT',
            'elk.spacing.nodeNode': '60',
            'elk.layered.spacing.nodeNodeBetweenLayers': '100',
            'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
        },
        children: nodes.map(n => ({
            id: n.id,
            width: NODE_WIDTH,
            height: HEADER_HEIGHT + ((n.data as any).columns?.length || 1) * ROW_HEIGHT,
        })),
        edges: edges.map(e => ({
            id: e.id,
            sources: [e.source],
            targets: [e.target],
        })),
    };

    const result = await elk.layout(elkGraph);
    const positions: Record<string, { x: number; y: number }> = {};
    for (const child of result.children || []) {
        positions[child.id] = { x: child.x!, y: child.y! };
    }
    return positions;
}

// ERD Graph View
const ERDTab = ({ nodes, edges, graphSignature, layout, connectionName }: {
    nodes: Node[];
    edges: Edge[];
    graphSignature?: string;
    layout?: { graphSignature: string; positions: Record<string, { x: number; y: number }> };
    connectionName?: string;
}) => {
    const [localNodes, setLocalNodes] = useState<Node[]>(nodes);
    const [localEdges, setLocalEdges] = useState<Edge[]>(edges);
    const rfInstance = useRef<ReactFlowInstance | null>(null);
    const saveTimerRef = useRef<any>(null);
    const currentSignatureRef = useRef<string | undefined>(graphSignature);
    const connectionNameRef = useRef<string | undefined>(connectionName);

    const persistPositions = useCallback((
        positions: Record<string, { x: number; y: number }>,
        sig: string,
        connName: string
    ) => {
        vscode.postMessage({
            command: 'saveErdLayout',
            data: { connectionName: connName, graphSignature: sig, positions }
        });
    }, []);

    // Apply layout on mount / data change
    useEffect(() => {
        currentSignatureRef.current = graphSignature;
        connectionNameRef.current = connectionName;

        const applyLayout = async () => {
            let positions: Record<string, { x: number; y: number }> | undefined;

            // Check if saved layout matches
            if (layout && layout.graphSignature === graphSignature && layout.positions) {
                positions = layout.positions;
            }

            // Run ELK if no matching saved layout
            if (!positions) {
                try {
                    positions = await computeElkLayout(nodes, edges);
                    // Persist the new layout
                    if (graphSignature && connectionName) {
                        persistPositions(positions, graphSignature, connectionName);
                    }
                } catch (err) {
                    console.warn('ELK layout failed, using fallback grid positions', err);
                    setLocalNodes(nodes);
                    setLocalEdges(edges);
                    return;
                }
            }

            // Apply positions to nodes
            const positionedNodes = nodes.map(n => ({
                ...n,
                position: positions![n.id] || n.position,
            }));
            setLocalNodes(positionedNodes);
            setLocalEdges(edges);

            // fitView after render
            setTimeout(() => rfInstance.current?.fitView({ padding: 0.15 }), 50);
        };

        applyLayout();
    }, [nodes, edges, graphSignature]);

    const onNodesChange = useCallback((changes: any) => setLocalNodes((nds) => applyNodeChanges(changes, nds)), []);
    const onEdgesChange = useCallback((changes: any) => {
        // Check for edge removals and notify extension if it's a custom edge
        changes.forEach((change: any) => {
            if (change.type === 'remove') {
                const edgeId = change.id;
                if (edgeId.startsWith('e-custom-')) {
                    vscode.postMessage({
                        command: 'deleteCustomRelationship',
                        data: { edgeId }
                    });
                }
            }
        });
        return setLocalEdges((eds) => applyEdgeChanges(changes, eds));
    }, []);

    // Debounced drag persistence
    const onNodeDragStop = useCallback(() => {
        if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
        saveTimerRef.current = setTimeout(() => {
            setLocalNodes(currentNodes => {
                const positions: Record<string, { x: number; y: number }> = {};
                currentNodes.forEach(n => { positions[n.id] = n.position; });
                if (currentSignatureRef.current && connectionNameRef.current) {
                    persistPositions(positions, currentSignatureRef.current, connectionNameRef.current);
                }
                return currentNodes;
            });
        }, 500);
    }, [persistPositions]);

    // Re-layout handler
    const handleRelayout = useCallback(async () => {
        try {
            const positions = await computeElkLayout(localNodes, localEdges);
            const repositioned = localNodes.map(n => ({
                ...n,
                position: positions[n.id] || n.position,
            }));
            setLocalNodes(repositioned);
            if (currentSignatureRef.current && connectionNameRef.current) {
                persistPositions(positions, currentSignatureRef.current, connectionNameRef.current);
            }
            setTimeout(() => rfInstance.current?.fitView({ padding: 0.15 }), 50);
        } catch (err) {
            console.warn('Re-layout failed', err);
        }
    }, [localNodes, localEdges, persistPositions]);

    // Handle new connection (drag from one column to another)
    const onConnect = useCallback((connection: any) => {
        const parseHandle = (handleId: string) => {
            const parts = handleId.split('-col-');
            const tableId = parts[0];
            const colIndex = parseInt(parts[1], 10);
            return { tableId, colIndex };
        };

        const source = parseHandle(connection.sourceHandle);
        const target = parseHandle(connection.targetHandle);

        const sourceNode = localNodes.find(n => n.id === connection.source);
        const targetNode = localNodes.find(n => n.id === connection.target);

        if (!sourceNode || !targetNode) return;

        const sourceCol = (sourceNode.data as any).columns[source.colIndex];
        const targetCol = (targetNode.data as any).columns[target.colIndex];

        if (!sourceCol || !targetCol) return;

        vscode.postMessage({
            command: 'saveCustomRelationship',
            data: {
                source: connection.source,
                sourceColumn: sourceCol.name,
                target: connection.target,
                targetColumn: targetCol.name
            }
        });

        const newEdge: Edge = {
            id: `e-custom-${connection.source}-${sourceCol.name}-${connection.target}-${targetCol.name}`,
            source: connection.source,
            sourceHandle: connection.sourceHandle,
            target: connection.target,
            targetHandle: connection.targetHandle,
            type: 'smoothstep',
            animated: false,
            style: { stroke: '#60a5fa', strokeWidth: 2, strokeDasharray: '5,5' }
        };

        setLocalEdges(eds => [...eds, newEdge]);
    }, [localNodes]);

    const onEdgeMouseEnter = useCallback((_event: any, edge: Edge) => {
        const full = (edge.data as any)?.labelFull;
        if (!full) return;
        setLocalEdges(eds => eds.map(e => e.id === edge.id ? { ...e, label: full } : e));
    }, []);

    const onEdgeMouseLeave = useCallback((_event: any, edge: Edge) => {
        const short = (edge.data as any)?.labelShort;
        if (!short) return;
        setLocalEdges(eds => eds.map(e => e.id === edge.id ? { ...e, label: short } : e));
    }, []);

    if (nodes.length === 0) return <div className="placeholder">Click "View ERD" in the Explorer or Local Data Work panels to visualize a schema.</div>;

    return (
        <div style={{ height: '100%', width: '100%', position: 'relative' }}>
            <div style={{
                position: 'absolute',
                top: 10,
                left: 10,
                zIndex: 10,
                display: 'flex',
                gap: '8px',
                alignItems: 'center'
            }}>
                <div style={{
                    background: 'var(--vscode-editor-background)',
                    border: '1px solid var(--vscode-widget-border)',
                    borderRadius: 4,
                    padding: '6px 10px',
                    fontSize: '11px',
                    color: 'var(--vscode-descriptionForeground)'
                }}>
                    Drag from a column handle to another to create a custom relationship
                </div>
                <button
                    onClick={handleRelayout}
                    style={{
                        background: 'var(--vscode-button-background)',
                        color: 'var(--vscode-button-foreground)',
                        border: 'none',
                        borderRadius: 4,
                        padding: '6px 10px',
                        fontSize: '11px',
                        cursor: 'pointer'
                    }}
                >
                    Re-layout
                </button>
            </div>
            <ReactFlow
                nodes={localNodes}
                edges={localEdges}
                nodeTypes={nodeTypes}
                onInit={(instance) => { rfInstance.current = instance; }}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                onNodeDragStop={onNodeDragStop}
                onEdgeMouseEnter={onEdgeMouseEnter}
                onEdgeMouseLeave={onEdgeMouseLeave}
                nodesDraggable={true}
                nodesConnectable={true}
                elementsSelectable={true}
                fitView
                minZoom={0.1}
                maxZoom={1.5}
                connectionLineStyle={{ stroke: '#60a5fa', strokeWidth: 2, strokeDasharray: '5,5' }}
            >
                <Background />
                <Controls />
            </ReactFlow>
        </div>
    );
};

// Lineage Graph View with Trace Support
const LineageGraph = ({ nodes, edges }: { nodes: Node[], edges: Edge[] }) => {
    const [localNodes, setLocalNodes] = useState<Node[]>(nodes);
    const [localEdges, setLocalEdges] = useState<Edge[]>(edges);
    const [traceActive, setTraceActive] = useState(false);

    useEffect(() => {
        setLocalNodes(nodes);
        setLocalEdges(edges.map(e => ({
            ...e,
            style: {
                ...e.style,
                stroke: (e.data as any)?.color || e.style?.stroke
            }
        })));
        setTraceActive(false);
    }, [nodes, edges]);

    const onNodesChange = useCallback((changes: any) => setLocalNodes((nds) => applyNodeChanges(changes, nds)), []);
    const onEdgesChange = useCallback((changes: any) => setLocalEdges((eds) => applyEdgeChanges(changes, eds)), []);

    const onEdgeMouseEnter = useCallback((_event: any, edge: Edge) => {
        const full = (edge.data as any)?.labelFull;
        if (!full) return;
        setLocalEdges(eds => eds.map(e => e.id === edge.id ? { ...e, label: full } : e));
    }, []);

    const onEdgeMouseLeave = useCallback((_event: any, edge: Edge) => {
        const short = (edge.data as any)?.labelShort;
        if (!short) return;
        setLocalEdges(eds => eds.map(e => e.id === edge.id ? { ...e, label: short } : e));
    }, []);

    // Trace Logic: Upstream (Reverse) traversal
    const handleColumnClick = (nodeId: string, colName: string, colIndex: number) => {
        if (!nodeId) return;

        // Reset if clicking same or clearing
        // For now, always re-compute trace on click
        const targetHandle = `${nodeId}-col-${colIndex}`;

        // Find upstream edges
        const upstreamEdges = new Set<string>();
        const upstreamNodes = new Set<string>();

        // Initial node is part of trace
        upstreamNodes.add(nodeId);

        // Queue for BFS/DFS reverse walk
        // We look for edges where edge.target == currentTable AND edge.targetHandle == handle
        const queue: { tableId: string, handleId: string }[] = [{ tableId: nodeId, handleId: targetHandle }];

        while (queue.length > 0) {
            const current = queue.shift()!;

            // Find all incoming edges to this specific column handle (or row)
            // Note: Lineage generator makes edges target specific handles
            const incoming = localEdges.filter(e => e.target === current.tableId && e.targetHandle === current.handleId);

            incoming.forEach(edge => {
                if (!upstreamEdges.has(edge.id)) {
                    upstreamEdges.add(edge.id);
                    upstreamNodes.add(edge.source);

                    // Add source to queue to continue walking up
                    queue.push({ tableId: edge.source, handleId: edge.sourceHandle! });
                }
            });

            // Just wildcards? 
            // If the edge target was a wild card, we might need to trace broadly? 
            // For now assuming exact handle match.
        }

        // Apply Styles
        setLocalEdges(eds => eds.map(e => {
            const isTrace = upstreamEdges.has(e.id);
            return {
                ...e,
                animated: false,
                style: {
                    ...e.style,
                    stroke: isTrace ? '#60a5fa' : '#b1b1b7',
                    strokeWidth: isTrace ? 3 : 1,
                    opacity: isTrace ? 1 : 0.15
                },
                zIndex: isTrace ? 100 : 0
            };
        }));

        setLocalNodes(nds => nds.map(n => ({
            ...n,
            style: {
                ...n.style,
                opacity: upstreamNodes.has(n.id) ? 1 : 0.5,
                border: upstreamNodes.has(n.id) ? '2px solid #60a5fa' : '1px solid transparent'
            },
            data: {
                ...n.data,
                // Pass handler down again
                onColumnClick: handleColumnClick
            }
        })));

        setTraceActive(true);
    };

    const clearTrace = () => {
        setLocalEdges(edges.map(e => ({
            ...e,
            animated: false,
            style: {
                ...e.style,
                stroke: (e.data as any)?.color || e.style?.stroke || '#b1b1b7',
                strokeWidth: 2,
                opacity: 1
            },
            zIndex: 0
        })));
        setLocalNodes(nodes.map(n => ({
            ...n,
            style: { opacity: 1, border: 'none' },
            data: { ...n.data, onColumnClick: handleColumnClick }
        })));
        setTraceActive(false);
    };

    // Initialize nodes with click handler
    useEffect(() => {
        setLocalNodes(nds => nds.map(n => ({
            ...n,
            data: { ...n.data, onColumnClick: handleColumnClick }
        })));
    }, [nodes]); // Run when new graph loaded

    return (
        <div style={{ height: '100%', width: '100%', position: 'relative' }}>
            {traceActive && (
                <button
                    onClick={clearTrace}
                    style={{
                        position: 'absolute', top: 10, right: 10, zIndex: 10,
                        padding: '6px 12px', background: 'var(--vscode-button-background)', color: 'white', border: 'none', borderRadius: 4, cursor: 'pointer'
                    }}
                >
                    Clear Trace
                </button>
            )}
            <ReactFlow
                nodes={localNodes}
                edges={localEdges}
                nodeTypes={nodeTypes}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onEdgeMouseEnter={onEdgeMouseEnter}
                onEdgeMouseLeave={onEdgeMouseLeave}
                nodesDraggable={true}
                nodesConnectable={false}
                elementsSelectable={true}
                fitView
                minZoom={0.1}
                maxZoom={1.5}
            >
                <Background />
                <Controls />
            </ReactFlow>
        </div>
    );
};

// --- Script Results Types ---
interface ScriptStatementResultData {
    index: number;
    sql: string;
    status: 'success' | 'error' | 'skipped';
    kind?: 'tabular' | 'non_tabular';
    affectedRows?: number | null;
    rowCount?: number;
    elapsedMs?: number;
    errorMessage?: string;
}

interface ScriptExecutionResultData {
    mode: 'script';
    totalStatements: number;
    executedStatements: number;
    failedAtIndex?: number;
    statements: ScriptStatementResultData[];
    lastTabularResult?: GridData;
}

// --- Script Results View ---
const ScriptResultsView = ({
    data,
    theme,
    allowCsvExport
}: {
    data: ScriptExecutionResultData;
    theme: any;
    allowCsvExport: boolean;
}) => {
    const skippedCount = data.statements.filter(s => s.status === 'skipped').length;
    const successCount = data.statements.filter(s => s.status === 'success').length;
    const hasTabularResult = !!data.lastTabularResult;

    const formatDuration = (ms?: number) => {
        if (ms === undefined) return '';
        if (ms < 1000) return `${ms}ms`;
        return `${(ms / 1000).toFixed(1)}s`;
    };

    const truncateSql = (sql: string, maxLen = 120) => {
        const oneLine = sql.replace(/\s+/g, ' ').trim();
        if (oneLine.length <= maxLen) return oneLine;
        return oneLine.slice(0, maxLen) + '...';
    };

    const statusIcon = (status: string) => {
        switch (status) {
            case 'success': return '\u2705';
            case 'error': return '\u274C';
            case 'skipped': return '\u23ED';
            default: return '\u2B1C';
        }
    };

    return (
        <div className="script-results-container">
            <div className="script-summary">
                <span className="script-summary-text">
                    Script: {data.executedStatements}/{data.totalStatements} executed
                    {skippedCount > 0 && `, ${skippedCount} skipped`}
                    {data.failedAtIndex && ` \u2014 failed at statement ${data.failedAtIndex}`}
                </span>
                {!data.failedAtIndex && successCount === data.totalStatements && (
                    <span className="script-summary-success">{'\u2705'} All statements succeeded</span>
                )}
            </div>
            <div className={`script-checklist ${hasTabularResult ? 'script-checklist-with-grid' : 'script-checklist-full'}`}>
                {data.statements.map((stmt) => (
                    <div key={stmt.index} className={`script-stmt script-stmt-${stmt.status}`}>
                        <div className="script-stmt-header">
                            <span className="script-stmt-icon">{statusIcon(stmt.status)}</span>
                            <span className="script-stmt-index">#{stmt.index}</span>
                            <span className="script-stmt-sql">{truncateSql(stmt.sql)}</span>
                            <span className="script-stmt-meta">
                                {stmt.status === 'success' && stmt.kind === 'tabular' && stmt.rowCount !== undefined && (
                                    <span className="script-stmt-rows">{stmt.rowCount} rows</span>
                                )}
                                {stmt.status === 'success' && stmt.kind === 'non_tabular' && stmt.affectedRows != null && (
                                    <span className="script-stmt-rows">{stmt.affectedRows} affected</span>
                                )}
                                {stmt.elapsedMs !== undefined && (
                                    <span className="script-stmt-duration">{formatDuration(stmt.elapsedMs)}</span>
                                )}
                            </span>
                        </div>
                        {stmt.status === 'error' && stmt.errorMessage && (
                            <div className="script-stmt-error">{stmt.errorMessage}</div>
                        )}
                    </div>
                ))}
            </div>
            {hasTabularResult && (
                <div className="script-grid-section">
                    <div className="script-grid-label">Last query result:</div>
                    <div className="script-grid-wrapper">
                        <ResultsTab data={data.lastTabularResult!} theme={theme} onOpenChartBuilder={() => {}} allowCsvExport={allowCsvExport} />
                    </div>
                </div>
            )}
        </div>
    );
};

// --- Main App ---

const App = () => {
    const [activeTab, setActiveTab] = useState<'results' | 'charts'>('results');
    const [gridData, setGridData] = useState<GridData | null>(null);
    const [scriptData, setScriptData] = useState<ScriptExecutionResultData | null>(null);
    const [allowCsvExport, setAllowCsvExport] = useState<boolean>(true);
    const [erdData, setErdData] = useState<{ nodes: Node[], edges: Edge[], graphSignature?: string, layout?: any, connectionName?: string }>({ nodes: [], edges: [] });
    const [lineageData, setLineageData] = useState<{ nodes: Node[], edges: Edge[] }>({ nodes: [], edges: [] });
    const [chartConfig, setChartConfig] = useState<any>(null);
    const [explainData, setExplainData] = useState<{ markdown?: string, error?: string }>({});
    const [savePreview, setSavePreview] = useState<ResultsetEditsPreview | null>(null);
    const [saveExecuting, setSaveExecuting] = useState<boolean>(false);
    const [appNotice, setAppNotice] = useState<AppNotice | null>(null);

    // Check View Type
    const rootElement = document.getElementById('root');
    const viewType = rootElement?.getAttribute('data-view-type'); // 'erd' or 'lineage' or null (results)

    useEffect(() => {
        const handler = (event: MessageEvent) => {
            const message: MessageData = event.data;
            switch (message.command) {
                case 'setAllowCsvExport':
                    setAllowCsvExport(message.data);
                    break;
                case 'updateResults':
                    setGridData(message.data);
                    setScriptData(null);
                    setActiveTab('results');
                    setSavePreview(null);
                    setSaveExecuting(false);
                    break;
                case 'updateScriptResults':
                    setScriptData(message.data);
                    setGridData(null);
                    setActiveTab('results');
                    setSavePreview(null);
                    setSaveExecuting(false);
                    break;
                case 'clearResults':
                    setGridData(null);
                    setScriptData(null);
                    setSavePreview(null);
                    setSaveExecuting(false);
                    break;
                case 'showERD':
                    setErdData(message.data);
                    break;
                case 'showLineage':
                    setLineageData(message.data);
                    break;
                case 'explainStart':
                    setExplainData({});
                    break;
                case 'explainResult':
                    setExplainData({ markdown: message.data.markdown });
                    break;
                case 'explainError':
                    setExplainData({ error: message.data.error });
                    break;
                case 'chartConfigSaved':
                    if (message.data.ok) {
                        vscode.postMessage({ command: 'loadChartConfig' });
                    } else {
                        setAppNotice({
                            title: 'Chart Save Failed',
                            message: `Failed to save chart config: ${message.data.error}`
                        });
                    }
                    break;
                case 'chartConfigLoaded':
                    setChartConfig(message.data.config);
                    break;
                case 'applyResultsetEditsPreview':
                    setActiveTab('results');
                    setSavePreview(message.data);
                    setSaveExecuting(false);
                    break;
                case 'applyResultsetEditsResult':
                    setSavePreview(null);
                    setSaveExecuting(false);
                    if (!message.data) break;
                    if (message.data.ok) {
                        const summary = message.data.summary || {};
                        if ((summary.applied || 0) > 0) {
                            vscode.postMessage({ command: 'viewReady' });
                        }
                    } else {
                        const firstError = message.data.rowResults?.find((r: any) => r.status !== 'applied');
                        if (firstError?.message) {
                            setAppNotice({
                                title: 'Save Changes Failed',
                                message: firstError.message
                            });
                        }
                    }
                    break;
            }
        };
        window.addEventListener('message', handler);
        vscode.postMessage({ command: 'viewReady' });
        return () => window.removeEventListener('message', handler);
    }, [viewType]);

    // View Routing
    if (viewType === 'erd') {
        return (
            <div className="app-container">
                <div className="content" style={{ height: '100vh' }}>
                    <ERDTab nodes={erdData.nodes} edges={erdData.edges} graphSignature={erdData.graphSignature} layout={erdData.layout} connectionName={erdData.connectionName} />
                </div>
            </div>
        );
    }

    if (viewType === 'lineage') {
        return (
            <div className="app-container">
                <div className="content" style={{ height: '100vh' }}>
                    <LineageGraph nodes={lineageData.nodes} edges={lineageData.edges} />
                </div>
            </div>
        );
    }

    if (viewType === 'explain') {
        return (
            <div className="app-container">
                <div className="content" style={{ height: '100vh' }}>
                    <ExplainTab markdown={explainData.markdown || null} error={explainData.error || null} />
                </div>
            </div>
        );
    }

    // --- Theme Detection ---
    const [activeTheme, setActiveTheme] = useState(myThemeDark);

    useEffect(() => {
        const updateTheme = () => {
            const isDark = document.body.classList.contains('vscode-dark') || document.body.classList.contains('vscode-high-contrast');
            // If the body class isn't set yet (rare), fallback to dark, or check prefers-color-scheme
            // But VS Code webviews usually interpret themes via these classes
            if (document.body.classList.contains('vscode-light')) {
                setActiveTheme(myThemeLight);
            } else if (isDark) {
                setActiveTheme(myThemeDark);
            } else {
                // Default fallback
                setActiveTheme(myThemeDark);
            }
        };

        // Initial check
        updateTheme();

        // Observe class changes on body
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                if (mutation.type === 'attributes' && mutation.attributeName === 'class') {
                    updateTheme();
                }
            });
        });

        observer.observe(document.body, { attributes: true });

        return () => observer.disconnect();
    }, []);

    // Load chart config when switching to Charts tab
    useEffect(() => {
        if (activeTab === 'charts') {
            vscode.postMessage({ command: 'loadChartConfig' });
        }
    }, [activeTab]);

    useEffect(() => {
        if (!savePreview) return;
        const onKeydown = (event: KeyboardEvent) => {
            if (event.key === 'Escape' && !saveExecuting) {
                setSavePreview(null);
            }
        };
        window.addEventListener('keydown', onKeydown);
        return () => window.removeEventListener('keydown', onKeydown);
    }, [savePreview, saveExecuting]);

    const handleCancelSave = () => {
        if (saveExecuting) return;
        setSavePreview(null);
    };

    const handleConfirmSave = () => {
        if (!savePreview || saveExecuting) return;
        setSaveExecuting(true);
        vscode.postMessage({
            command: 'applyResultsetEdits',
            data: {
                ...savePreview.request,
                confirmed: true
            }
        });
    };

    useEffect(() => {
        if (!appNotice) return;
        const onKeydown = (event: KeyboardEvent) => {
            if (event.key === 'Escape') {
                setAppNotice(null);
            }
        };
        window.addEventListener('keydown', onKeydown);
        return () => window.removeEventListener('keydown', onKeydown);
    }, [appNotice]);

    // Normal mode with tabs
    return (
        <div className="app-container">
            <div className="tabs">
                <button className={`tab ${activeTheme === myThemeLight ? 'light-tab' : ''} ${activeTab === 'results' ? 'active' : ''}`} onClick={() => setActiveTab('results')}>Results</button>
                <button className={`tab ${activeTheme === myThemeLight ? 'light-tab' : ''} ${activeTab === 'charts' ? 'active' : ''}`} onClick={() => setActiveTab('charts')}>Charts</button>
            </div>
            <div className="content">
                {activeTab === 'results' && scriptData && <ScriptResultsView data={scriptData} theme={activeTheme} allowCsvExport={allowCsvExport} />}
                {activeTab === 'results' && !scriptData && <ResultsTab data={gridData} theme={activeTheme} onOpenChartBuilder={() => setActiveTab('charts')} allowCsvExport={allowCsvExport} />}
                {activeTab === 'charts' && (
                    <ChartsTab
                        config={chartConfig}
                        data={gridData}
                        onNotify={(title, message) => setAppNotice({ title, message })}
                    />
                )}
            </div>
            <SaveConfirmModal
                preview={savePreview}
                executing={saveExecuting}
                onCancel={handleCancelSave}
                onConfirm={handleConfirmSave}
            />
            <AppNoticeModal
                notice={appNotice}
                onClose={() => setAppNotice(null)}
            />
        </div>
    );
};

const root = createRoot(document.getElementById('root')!);
root.render(
    <ErrorBoundary>
        <App />
    </ErrorBoundary>
);
