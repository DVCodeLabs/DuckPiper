import React, { useState, useEffect } from 'react';
import { createRoot } from 'react-dom/client';
import { NotebookData, NotebookCell, CellResult } from '../notebookTypes';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ColDef, ModuleRegistry, themeBalham, colorSchemeDark } from 'ag-grid-community';

// Register AG Grid modules
ModuleRegistry.registerModules([AllCommunityModule]);

declare const acquireVsCodeApi: any;
const vscode = acquireVsCodeApi();

// Theme setup (re-using logic from results view style)
const gridTheme = themeBalham.withPart(colorSchemeDark).withParams({
    fontFamily: "var(--vscode-editor-font-family)",
    backgroundColor: "transparent",
    headerBackgroundColor: "var(--vscode-editor-background)",
    rowHoverColor: "var(--vscode-list-hoverBackground)",
    oddRowBackgroundColor: "var(--vscode-editor-background)",
    foregroundColor: "var(--vscode-editor-foreground)",
    headerTextColor: "var(--vscode-editor-foreground)",
    spacing: 4,
    cellHorizontalPadding: 8
});

const CellComponent = ({ cell, onRun, result, onChange }: {
    cell: NotebookCell,
    onRun: (id: string) => void,
    result?: CellResult,
    onChange: (id: string, updates: Partial<NotebookCell>) => void
}) => {
    const isSql = cell.type === 'sql';
    const [code, setCode] = useState(cell.sql || cell.content || '');

    useEffect(() => {
        setCode(cell.sql || cell.content || '');
    }, [cell.sql, cell.content]);

    const handleRun = () => onRun(cell.id);

    const handleCodeChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
        const val = e.target.value;
        setCode(val);
        onChange(cell.id, isSql ? { sql: val } : { content: val });
    };

    // Derived badge color
    const badgeColor = {
        bronze: '#CD7F32',
        silver: '#C0C0C0',
        gold: '#FFD700',
        dp_app: '#888888'
    }[cell.layer || 'dp_app'] || '#888888';

    const colDefs: ColDef[] = result?.head && result.head.length > 0
        ? Object.keys(result.head[0]).map(k => ({ field: k, headerName: k, filter: true, sortable: true }))
        : [];

    return (
        <div style={{ marginBottom: '24px', border: '1px solid var(--vscode-widget-border)', borderRadius: '4px', overflow: 'hidden' }}>
            {/* Header */}
            <div style={{ padding: '8px 12px', background: 'var(--vscode-editor-lineHighlightBackground)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                    {isSql && (
                        <span style={{
                            background: badgeColor, color: '#000', padding: '2px 8px', borderRadius: '10px',
                            fontSize: '11px', fontWeight: 'bold', textTransform: 'uppercase'
                        }}>
                            {cell.layer || 'internal'}
                        </span>
                    )}
                    <span style={{ fontWeight: 600, fontSize: '13px' }}>
                        {isSql ? `${cell.materialize === 'view' ? 'VIEW' : 'TABLE'}: ${cell.outputName || 'untitled'}` : 'Markdown'}
                    </span>
                </div>
                <div style={{ display: 'flex', gap: '8px' }}>
                    <button
                        onClick={handleRun}
                        style={{
                            background: 'var(--vscode-button-background)',
                            color: 'var(--vscode-button-foreground)',
                            border: 'none', padding: '4px 12px', cursor: 'pointer', borderRadius: '2px'
                        }}
                    >
                        Run
                    </button>
                </div>
            </div>

            {/* Editor Area */}
            <div style={{ padding: '0' }}>
                <textarea
                    value={code}
                    onChange={handleCodeChange}
                    spellCheck={false}
                    style={{
                        width: '100%', minHeight: '100px', background: 'var(--vscode-editor-background)',
                        color: 'var(--vscode-editor-foreground)', border: 'none', padding: '12px',
                        fontFamily: 'var(--vscode-editor-font-family)', fontSize: 'var(--vscode-editor-font-size)',
                        resize: 'vertical', outline: 'none', whiteSpace: 'pre'
                    }}
                />
            </div>

            {/* Result Area */}
            {result && (
                <div style={{ borderTop: '1px solid var(--vscode-widget-border)', padding: '0' }}>
                    {result.error ? (
                        <div style={{ padding: '12px', color: 'var(--vscode-errorForeground)', fontFamily: 'monospace' }}>
                            Error: {result.error}
                        </div>
                    ) : (
                        <div style={{ height: '300px' }}>
                            {/* Stats bar */}
                            <div style={{ padding: '4px 12px', fontSize: '11px', color: 'var(--vscode-descriptionForeground)', borderBottom: '1px solid var(--vscode-widget-border)' }}>
                                {result.rowCount} rows • {result.executionTimeMs}ms
                            </div>
                            {/* Grid */}
                            <AgGridReact
                                theme={gridTheme}
                                rowData={result.head}
                                columnDefs={colDefs}
                                defaultColDef={{ flex: 1, minWidth: 100 }}
                            />
                        </div>
                    )}
                </div>
            )}
        </div>
    );
};

const NotebookApp = () => {
    const [notebook, setNotebook] = useState<NotebookData | null>(null);
    const [results, setResults] = useState<Record<string, CellResult>>({});

    useEffect(() => {
        const handler = (event: MessageEvent) => {
            const msg = event.data;
            if (msg.type === 'load') {
                setNotebook(msg.data);
            } else if (msg.type === 'cellResult') {
                setResults(prev => ({ ...prev, [msg.result.cellId]: msg.result }));
            }
        };
        window.addEventListener('message', handler);
        // Signal ready
        vscode.postMessage({ type: 'ready' });
        return () => window.removeEventListener('message', handler);
    }, []);

    const handleRunCell = (cellId: string) => {
        vscode.postMessage({ type: 'runCell', cellId });
    };

    const handleCellChange = (cellId: string, updates: Partial<NotebookCell>) => {
        if (!notebook) return;
        const updatedCells = notebook.cells.map(c => c.id === cellId ? { ...c, ...updates } : c);
        const updatedNotebook = { ...notebook, cells: updatedCells };
        setNotebook(updatedNotebook);
        // Send update to host to save file
        vscode.postMessage({ type: 'update', data: updatedNotebook });
    };

    if (!notebook) return <div style={{ padding: 20 }}>Loading notebook...</div>;

    return (
        <div style={{ padding: '20px', maxWidth: '1200px', margin: '0 auto' }}>
            <h1 style={{ fontSize: '24px', marginBottom: '8px' }}>{notebook.name}</h1>
            <div style={{ marginBottom: '24px', opacity: 0.7 }}>
                Engine: Local DuckDB • {notebook.cells.length} cells
            </div>

            {notebook.cells.map(cell => (
                <CellComponent
                    key={cell.id}
                    cell={cell}
                    onRun={handleRunCell}
                    result={results[cell.id]}
                    onChange={handleCellChange}
                />
            ))}
        </div>
    );
};

const root = createRoot(document.getElementById('root')!);
root.render(<NotebookApp />);
