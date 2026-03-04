import React, { useState, useEffect } from 'react';
import { ChartConfig, DatasetStyle } from '../core/types';

// Supported Chart Types
const CHART_TYPES = ['bar', 'line', 'pie', 'doughnut', 'scatter', 'radar'];

// Default color palette
const DEFAULT_COLORS = [
    '#3592eb', // Blue
    '#ff6384', // Red
    '#4bc0c0', // Teal
    '#ffce56', // Yellow
    '#9966ff', // Purple
    '#ff9f40', // Orange
    '#c9cbcf', // Gray
    '#7cbb00', // Green
];

interface ChartBuilderProps {
    config: ChartConfig | null;
    columns: any[]; // ag-grid col definitions or similar
    onBuild: (config: ChartConfig) => void;
    onCancel?: () => void;
    onValidationError?: (message: string) => void;
}

export const ChartBuilder = ({ config, columns, onBuild, onCancel, onValidationError }: ChartBuilderProps) => {
    // Local state for form
    const [type, setType] = useState<string>(config?.type || 'bar');
    const [title, setTitle] = useState<string>(config?.title || '');
    const [labelColumn, setLabelColumn] = useState<string>(config?.labelColumn || '');
    const [datasetColumns, setDatasetColumns] = useState<string[]>(config?.datasetColumns || []);
    const [datasetColors, setDatasetColors] = useState<Record<string, string>>({});

    useEffect(() => {
        if (config) {
            setType(config.type);
            setTitle(config.title || '');
            setLabelColumn(config.labelColumn || '');
            setDatasetColumns(config.datasetColumns || []);
            // Restore colors from datasetStyles
            if (config.datasetStyles) {
                const colors: Record<string, string> = {};
                Object.entries(config.datasetStyles).forEach(([colName, style]) => {
                    if (style.backgroundColor) {
                        colors[colName] = style.backgroundColor;
                    }
                });
                setDatasetColors(colors);
            }
        } else {
            // Reset or Default
            if (columns.length > 0) {
                // Try to pick sensible defaults
                const firstString = columns.find(c => ['string', 'varchar', 'text'].some(t => c.type?.toLowerCase().includes(t)))?.name || columns[0].name;
                setLabelColumn(firstString);
            }
        }
    }, [config, columns]);

    const handleDatasetToggle = (colName: string) => {
        if (datasetColumns.includes(colName)) {
            setDatasetColumns(datasetColumns.filter(c => c !== colName));
            // Remove color when deselected
            const newColors = { ...datasetColors };
            delete newColors[colName];
            setDatasetColors(newColors);
        } else {
            setDatasetColumns([...datasetColumns, colName]);
            // Assign default color
            const colorIndex = datasetColumns.length % DEFAULT_COLORS.length;
            setDatasetColors({ ...datasetColors, [colName]: DEFAULT_COLORS[colorIndex] });
        }
    };

    const handleColorChange = (colName: string, color: string) => {
        setDatasetColors({ ...datasetColors, [colName]: color });
    };

    const handleBuild = () => {
        // Validation
        if (!type) return;
        if (datasetColumns.length === 0) {
            onValidationError?.("Please select at least one dataset column.");
            return;
        }

        // Build datasetStyles as Record<string, DatasetStyle>
        const datasetStyles: Record<string, DatasetStyle> = {};
        datasetColumns.forEach((col, idx) => {
            datasetStyles[col] = {
                backgroundColor: datasetColors[col] || DEFAULT_COLORS[idx % DEFAULT_COLORS.length],
                borderColor: datasetColors[col] || DEFAULT_COLORS[idx % DEFAULT_COLORS.length],
            };
        });

        const newConfig: ChartConfig = {
            type: type as any,
            title,
            labelColumn,
            datasetColumns,
            datasetStyles
        };
        onBuild(newConfig);
    };

    return (
        <div style={{ padding: '20px', display: 'flex', flexDirection: 'column', gap: '15px', color: 'var(--vscode-editor-foreground)' }}>
            <h3>Chart Configuration</h3>

            <div style={{ display: 'grid', gridTemplateColumns: '120px 1fr', gap: '10px', alignItems: 'center' }}>
                <label>Chart Type</label>
                <select
                    value={type}
                    onChange={e => setType(e.target.value)}
                    style={{ background: 'var(--vscode-dropdown-background)', color: 'var(--vscode-dropdown-foreground)', border: '1px solid var(--vscode-dropdown-border)', padding: '5px' }}
                >
                    {CHART_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>

                <label>Title</label>
                <input
                    type="text"
                    value={title}
                    onChange={e => setTitle(e.target.value)}
                    placeholder="Chart Title"
                    style={{ background: 'var(--vscode-input-background)', color: 'var(--vscode-input-foreground)', border: '1px solid var(--vscode-input-border)', padding: '5px' }}
                />

                <label>X Axis (Labels)</label>
                <select
                    value={labelColumn}
                    onChange={e => setLabelColumn(e.target.value)}
                    style={{ background: 'var(--vscode-dropdown-background)', color: 'var(--vscode-dropdown-foreground)', border: '1px solid var(--vscode-dropdown-border)', padding: '5px' }}
                >
                    {columns.map(c => <option key={c.name} value={c.name}>{c.name} {c.type && c.type !== 'unknown' ? `(${c.type})` : ''}</option>)}
                </select>

                <label style={{ alignSelf: 'start', paddingTop: '5px' }}>Y Axis (Data)</label>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '5px', maxHeight: '200px', overflowY: 'auto', border: '1px solid var(--vscode-widget-border)', padding: '5px', borderRadius: '4px' }}>
                    {columns.map(c => (
                        <div key={c.name} style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                            <input
                                type="checkbox"
                                checked={datasetColumns.includes(c.name)}
                                onChange={() => handleDatasetToggle(c.name)}
                            />
                            <span style={{ flex: 1 }}>
                                {c.name} <span style={{ opacity: 0.7, fontSize: '0.9em' }}>{c.type && c.type !== 'unknown' ? `(${c.type})` : ''}</span>
                            </span>
                            {datasetColumns.includes(c.name) && (
                                <input
                                    type="color"
                                    value={datasetColors[c.name] || DEFAULT_COLORS[datasetColumns.indexOf(c.name) % DEFAULT_COLORS.length]}
                                    onChange={e => handleColorChange(c.name, e.target.value)}
                                    style={{ width: '30px', height: '24px', border: 'none', cursor: 'pointer', background: 'transparent' }}
                                    title="Select color"
                                />
                            )}
                        </div>
                    ))}
                </div>
            </div>

            <div style={{ marginTop: '10px', display: 'flex', gap: '10px' }}>
                {onCancel && (
                    <button
                        onClick={onCancel}
                        style={{
                            background: 'var(--vscode-button-secondaryBackground)',
                            color: 'var(--vscode-button-secondaryForeground)',
                            border: 'none',
                            padding: '8px 16px',
                            cursor: 'pointer',
                            borderRadius: '4px'
                        }}
                    >
                        Cancel
                    </button>
                )}
                <button
                    onClick={handleBuild}
                    style={{
                        background: 'var(--vscode-button-background)',
                        color: 'var(--vscode-button-foreground)',
                        border: 'none',
                        padding: '8px 16px',
                        cursor: 'pointer',
                        borderRadius: '4px'
                    }}
                >
                    Build & Save Chart
                </button>
            </div>
        </div>
    );
};
