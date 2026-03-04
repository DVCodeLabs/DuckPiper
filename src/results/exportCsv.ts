
import * as vscode from 'vscode';
import { QueryResult } from '../core/types';

export async function exportToCsv(data: QueryResult): Promise<void> {
    if (!data || !data.rows || data.rows.length === 0) {
        vscode.window.showWarningMessage("No results to export.");
        return;
    }

    const uri = await vscode.window.showSaveDialog({
        filters: { 'CSV': ['csv'] },
        saveLabel: 'Export CSV'
    });

    if (!uri) return;

    try {
        const csvContent = convertToCsv(data);
        await vscode.workspace.fs.writeFile(uri, Buffer.from(csvContent, 'utf8'));
        vscode.window.showInformationMessage(`Exported ${data.rowCount} rows to CSV.`);
    } catch (e: unknown) {
        vscode.window.showErrorMessage(`Export failed: ${e instanceof Error ? e.message : String(e)}`);
    }
}

function convertToCsv(data: QueryResult): string {
    const header = data.columns.map(c => `"${escapeCsv(c.name)}"`).join(',');
    const rows = data.rows.map(row => {
        // If objects:
        return data.columns.map(c => {
            const val = (row as Record<string, unknown>)[c.name];
            return `"${escapeCsv(val)}"`;
        }).join(',');
    });

    return [header, ...rows].join('\n');
}

function escapeCsv(val: unknown): string {
    if (val === null || val === undefined) return '';
    const s = String(val);
    return s.replace(/"/g, '""'); // escape double quotes
}
