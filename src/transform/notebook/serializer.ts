import * as vscode from 'vscode';
import { TextDecoder, TextEncoder } from 'util';
import * as crypto from 'crypto';
import { NotebookData as JsonNotebookData, NotebookCell as JsonCell } from '../notebookTypes';

export class DPNotebookSerializer implements vscode.NotebookSerializer {
    async deserializeNotebook(
        content: Uint8Array,
        _token: vscode.CancellationToken
    ): Promise<vscode.NotebookData> {
        const contents = new TextDecoder().decode(content);

        let raw: JsonNotebookData;
        try {
            raw = JSON.parse(contents);
        } catch {
            raw = { version: "0.1", name: "New Notebook", cells: [] };
        }

        const cells = raw.cells.map(item => {
            const kind = item.type === 'sql' ? vscode.NotebookCellKind.Code : vscode.NotebookCellKind.Markup;
            const language = item.type === 'sql' ? 'sql' : 'markdown';
            const text = item.type === 'sql' ? (item.sql || '') : (item.content || '');

            const cell = new vscode.NotebookCellData(kind, text, language);

            // Store DuckPiper specific metadata
            cell.metadata = { dp: { ...item } };
            // Remove 'sql'/'content' duplication in metadata to save space if desired, but keep for simplicity

            return cell;
        });

        const data = new vscode.NotebookData(cells);
        data.metadata = { dp: { name: raw.name, version: raw.version, engine: raw.engine } };

        return data;
    }

    async serializeNotebook(
        data: vscode.NotebookData,
        _token: vscode.CancellationToken
    ): Promise<Uint8Array> {
        const cells: JsonCell[] = data.cells.map(cell => {
            const dpMeta = cell.metadata?.dp || {};

            // Base object
            const jsonCell: JsonCell = {
                id: dpMeta.id || crypto.randomUUID(), // Ensure ID
                type: cell.kind === vscode.NotebookCellKind.Code ? 'sql' : 'markdown',
                ...dpMeta // spread existing metadata
            };

            // Update content from editor state
            if (jsonCell.type === 'sql') {
                jsonCell.sql = cell.value;
                delete jsonCell.content;
            } else {
                jsonCell.content = cell.value;
                delete jsonCell.sql;
            }

            return jsonCell;
        });

        const dpGlobal = data.metadata?.dp || {};
        const notebookJson: JsonNotebookData = {
            version: dpGlobal.version || "0.1",
            name: dpGlobal.name || "Notebook",
            engine: dpGlobal.engine,
            cells: cells
        };

        return new TextEncoder().encode(JSON.stringify(notebookJson, null, 2));
    }
}
