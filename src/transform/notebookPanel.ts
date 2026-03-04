import * as vscode from 'vscode';
import * as crypto from 'crypto';
import * as fs from 'fs/promises';
import { NotebookData, CellResult } from './notebookTypes';
import { LocalDuckDB } from './localDuckDB';

export class NotebookPanel {
    public static currentPanel: NotebookPanel | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private readonly _fileUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];
    private _data: NotebookData | null = null;

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri, fileUri: vscode.Uri) {
        this._panel = panel;
        this._extensionUri = extensionUri;
        this._fileUri = fileUri;

        // Set content
        this._panel.webview.html = this._getHtmlForWebview(this._panel.webview);

        // Listen for disposal
        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

        // Message listener
        this._panel.webview.onDidReceiveMessage(
            async (message) => {
                switch (message.type) {
                    case 'ready':
                        await this.loadFile();
                        break;
                    case 'update':
                        await this.saveFile(message.data);
                        break;
                    case 'runCell':
                        await this.runCell(message.cellId);
                        break;
                }
            },
            null,
            this._disposables
        );
    }

    public static createOrShow(extensionUri: vscode.Uri, fileUri: vscode.Uri) {
        const column = vscode.window.activeTextEditor
            ? vscode.window.activeTextEditor.viewColumn
            : undefined;

        // If we already have a panel for this file, show it
        // (Simple MVP: single panel limitation or check title?)
        if (NotebookPanel.currentPanel && NotebookPanel.currentPanel._fileUri.toString() === fileUri.toString()) {
            NotebookPanel.currentPanel._panel.reveal(column);
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'dpNotebook',
            'DuckPiper Notebook',
            column || vscode.ViewColumn.One,
            {
                enableScripts: true,
                localResourceRoots: [
                    vscode.Uri.joinPath(extensionUri, 'dist'),
                    vscode.Uri.joinPath(extensionUri, 'media')
                ]
            }
        );

        NotebookPanel.currentPanel = new NotebookPanel(panel, extensionUri, fileUri);
    }

    private async loadFile() {
        try {
            const content = await fs.readFile(this._fileUri.fsPath, 'utf-8');
            this._data = JSON.parse(content);
            this._panel.title = this._data?.name || "Notebook";
            this._panel.webview.postMessage({ type: 'load', data: this._data });
        } catch (e) {
            vscode.window.showErrorMessage(`Failed to load notebook: ${e}`);
        }
    }

    private async saveFile(data: NotebookData) {
        this._data = data;
        try {
            await fs.writeFile(this._fileUri.fsPath, JSON.stringify(data, null, 2), 'utf-8');
            // vscode.window.setStatusBarMessage("Notebook saved", 2000);
        } catch (e) {
            vscode.window.showErrorMessage(`Failed to save notebook: ${e}`);
        }
    }

    private async runCell(cellId: string) {
        if (!this._data) return;
        const cell = this._data.cells.find(c => c.id === cellId);
        if (!cell || cell.type !== 'sql') return;

        // Execute logic
        try {
            const start = Date.now();
            const db = LocalDuckDB.getInstance();
            // Ensure schemas exist just in case
            await db.initialize();

            // Construct SQL
            const layer = cell.layer || 'dp_app';
            const mat = cell.materialize || 'table';
            const table = cell.outputName || `cell_${cell.id.substring(0, 8)}`;
            const target = `${layer}.${table}`;

            // "CREATE OR REPLACE TABLE x AS ..."
            const transformSql = `CREATE OR REPLACE ${mat === 'view' ? 'VIEW' : 'TABLE'} ${target} AS ${cell.sql}`;

            // Execute Transform
            await this.runSql(transformSql);

            // Preview
            const limit = vscode.workspace.getConfiguration('dp').get<number>('transform.previewRows', 200);
            const previewSql = `SELECT * FROM ${target} LIMIT ${limit}`;
            const rows = await this.runSql(previewSql);

            const result: CellResult = {
                cellId,
                success: true,
                rowCount: rows.length, // approximate if limited
                head: rows,
                executionTimeMs: Date.now() - start
            };

            this._panel.webview.postMessage({ type: 'cellResult', result });

        } catch (e: unknown) {
            const msg = e instanceof Error ? e.message : String(e);
            this._panel.webview.postMessage({
                type: 'cellResult',
                result: { cellId, success: false, error: msg }
            });
        }
    }

    private runSql(sql: string): Promise<Record<string, unknown>[]> {
        return LocalDuckDB.getInstance().query(sql);
    }

    public dispose() {
        NotebookPanel.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) x.dispose();
        }
    }

    private _getHtmlForWebview(webview: vscode.Webview) {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'dist', 'notebookApp.js'));
        const _styleUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'dist', 'notebookApp.css')); // if generated

        // Use a nonce to whitelist which scripts can be run
        const nonce = getNonce();

        return `<!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; font-src ${webview.cspSource} data:;">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>DuckPiper Notebook</title>
                <style>
                    body {
                        padding: 0;
                        margin: 0;
                        background-color: var(--vscode-editor-background);
                        color: var(--vscode-editor-foreground);
                        font-family: var(--vscode-font-family);
                    }
                </style>
            </head>
            <body>
                <div id="root"></div>
                <script nonce="${nonce}" src="${scriptUri}"></script>
            </body>
            </html>`;
    }
}

function getNonce() {
    return crypto.randomBytes(16).toString('hex');
}
