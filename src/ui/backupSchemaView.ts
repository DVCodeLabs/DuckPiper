import * as vscode from 'vscode';
import { BackupOptions } from '../core/backupSchemaSql';

export interface BackupSchemaContext {
    connectionId: string;
    connectionName: string;
    schemaName: string;
    dialect: string;
    hasViews: boolean;
    hasRoutines: boolean;
    isLocalDuckDB?: boolean;
    defaultFilePath: string;
}

export class BackupSchemaView {
    public static currentPanel: BackupSchemaView | undefined;

    private readonly panel: vscode.WebviewPanel;
    private readonly extensionUri: vscode.Uri;
    private readonly panelContext: BackupSchemaContext;
    private readonly onExecute: (filePath: string, options: BackupOptions) => Promise<void>;
    private readonly disposables: vscode.Disposable[] = [];

    private constructor(
        panel: vscode.WebviewPanel,
        extensionUri: vscode.Uri,
        panelContext: BackupSchemaContext,
        onExecute: (filePath: string, options: BackupOptions) => Promise<void>
    ) {
        this.panel = panel;
        this.extensionUri = extensionUri;
        this.panelContext = panelContext;
        this.onExecute = onExecute;

        this.panel.onDidDispose(() => this.dispose(), null, this.disposables);
        this.panel.webview.html = this.getWebviewContent(this.panel.webview, this.extensionUri);
        this.setMessageListener(this.panel.webview);
    }

    public static render(
        extensionUri: vscode.Uri,
        panelContext: BackupSchemaContext,
        onExecute: (filePath: string, options: BackupOptions) => Promise<void>
    ): void {
        if (BackupSchemaView.currentPanel) {
            BackupSchemaView.currentPanel.dispose();
        }

        const panel = vscode.window.createWebviewPanel(
            'backupSchema',
            `Backup: ${panelContext.schemaName}`,
            vscode.ViewColumn.One,
            {
                enableScripts: true,
                retainContextWhenHidden: true,
                localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'dist')]
            }
        );

        BackupSchemaView.currentPanel = new BackupSchemaView(panel, extensionUri, panelContext, onExecute);
    }

    public dispose(): void {
        BackupSchemaView.currentPanel = undefined;
        this.panel.dispose();
        while (this.disposables.length) {
            const d = this.disposables.pop();
            if (d) d.dispose();
        }
    }

    private setMessageListener(webview: vscode.Webview): void {
        webview.onDidReceiveMessage(
            async (message: Record<string, unknown>) => {
                const command = message?.command;

                if (command === 'ready') {
                    webview.postMessage({ command: 'setContext', data: this.panelContext });
                    return;
                }

                if (command === 'cancel') {
                    this.dispose();
                    return;
                }

                if (command === 'browse') {
                    const currentPath = message?.currentPath as string | undefined;
                    let defaultUri: vscode.Uri | undefined;
                    if (currentPath) {
                        try { defaultUri = vscode.Uri.file(currentPath); } catch { /* ignore */ }
                    }
                    if (!defaultUri) {
                        defaultUri = vscode.Uri.file(this.panelContext.defaultFilePath);
                    }

                    const uri = await vscode.window.showSaveDialog({
                        saveLabel: 'Select',
                        filters: { 'SQL Files': ['sql'] },
                        defaultUri
                    });

                    if (uri) {
                        webview.postMessage({ command: 'filePath', path: uri.fsPath });
                    }
                    return;
                }

                if (command === 'execute') {
                    const { filePath, options } = (message?.data as Record<string, unknown>) ?? {};
                    if (!filePath || !options) {
                        webview.postMessage({
                            command: 'backupResult',
                            data: { ok: false, message: 'Missing file path or options.' }
                        });
                        return;
                    }

                    try {
                        await this.onExecute(filePath as string, options as BackupOptions);
                        webview.postMessage({
                            command: 'backupResult',
                            data: { ok: true, message: 'Backup complete!' }
                        });
                    } catch (error: unknown) {
                        webview.postMessage({
                            command: 'backupResult',
                            data: {
                                ok: false,
                                message: error instanceof Error ? error.message : 'Backup failed.'
                            }
                        });
                    }
                    return;
                }
            },
            undefined,
            this.disposables
        );
    }

    private getWebviewContent(webview: vscode.Webview, extensionUri: vscode.Uri): string {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'dist', 'backupSchemaApp.js'));
        const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'dist', 'backupSchemaApp.css'));

        return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src ${webview.cspSource} 'unsafe-inline';">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="stylesheet" href="${styleUri}">
  <title>Backup Schema</title>
</head>
<body>
  <div id="root"></div>
  <script src="${scriptUri}"></script>
</body>
</html>`;
    }
}
