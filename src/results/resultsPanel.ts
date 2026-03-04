
import * as vscode from 'vscode';
import * as utils from 'path'; // using path as utils to avoid shadowing
import { fileExists } from '../core/fsWorkspace';
import { QueryResult } from '../core/types';
import { Logger } from '../core/logger';
import { ErrorHandler, ErrorSeverity, formatGeneralError } from '../core/errorHandler';

export class ResultsPanel {
    // Map of document URI (string) -> ResultsPanel instance
    public static panels = new Map<string, ResultsPanel>();

    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private readonly _docUri: string; // The key
    private _disposables: vscode.Disposable[] = [];

    // state
    private _lastResult?: QueryResult;
    private _allowCsvExport?: boolean;

    public get lastResult(): QueryResult | undefined {
        return this._lastResult;
    }

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri, docUri: string) {
        this._panel = panel;
        this._extensionUri = extensionUri;
        this._docUri = docUri;

        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);

        // Handle messages from webview
        this._panel.webview.onDidReceiveMessage(
            message => {
                switch (message.command) {
                    case 'exportCsv':
                        if (this._allowCsvExport === false) {
                            vscode.window.showInformationMessage('CSV export is not available for this connection.');
                            return;
                        }
                        if (this._lastResult) {
                            const { exportToCsv } = require('./exportCsv');
                            exportToCsv(this._lastResult);
                        } else {
                            vscode.window.showWarningMessage('No results to export.');
                        }
                        return;

                    case 'generateChart':
                        vscode.commands.executeCommand('dp.chart.createFromResults', this._docUri);
                        return;

                    case 'viewReady':
                        if (this._allowCsvExport !== undefined) {
                            this._panel.webview.postMessage({ command: 'setAllowCsvExport', data: this._allowCsvExport });
                        }
                        if (this._lastResult) {
                            // Replay the last result to the fresh view
                            this._panel.webview.postMessage({ command: 'updateResults', data: this._lastResult });
                        }
                        return;

                    case 'cacheLoadedRows':
                        // data is gridData (rows, columns)
                        // We might want to attach connId if we have it
                        vscode.commands.executeCommand('dp.cache.saveLoadedResultsToDuckDBCache', message.data);
                        return;

                    case 'cacheRerunQuery':
                        vscode.commands.executeCommand('dp.cache.saveQueryResultsToDuckDBCache');
                        return;

                    case 'loadChartConfig':
                        this._loadChartConfig();
                        return;

                    case 'saveChartConfig':
                        this._saveChartConfig(message.data?.config);
                        return;

                    case 'applyResultsetEdits':
                        vscode.commands.executeCommand('dp.results.applyEdits', vscode.Uri.parse(this._docUri), message.data);
                        return;
                }
            },
            null,
            this._disposables
        );

        this._panel.webview.html = this._getWebviewContent(this._panel.webview);
    }

    public static render(extensionUri: vscode.Uri, docUri: vscode.Uri, title: string) {
        const key = docUri.toString();

        if (ResultsPanel.panels.has(key)) {
            ResultsPanel.panels.get(key)?._panel.reveal(vscode.ViewColumn.Beside);
        } else {
            const panel = vscode.window.createWebviewPanel(
                'dpResults',
                title || 'Data Results',
                vscode.ViewColumn.Beside, // Split view by default
                {
                    enableScripts: true,
                    localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'dist')]
                }
            );

            const instance = new ResultsPanel(panel, extensionUri, key);
            ResultsPanel.panels.set(key, instance);
        }
    }

    public static postMessage(docUri: vscode.Uri, command: string, data: unknown) {
        const key = docUri.toString();
        const panel = ResultsPanel.panels.get(key);

        if (panel) {
            if (command === 'updateResults') {
                panel._lastResult = data as QueryResult;
            }
            if (command === 'setAllowCsvExport') {
                panel._allowCsvExport = data as boolean;
            }
            panel._panel.webview.postMessage({ command, data });
        }
    }

// openERD removed (legacy)


    private async _loadChartConfig() {
        if (!this._docUri) return;
        try {
            // Check for .chartconfig.json
            const sqlUri = vscode.Uri.parse(this._docUri);
            const baseName = utils.basename(sqlUri.path, '.sql');
            const configUri = vscode.Uri.joinPath(sqlUri, '..', baseName + '.chartconfig.json');

            // Try .chartconfig.json
            if (await fileExists(configUri)) {
                const bytes = await vscode.workspace.fs.readFile(configUri);
                const content = new TextDecoder().decode(bytes);
                const config = JSON.parse(content);
                this._panel.webview.postMessage({ command: 'chartConfigLoaded', data: { config } });
                return;
            }

            // Nothing found
            this._panel.webview.postMessage({ command: 'chartConfigLoaded', data: { config: null } });

        } catch (e) {
            Logger.error("Failed to load chart config", e);
            this._panel.webview.postMessage({ command: 'chartConfigLoaded', data: { config: null } });
        }
    }

    private async _saveChartConfig(config: unknown) {
        if (!this._docUri) {
            await ErrorHandler.handle(
                new Error(formatGeneralError(
                    'Save chart config',
                    'No document URI associated with this panel',
                    'Open a SQL file and run a query first'
                )),
                { severity: ErrorSeverity.Warning, context: 'Save Chart Config' }
            );
            return;
        }
        if (!config) {
            Logger.error("Charts: Received empty config to save.");
            return;
        }
        try {
            const sqlUri = vscode.Uri.parse(this._docUri);
            const baseName = utils.basename(sqlUri.path, '.sql');
            const configUri = vscode.Uri.joinPath(sqlUri, '..', baseName + '.chartconfig.json');

            // Format JSON
            const content = JSON.stringify(config, null, 2);
            await vscode.workspace.fs.writeFile(configUri, new TextEncoder().encode(content));

            this._panel.webview.postMessage({ command: 'chartConfigSaved', data: { ok: true } });
        } catch (e: unknown) {
            const errorMsg = formatGeneralError(
                'Save chart config',
                ErrorHandler.extractErrorMessage(e),
                'Check file permissions and try again'
            );
            await ErrorHandler.handle(e, {
                severity: ErrorSeverity.Error,
                userMessage: errorMsg,
                context: 'Save Chart Config'
            });
            this._panel.webview.postMessage({ command: 'chartConfigSaved', data: { ok: false, error: ErrorHandler.extractErrorMessage(e) } });
        }
    }

    public dispose() {
        if (this._docUri) {
            ResultsPanel.panels.delete(this._docUri);
        }
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) {
                x.dispose();
            }
        }
    }

    private _getWebviewContent(webview: vscode.Webview) {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'dist', 'webviewApp.js'));
        const cssUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'dist', 'webviewApp.css'));

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="Content-Security-Policy" 
                content="default-src 'none'; 
                style-src ${webview.cspSource} 'unsafe-inline'; 
                script-src ${webview.cspSource} 'unsafe-inline' 'unsafe-eval';">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Data Results</title>
            <link rel="stylesheet" href="${cssUri}">
        </head>
        <body>
            <div id="root" style="height: 100vh; width: 100vw; overflow: hidden;"></div>
            <script src="${scriptUri}"></script>
        </body>
        </html>`;
    }
}
