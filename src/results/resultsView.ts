import * as vscode from 'vscode';
import { QueryResult, ScriptExecutionResult } from '../core/types';
import { Logger } from '../core/logger';
import { ErrorHandler, ErrorSeverity, formatGeneralError } from '../core/errorHandler';

export class ResultsViewProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'dp.resultsView';
    public static current: ResultsViewProvider | undefined;

    private _view?: vscode.WebviewView;
    private _extensionUri: vscode.Uri;

    // State
    private _activeDocUri?: string;
    private _lastResultByDocUri = new Map<string, QueryResult>();
    private _lastScriptResultByDocUri = new Map<string, ScriptExecutionResult>();
    private _allowCsvExportByDocUri = new Map<string, boolean>();

    constructor(
        private readonly _context: vscode.ExtensionContext
    ) {
        this._extensionUri = _context.extensionUri;
        ResultsViewProvider.current = this;
    }

    public resolveWebviewView(
        webviewView: vscode.WebviewView,
        _context: vscode.WebviewViewResolveContext,
        _token: vscode.CancellationToken,
    ) {
        this._view = webviewView;

        webviewView.webview.options = {
            enableScripts: true,
            localResourceRoots: [
                vscode.Uri.joinPath(this._extensionUri, 'dist'),
                vscode.Uri.joinPath(this._extensionUri, 'resources')
            ]
        };



        webviewView.onDidDispose(() => {
            this._view = undefined;
        });

        webviewView.webview.html = this._getWebviewContent(webviewView.webview);

        // Handle messages from the webview
        webviewView.webview.onDidReceiveMessage(message => {
            switch (message.command) {
                case 'exportCsv':
                    if (this._activeDocUri) {
                        const csvAllowed = this._allowCsvExportByDocUri.get(this._activeDocUri);
                        if (csvAllowed === false) {
                            vscode.window.showInformationMessage('CSV export is not available for this connection.');
                            return;
                        }
                        const result = this._lastResultByDocUri.get(this._activeDocUri);
                        if (result) {
                            const { exportToCsv } = require('./exportCsv');
                            exportToCsv(result);
                        } else {
                            vscode.window.showWarningMessage('No results to export.');
                        }
                    }
                    return;

                case 'viewReady':
                    this._updateViewForActiveDoc();
                    return;

                case 'cacheLoadedRows':
                    vscode.commands.executeCommand('dp.system.saveLoadedResultsToDuckDB', message.data);
                    return;

                case 'loadChartConfig':
                    this._loadChartConfig();
                    return;

                case 'saveChartConfig':
                    this._saveChartConfig(message.data?.config);
                    return;

                case 'applyResultsetEdits':
                    if (this._activeDocUri) {
                        vscode.commands.executeCommand('dp.results.applyEdits', vscode.Uri.parse(this._activeDocUri), message.data);
                    }
                    return;
            }
        });
    }

    public show(docUri: vscode.Uri) {
        this._activeDocUri = docUri.toString();
        // If view is visible, update it. If not, it will update on resolve/viewReady.
        if (this._view) {
            this._view.show(true);
            this._updateViewForActiveDoc();
        } else {
            // Focus the view
            vscode.commands.executeCommand('dp.resultsView.focus');
        }
    }

    public postMessage(docUri: vscode.Uri, command: string, data: unknown) {
        const uriStr = docUri.toString();

        // Update cache
        if (command === 'updateResults') {
            this._lastResultByDocUri.set(uriStr, data as QueryResult);
            this._lastScriptResultByDocUri.delete(uriStr); // clear script cache
        }
        if (command === 'updateScriptResults') {
            this._lastScriptResultByDocUri.set(uriStr, data as ScriptExecutionResult);
            this._lastResultByDocUri.delete(uriStr); // clear single-result cache
        }
        if (command === 'setAllowCsvExport') {
            this._allowCsvExportByDocUri.set(uriStr, data as boolean);
        }

        // If this is the active doc, send to webview
        if (this._activeDocUri === uriStr) {
            if (this._view) {
                this._view.webview.postMessage({ command, data });
            }
        }
    }

    public getLastResult(docUri: vscode.Uri): QueryResult | undefined {
        return this._lastResultByDocUri.get(docUri.toString());
    }

    private _updateViewForActiveDoc() {
        if (!this._view || !this._activeDocUri) {
            return;
        }

        const allowCsvExport = this._allowCsvExportByDocUri.get(this._activeDocUri);
        if (allowCsvExport !== undefined) {
            this._view.webview.postMessage({ command: 'setAllowCsvExport', data: allowCsvExport });
        }

        // Script results take precedence
        const scriptResult = this._lastScriptResultByDocUri.get(this._activeDocUri);
        if (scriptResult) {
            this._view.webview.postMessage({ command: 'updateScriptResults', data: scriptResult });
            return;
        }

        const result = this._lastResultByDocUri.get(this._activeDocUri);
        if (result) {
            this._view.webview.postMessage({ command: 'updateResults', data: result });
        } else {
            this._view.webview.postMessage({ command: 'clearResults' });
        }
    }

    private async _loadChartConfig() {
        if (!this._view || !this._activeDocUri) return;
        try {
            const sqlUri = vscode.Uri.parse(this._activeDocUri);
            const path = require('path');
            const baseName = path.basename(sqlUri.path, '.sql');
            const configUri = vscode.Uri.joinPath(sqlUri, '..', baseName + '.chartconfig.json');

            const { fileExists } = require('../core/fsWorkspace');
            if (await fileExists(configUri)) {
                const bytes = await vscode.workspace.fs.readFile(configUri);
                const content = new TextDecoder().decode(bytes);
                const config = JSON.parse(content);
                this._view.webview.postMessage({ command: 'chartConfigLoaded', data: { config } });
                return;
            }

            // Nothing found
            this._view.webview.postMessage({ command: 'chartConfigLoaded', data: { config: null } });
        } catch (e) {
            Logger.error("Failed to load chart config", e);
            this._view.webview.postMessage({ command: 'chartConfigLoaded', data: { config: null } });
        }
    }

    private async _saveChartConfig(config: unknown) {
        if (!this._view || !this._activeDocUri) {
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
            const sqlUri = vscode.Uri.parse(this._activeDocUri);
            const path = require('path');
            const baseName = path.basename(sqlUri.path, '.sql');
            const configUri = vscode.Uri.joinPath(sqlUri, '..', baseName + '.chartconfig.json');

            const content = JSON.stringify(config, null, 2);
            await vscode.workspace.fs.writeFile(configUri, new TextEncoder().encode(content));

            this._view.webview.postMessage({ command: 'chartConfigSaved', data: { ok: true } });
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
            this._view.webview.postMessage({ command: 'chartConfigSaved', data: { ok: false, error: ErrorHandler.extractErrorMessage(e) } });
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
