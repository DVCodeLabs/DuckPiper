import * as vscode from 'vscode';
import * as crypto from 'crypto';
import { PipelineRepository } from './pipelineRepository';
import { generateLineageGraph, LineageGraphData } from './lineageGenerator';

export class LineageViewProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'dp.lineageView';
    public static current: LineageViewProvider | undefined;

    private _view?: vscode.WebviewView;
    private _extensionUri: vscode.Uri;

    // State
    private _currentPipelineId?: string;
    private _currentData?: LineageGraphData;

    constructor(private readonly _context: vscode.ExtensionContext) {
        this._extensionUri = _context.extensionUri;
        LineageViewProvider.current = this;
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

        webviewView.webview.html = this._getWebviewContent(webviewView.webview);

        webviewView.onDidDispose(() => {
            this._view = undefined;
        });

        // Handle messages from webview
        webviewView.webview.onDidReceiveMessage(message => {
            switch (message.command) {
                case 'viewReady':
                    if (this._currentData) {
                        this._view?.webview.postMessage({ command: 'showLineage', data: this._currentData });
                    }
                    return;
            }
        });
    }

    public async showLineage(pipelineId: string) {
        this._currentPipelineId = pipelineId;

        // Focus the view
        if (this._view) {
            this._view.show(true);
        } else {
            await vscode.commands.executeCommand('dp.lineageView.focus');
        }

        const { DuckDBLifecycleManager } = require('../transform/duckdbLifecycle');
        await DuckDBLifecycleManager.getInstance().reconcileLineage(pipelineId);

        const repo = PipelineRepository.getInstance();
        const lineage = await repo.loadLineage(pipelineId);

        if (!lineage) {
            vscode.window.showErrorMessage(`Lineage data not found for pipeline ${pipelineId}`);
            return;
        }

        const graphData = generateLineageGraph(lineage);
        this._currentData = graphData;

        if (this._view) {
            this._view.webview.postMessage({ command: 'showLineage', data: graphData });
        }
    }

    private _getWebviewContent(webview: vscode.Webview) {
        const scriptUri = webview.asWebviewUri(
            vscode.Uri.joinPath(this._extensionUri, 'dist', 'webviewApp.js')
        );
        const styleUri = webview.asWebviewUri(
            vscode.Uri.joinPath(this._extensionUri, 'dist', 'webviewApp.css')
        );

        const nonce = getNonce();

        return `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; font-src ${webview.cspSource}; img-src ${webview.cspSource} data: https:;">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="${styleUri}" rel="stylesheet">
    <title>Lineage</title>
</head>
<body>
    <div id="root" data-view-type="lineage"></div>
    <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>
        `;
    }
}

function getNonce() {
    return crypto.randomBytes(16).toString('hex');
}
