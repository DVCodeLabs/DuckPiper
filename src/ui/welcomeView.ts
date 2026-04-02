import * as vscode from 'vscode';
import { isProjectInitialized, updateProjectInitializedContext } from '../core/isProjectInitialized';
import { fileExists } from '../core/fsWorkspace';

export class WelcomeView {
    public static currentPanel: WelcomeView | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri) {
        this._panel = panel;
        this._extensionUri = extensionUri;

        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
        vscode.workspace.onDidChangeWorkspaceFolders(() => {
            void this._sendStatus();
        }, null, this._disposables);
        this._panel.webview.html = this._getWebviewContent(this._panel.webview, extensionUri);
        this._setWebviewMessageListener(this._panel.webview);
    }

    public static render(extensionUri: vscode.Uri) {
        if (WelcomeView.currentPanel) {
            WelcomeView.currentPanel._panel.reveal(vscode.ViewColumn.One);
            // Refresh status
            WelcomeView.currentPanel._sendStatus();
            return;
        }

        const panel = vscode.window.createWebviewPanel(
            'dpWelcome',
            'Welcome to DuckPiper',
            vscode.ViewColumn.One,
            {
                enableScripts: true,
                retainContextWhenHidden: true,
                localResourceRoots: [vscode.Uri.joinPath(extensionUri, 'dist')]
            }
        );

        WelcomeView.currentPanel = new WelcomeView(panel, extensionUri);
    }

    private async _sendStatus() {
        const initialized = await isProjectInitialized();
        const hasWorkspace = (vscode.workspace.workspaceFolders?.length ?? 0) > 0;
        this._panel.webview.postMessage({ command: 'setStatus', initialized, hasWorkspace });
    }

    public dispose() {
        WelcomeView.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) x.dispose();
        }
    }

    private _setWebviewMessageListener(webview: vscode.Webview) {
        webview.onDidReceiveMessage(
            async (message: Record<string, unknown>) => {
                switch (message.command) {
                    case 'ready':
                        await this._sendStatus();
                        break;

                    case 'initialize':
                        try {
                            if ((vscode.workspace.workspaceFolders?.length ?? 0) === 0) {
                                vscode.window.showWarningMessage('Open a folder before initializing DuckPiper.');
                                return;
                            }

                            // Full initialization - must match dp.project.initialize command flow
                            const { ensureDPDirs, ensureAgentsMd, ensureReadmeMd } = require('../core/fsWorkspace');
                            const { initializePromptFiles } = require('../ai/prompts');
                            const { queryIndex } = require('../queryLibrary/queryIndex');
                            const { PipelineRepository } = require('../pipelines/pipelineRepository');
                            const { LocalDuckDB } = require('../transform/localDuckDB');
                            const { HistoryService } = require('../services/historyService');

                            await ensureDPDirs();
                            await queryIndex.initialize();
                            await initializePromptFiles();
                            await ensureAgentsMd();
                            await ensureReadmeMd();
                            await PipelineRepository.getInstance().initialize();
                            await LocalDuckDB.getInstance().initialize();
                            await HistoryService.getInstance().initialize();

                            await updateProjectInitializedContext();
                            await vscode.commands.executeCommand('dp.view.refreshConnections');
                            await vscode.commands.executeCommand('dp.view.refreshLocalDuckDB');
                            vscode.window.showInformationMessage('DuckPiper project initialized successfully!');
                            await this._sendStatus();
                        } catch (e: unknown) {
                            vscode.window.showErrorMessage(`Initialization failed: ${e instanceof Error ? e.message : String(e)}`);
                        }
                        break;

                    case 'addConnection':
                        vscode.commands.executeCommand('dp.connection.add');
                        break;

                    case 'openSettings':
                        vscode.commands.executeCommand('dp.openSettings');
                        break;

                    case 'openFolder':
                        vscode.commands.executeCommand('vscode.openFolder');
                        break;

                    case 'openReadme':
                        try {
                            const folders = vscode.workspace.workspaceFolders;
                            if (!folders) {
                                vscode.window.showWarningMessage('No workspace folder open.');
                                return;
                            }

                            const readmeDpPath = vscode.Uri.joinPath(folders[0].uri, 'README_DP.md');
                            if (!(await fileExists(readmeDpPath))) {
                                vscode.window.showWarningMessage('README_DP.md not found. Initialize DuckPiper to create it.');
                                return;
                            }

                            const doc = await vscode.workspace.openTextDocument(readmeDpPath);
                            await vscode.window.showTextDocument(doc, { preview: true, viewColumn: vscode.ViewColumn.Beside });
                        } catch (_e: unknown) {
                            vscode.window.showWarningMessage('Could not open README_DP.md.');
                        }
                        break;
                }
            },
            undefined,
            this._disposables
        );
    }

    private _getWebviewContent(webview: vscode.Webview, extensionUri: vscode.Uri): string {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, 'dist', 'welcomeApp.js'));

        return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src ${webview.cspSource} 'unsafe-inline';">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Welcome to DuckPiper</title>
</head>
<body>
  <div id="root"></div>
  <script src="${scriptUri}"></script>
</body>
</html>`;
    }
}
