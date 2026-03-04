import * as vscode from 'vscode';
import * as crypto from 'crypto';
import { sanitizeFilename } from '../core/utils';
import { CustomRelationshipsFile, CustomRelationship, ConnectionProfile, ConnectionSecrets, SchemaIntrospection } from '../core/types';
import { readJson, writeJson } from '../core/fsWorkspace';
import { Logger } from '../core/logger';
import { ErrorHandler, ErrorSeverity, formatERDError } from '../core/errorHandler';

interface ERDLayoutSidecar {
    graphSignature: string;
    positions: Record<string, { x: number; y: number }>;
    [key: string]: unknown;
}

export class ERDViewProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'dp.erdView';
    public static current: ERDViewProvider | undefined;

    private _view?: vscode.WebviewView;
    private _extensionUri: vscode.Uri;

    // State
    private _currentConnectionId?: string;
    private _currentConnectionName?: string;
    private _currentData?: Record<string, unknown>;

    constructor(private readonly _context: vscode.ExtensionContext) {
        this._extensionUri = _context.extensionUri;
        ERDViewProvider.current = this;
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
        webviewView.webview.onDidReceiveMessage(async message => {
            switch (message.command) {
                case 'viewReady':
                    if (this._currentData) {
                        this._view?.webview.postMessage({ command: 'showERD', data: this._currentData });
                    }
                    return;
                case 'saveCustomRelationship':
                    await this._handleSaveCustomRelationship(message.data);
                    return;
                case 'deleteCustomRelationship':
                    await this._handleDeleteCustomRelationship(message.data);
                    return;
                case 'saveErdLayout':
                    await this._handleSaveErdLayout(message.data);
                    return;
            }
        });
    }

    public async showERD(
        connectionProfile: ConnectionProfile,
        secrets: ConnectionSecrets,
        introspectionOverride?: SchemaIntrospection
    ) {
        this._currentConnectionId = connectionProfile.id;
        this._currentConnectionName = connectionProfile.name;

        // Focus the view
        if (this._view) {
            this._view.show(true);
        } else {
            await vscode.commands.executeCommand('dp.erdView.focus');
        }

        try {
            // 1. Introspect Schema
            let introspection = introspectionOverride;
            if (!introspection) {
                const { getAdapter } = require('../connections/adapterFactory');
                const adapter = getAdapter(connectionProfile.dialect);
                introspection = await adapter.introspectSchema(connectionProfile, secrets);
            }

            if (!introspection) {
                throw new Error('Failed to introspect schema');
            }

            // 2. Load custom relationships if available
            let customRelationships;
            if (introspection.customRelationshipsPath) {
                try {
                    const customRelUri = vscode.Uri.file(introspection.customRelationshipsPath);
                    const customRelFile = await readJson<CustomRelationshipsFile>(customRelUri);
                    customRelationships = customRelFile?.relationships || [];
                } catch (e) {
                    Logger.warn('Failed to load custom relationships:', e);
                    customRelationships = [];
                }
            }

            // 3. Create ERD Nodes/Edges
            const { generateERD, computeGraphSignature } = require('./erdGenerator');
            const data = generateERD(introspection, customRelationships);

            // 4. Compute graph signature and read layout sidecar
            const graphSignature = computeGraphSignature(data.nodes, data.edges);
            let layout: ERDLayoutSidecar | undefined = undefined;

            const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
            if (workspaceFolder) {
                const dpDir = vscode.Uri.joinPath(workspaceFolder.uri, 'DP');
                const safeName = sanitizeFilename(connectionProfile.name);

                // Save ERD topology
                const erdPath = vscode.Uri.joinPath(dpDir, 'system', 'erd', `${safeName}.erd.json`);
                try {
                    await vscode.workspace.fs.writeFile(erdPath, Buffer.from(JSON.stringify(data, null, 2)));
                } catch (err) {
                    Logger.error('Failed to save ERD JSON:', err);
                }

                // Read layout sidecar
                const layoutPath = vscode.Uri.joinPath(dpDir, 'system', 'erd', `${safeName}.layout.json`);
                try {
                    const saved = await readJson<ERDLayoutSidecar>(layoutPath);
                    if (saved?.graphSignature === graphSignature && saved?.positions) {
                        layout = saved;
                    }
                } catch {
                    // No saved layout — webview will run ELK
                }
            }

            // 5. Send to Webview with layout data
            this._currentData = { ...data, graphSignature, layout, connectionName: connectionProfile.name };
            if (this._view) {
                this._view.webview.postMessage({ command: 'showERD', data: this._currentData });
            }

        } catch (error: unknown) {
            await ErrorHandler.handle(error, {
                severity: ErrorSeverity.Error,
                userMessage: formatERDError(
                    'ERD generation',
                    ErrorHandler.extractErrorMessage(error),
                    'Check connection and schema data'
                ),
                context: 'Show ERD'
            });
        }
    }

    private async _handleSaveCustomRelationship(relationship: CustomRelationship) {
        try {
            if (!this._currentConnectionId) {
                await ErrorHandler.handle(
                    new Error(formatERDError(
                        'Save relationship',
                        'No connection selected',
                        'Open an ERD diagram first'
                    )),
                    { severity: ErrorSeverity.Warning, context: 'Save Custom Relationship' }
                );
                return;
            }

            // Load existing custom relationships
            const { loadSchemas, saveSchema } = require('../schema/schemaStore');
            const { ensureDPDirs } = require('../core/fsWorkspace');
            const schemas = await loadSchemas();
            const schema = schemas.find((s: Record<string, unknown>) => s.connectionId === this._currentConnectionId);

            if (!schema) {
                await ErrorHandler.handle(
                    new Error(formatERDError(
                        'Save relationship',
                        'Schema not found for connection',
                        'Introspect the connection first'
                    )),
                    { severity: ErrorSeverity.Warning, context: 'Save Custom Relationship' }
                );
                return;
            }

            // If customRelationshipsPath doesn't exist, create the file and update schema
            let customRelUri: vscode.Uri;
            if (!schema.customRelationshipsPath) {
                const dpDir = await ensureDPDirs();
                const safeName = schema.connectionName?.replace(/[^a-z0-9_\-\.]/gi, '_') || schema.connectionId;
                const filename = `${safeName}.custom.relationships.json`;
                customRelUri = vscode.Uri.joinPath(dpDir, 'schemas', filename);

                // Create new file
                const newFile: CustomRelationshipsFile = {
                    version: "0.1",
                    connectionId: schema.connectionId,
                    connectionName: schema.connectionName,
                    relationships: []
                };
                await writeJson(customRelUri, newFile);

                // Update schema with path
                schema.customRelationshipsPath = customRelUri.fsPath;
                await saveSchema(schema);
            } else {
                customRelUri = vscode.Uri.file(schema.customRelationshipsPath);
            }

            const customRelFile = await readJson<CustomRelationshipsFile>(customRelUri);

            if (!customRelFile) {
                await ErrorHandler.handle(
                    new Error(formatERDError(
                        'Save relationship',
                        'Failed to load custom relationships file',
                        'Check file permissions and try again'
                    )),
                    { severity: ErrorSeverity.Error, context: 'Save Custom Relationship' }
                );
                return;
            }

            // Check if relationship already exists
            const exists = customRelFile.relationships.some(r =>
                r.source === relationship.source &&
                r.sourceColumn === relationship.sourceColumn &&
                r.target === relationship.target &&
                r.targetColumn === relationship.targetColumn
            );

            if (!exists) {
                customRelFile.relationships.push(relationship);
                await writeJson(customRelUri, customRelFile);
                vscode.window.showInformationMessage('Custom relationship saved!');
            }
        } catch (error: unknown) {
            await ErrorHandler.handle(error, {
                severity: ErrorSeverity.Error,
                userMessage: formatERDError(
                    'Save custom relationship',
                    ErrorHandler.extractErrorMessage(error),
                    'Check file permissions and try again'
                ),
                context: 'Save Custom Relationship'
            });
        }
    }

    private async _handleDeleteCustomRelationship(data: { edgeId: string }) {
        try {
            if (!this._currentConnectionId) {
                return;
            }

            // Extract relationship info from edge ID
            // Format: "e-custom-{source}-{sourceCol}-{target}-{targetCol}"
            if (!data.edgeId.startsWith('e-custom-')) {
                // Not a custom edge, ignore
                return;
            }

            const { loadSchemas } = require('../schema/schemaStore');
            const schemas = await loadSchemas();
            const schema = schemas.find((s: Record<string, unknown>) => s.connectionId === this._currentConnectionId);

            if (!schema || !schema.customRelationshipsPath) {
                return;
            }

            const customRelUri = vscode.Uri.file(schema.customRelationshipsPath);
            const customRelFile = await readJson<CustomRelationshipsFile>(customRelUri);

            if (!customRelFile) {
                return;
            }

            // Parse edge ID to find and remove the relationship
            // This is tricky because table/column names might contain hyphens
            // For now, we'll filter by checking each relationship's generated ID
            const edgeId = data.edgeId;
            customRelFile.relationships = customRelFile.relationships.filter(r => {
                const expectedId = `e-custom-${r.source}-${r.sourceColumn}-${r.target}-${r.targetColumn}`;
                return expectedId !== edgeId;
            });

            await writeJson(customRelUri, customRelFile);
            vscode.window.showInformationMessage('Custom relationship deleted!');

            // Reload ERD to reflect changes
            // We'd need to re-fetch connection profile and secrets...
            // For now, user can refresh manually
        } catch (error: unknown) {
            Logger.error('Failed to delete custom relationship:', error);
        }
    }

    private async _handleSaveErdLayout(data: {
        connectionName: string;
        graphSignature: string;
        positions: Record<string, { x: number; y: number }>;
    }) {
        try {
            const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
            if (!workspaceFolder) return;
            const dpDir = vscode.Uri.joinPath(workspaceFolder.uri, 'DP');
            const safeName = sanitizeFilename(data.connectionName);
            const layoutPath = vscode.Uri.joinPath(dpDir, 'system', 'erd', `${safeName}.layout.json`);

            const sidecar = {
                version: "0.1",
                connectionId: this._currentConnectionId,
                connectionName: data.connectionName,
                graphSignature: data.graphSignature,
                positions: data.positions,
                updatedAt: new Date().toISOString()
            };
            await writeJson(layoutPath, sidecar);
        } catch (err) {
            Logger.error('Failed to save ERD layout:', err);
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
    <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src 'nonce-${nonce}'; font-src ${webview.cspSource};">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="${styleUri}" rel="stylesheet">
    <title>ERD</title>
</head>
<body>
    <div id="root" data-view-type="erd"></div>
    <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>
        `;
    }
}

function getNonce() {
    return crypto.randomBytes(16).toString('hex');
}
