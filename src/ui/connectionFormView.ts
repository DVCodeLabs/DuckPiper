import * as vscode from 'vscode';
import * as crypto from 'crypto';
import { ProviderRegistry } from '../connections/providerRegistry';
import { loadConnectionProfiles, saveConnectionProfile, saveConnectionSecrets, getConnectionSecrets, validateConnectionName } from '../connections/connectionStore';
import { getAdapter } from '../connections/adapterFactory';
import { performIntrospection } from '../connections/connectionCommands';
import { formatDatabaseConnectionError } from '../connections/connectionErrors';
import { buildReuseDraft, getCompatibleSources, isReuseEnabled } from '../connections/connectionReuse';
import { Logger } from '../core/logger';
import { ConnectionProfile, ConnectionSecrets, DPConnectionFieldPicker } from '../core/types';

interface PickFieldMessage {
    command: 'pickFieldValue';
    field: {
        key: string;
        storage?: 'profile' | 'secrets' | 'local';
        picker?: DPConnectionFieldPicker;
    };
}

interface RunProviderActionMessage {
    command: 'runProviderAction';
    dialect: string;
    actionId: string;
    payload?: Record<string, unknown>;
}

interface RequestReuseDraftMessage {
    command: 'requestReuseDraft';
    sourceId: string;
    dialect: string;
}

/** Incoming messages from the connection form webview */
interface FormWebviewMessage {
    command: string;
    profile?: ConnectionProfile;
    secrets?: ConnectionSecrets;
}

export class ConnectionFormView {
    public static currentPanel: ConnectionFormView | undefined;
    private readonly _panel: vscode.WebviewPanel;
    private readonly _extensionUri: vscode.Uri;
    private _disposables: vscode.Disposable[] = [];

    private constructor(panel: vscode.WebviewPanel, extensionUri: vscode.Uri, profile?: ConnectionProfile, secrets?: ConnectionSecrets) {
        this._panel = panel;
        this._extensionUri = extensionUri;

        this._panel.onDidDispose(() => this.dispose(), null, this._disposables);
        this._panel.webview.html = this._getWebviewContent(this._panel.webview, extensionUri);
        this._setWebviewMessageListener(this._panel.webview, profile, secrets);
    }

    public static render(extensionUri: vscode.Uri, profile?: ConnectionProfile, secrets?: ConnectionSecrets) {
        if (ConnectionFormView.currentPanel) {
            ConnectionFormView.currentPanel.dispose();
        }

        const title = profile ? `Edit: ${profile.name}` : "Add DB Connection";
        const panel = vscode.window.createWebviewPanel("connectionForm", title, vscode.ViewColumn.One, {
            enableScripts: true,
            retainContextWhenHidden: true,
            localResourceRoots: [vscode.Uri.joinPath(extensionUri, "dist")]
        });
        ConnectionFormView.currentPanel = new ConnectionFormView(panel, extensionUri, profile, secrets);
    }

    public dispose() {
        ConnectionFormView.currentPanel = undefined;
        this._panel.dispose();
        while (this._disposables.length) {
            const x = this._disposables.pop();
            if (x) x.dispose();
        }
    }

    private _setWebviewMessageListener(webview: vscode.Webview, initProfile?: ConnectionProfile, initSecrets?: ConnectionSecrets) {
        webview.onDidReceiveMessage(
            async (message: FormWebviewMessage) => {
                switch (message.command) {
                    case 'ready': {
                        const providers = ProviderRegistry.getInstance().getProviders();
                        webview.postMessage({ command: 'setProviders', providers });

                        if (initProfile) {
                            webview.postMessage({
                                command: 'setProfile',
                                profile: initProfile,
                                secrets: initSecrets
                            });
                        } else {
                            // Add mode: send existing profiles for reuse selector
                            const allProfiles = await loadConnectionProfiles();
                            const reuseSources: Record<string, unknown[]> = {};
                            for (const provider of providers) {
                                if (!isReuseEnabled(provider)) continue;
                                const sources = getCompatibleSources(allProfiles, provider);
                                if (sources.length > 0) {
                                    reuseSources[provider.dialect] = sources;
                                }
                            }
                            webview.postMessage({ command: 'setReuseSources', reuseSources });
                        }
                        break;
                    }
                    case 'test':
                        try {
                            if (!message.profile) break;
                            const adapter = getAdapter(message.profile.dialect);
                            await adapter.testConnection(message.profile, message.secrets ?? {});
                            webview.postMessage({ command: 'testResult', success: true, message: 'Connection successful!' });
                        } catch (e: unknown) {
                            Logger.warn('Connection test failed:', e);
                            webview.postMessage({ command: 'testResult', success: false, message: formatDatabaseConnectionError(e) });
                        }
                        break;
                    case 'requestReuseDraft':
                        await this._handleReuseDraft(message as RequestReuseDraftMessage, webview);
                        break;
                    case 'save':
                        try {
                            if (!message.profile) break;
                            const profile = message.profile;
                            const secrets = message.secrets ?? {};

                            // Validate connection name uniqueness
                            const nameError = await validateConnectionName(
                                profile.name ?? '',
                                profile.id || undefined
                            );
                            if (nameError) {
                                webview.postMessage({
                                    command: 'saveResult',
                                    success: false,
                                    message: nameError
                                });
                                break;
                            }

                            try {
                                const adapter = getAdapter(profile.dialect);
                                const testProfile = profile.dialect === 'duckdb'
                                    ? { ...profile, _runqlAllowCreateOnTest: true }
                                    : profile;
                                await adapter.testConnection(testProfile, secrets);
                            } catch (e: unknown) {
                                Logger.warn('Connection test before save failed:', e);
                                webview.postMessage({
                                    command: 'saveResult',
                                    success: false,
                                    message: formatDatabaseConnectionError(e)
                                });
                                break;
                            }

                            const now = new Date().toISOString();
                            if (!profile.id) {
                                profile.id = crypto.randomUUID();
                                profile.createdAt = now;
                            } else if (!profile.createdAt) {
                                profile.createdAt = now;
                            }
                            profile.updatedAt = now;

                            await saveConnectionSecrets(profile.id, secrets, profile.credentialStorageMode);
                            await saveConnectionProfile(profile);

                            vscode.window.showInformationMessage(`Connection '${profile.name}' saved.`);

                            vscode.commands.executeCommand('dp.view.refreshConnections');
                            performIntrospection(profile, true);

                            this.dispose();
                        } catch (e: unknown) {
                            Logger.warn('Save failed:', e);
                            const errMsg = e instanceof Error ? e.message : 'Save failed.';
                            webview.postMessage({
                                command: 'saveResult',
                                success: false,
                                message: errMsg
                            });
                        }
                        break;
                    case 'pickFieldValue':
                        await this._pickFieldValue(message as PickFieldMessage, webview);
                        break;
                    case 'runProviderAction':
                        await this._runProviderAction(message as RunProviderActionMessage, webview);
                        break;
                }
            },
            undefined,
            this._disposables
        );
    }

    private async _handleReuseDraft(message: RequestReuseDraftMessage, webview: vscode.Webview): Promise<void> {
        if (!message.sourceId || !message.dialect) return;

        try {
            const allProfiles = await loadConnectionProfiles();
            const source = allProfiles.find((p) => p.id === message.sourceId);
            if (!source) {
                webview.postMessage({
                    command: 'reuseDraftResult',
                    sourceId: message.sourceId,
                    profilePatch: {},
                    secretsPatch: {},
                    secretsAvailable: false,
                    status: { type: 'error', text: 'Source connection not found.' }
                });
                return;
            }

            const provider = ProviderRegistry.getInstance().getProvider(message.dialect);
            if (!provider) {
                webview.postMessage({
                    command: 'reuseDraftResult',
                    sourceId: message.sourceId,
                    profilePatch: {},
                    secretsPatch: {},
                    secretsAvailable: false,
                    status: { type: 'error', text: 'Provider not found.' }
                });
                return;
            }

            // Don't copy secrets from session-mode sources
            const isSessionOnly = source.credentialStorageMode === 'session';
            let sourceSecrets = {};
            let secretsAvailable = false;

            if (!isSessionOnly) {
                try {
                    sourceSecrets = await getConnectionSecrets(source.id);
                    secretsAvailable = true;
                } catch {
                    // Secrets not available — proceed with profile-only copy
                }
            }

            const { profilePatch, secretsPatch } = buildReuseDraft(
                source, sourceSecrets, provider, secretsAvailable
            );

            const status = secretsAvailable
                ? { type: 'success' as const, text: 'Settings copied from existing connection.' }
                : { type: 'info' as const, text: 'Server settings copied. Enter credentials to continue.' };

            webview.postMessage({
                command: 'reuseDraftResult',
                sourceId: message.sourceId,
                profilePatch,
                secretsPatch,
                secretsAvailable,
                status
            });
        } catch (e: unknown) {
            Logger.warn('Failed to build reuse draft:', e);
            webview.postMessage({
                command: 'reuseDraftResult',
                sourceId: message.sourceId,
                profilePatch: {},
                secretsPatch: {},
                secretsAvailable: false,
                status: { type: 'error', text: 'Failed to load source connection.' }
            });
        }
    }

    private async _runProviderAction(message: RunProviderActionMessage, webview: vscode.Webview): Promise<void> {
        if (!message.dialect || !message.actionId) return;

        try {
            const result = await ProviderRegistry.getInstance().runProviderAction(
                message.dialect,
                message.actionId,
                message.payload ?? {}
            );
            webview.postMessage({
                command: 'providerActionResult',
                actionId: message.actionId,
                result: result ?? null
            });
        } catch (e: unknown) {
            const errMsg = e instanceof Error ? e.message : 'Action failed.';
            webview.postMessage({
                command: 'providerActionResult',
                actionId: message.actionId,
                result: {
                    status: {
                        type: 'error',
                        text: errMsg
                    }
                }
            });
        }
    }

    private async _pickFieldValue(message: PickFieldMessage, webview: vscode.Webview): Promise<void> {
        const picker = message.field?.picker;
        if (!picker) return;

        if ((picker.mode ?? 'open') === 'save') {
            const saveUri = await vscode.window.showSaveDialog({
                saveLabel: picker.openLabel ?? 'Save',
                title: picker.title,
                filters: picker.filters
            });
            if (saveUri) {
                webview.postMessage({
                    command: 'fieldValueSelected',
                    key: message.field.key,
                    storage: message.field.storage ?? 'profile',
                    value: saveUri.fsPath
                });
            }
            return;
        }

        const fileUris = await vscode.window.showOpenDialog({
            canSelectFiles: picker.canSelectFiles ?? true,
            canSelectFolders: picker.canSelectFolders ?? false,
            canSelectMany: false,
            filters: picker.filters,
            openLabel: picker.openLabel ?? 'Select'
        });

        if (fileUris && fileUris.length > 0) {
            webview.postMessage({
                command: 'fieldValueSelected',
                key: message.field.key,
                storage: message.field.storage ?? 'profile',
                value: fileUris[0].fsPath
            });
        }
    }

    private _getWebviewContent(webview: vscode.Webview, extensionUri: vscode.Uri) {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, "dist", "connectionFormApp.js"));
        const styleUri = webview.asWebviewUri(vscode.Uri.joinPath(extensionUri, "dist", "connectionFormApp.css"));
        return `<!DOCTYPE html>
         <html lang="en">
         <head>
            <meta charset="UTF-8">
            <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline'; script-src ${webview.cspSource} 'unsafe-inline';">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link rel="stylesheet" href="${styleUri}">
            <title>Add DB Connection</title>
         </head>
         <body>
            <div id="root"></div>
            <script src="${scriptUri}"></script>
         </body>
         </html>`;
    }
}
