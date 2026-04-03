import * as vscode from 'vscode';
import * as path from 'path';
import { fileExists } from '../core/fsWorkspace';

export class MarkdownViewProvider implements vscode.WebviewViewProvider {
    public static readonly viewType = 'dp.markdownView';
    public static current: MarkdownViewProvider | undefined;

    private _view?: vscode.WebviewView;
    private _extensionUri: vscode.Uri;

    // State keyed by source document URI string (SQL or notebook)
    private _activeDocUri?: string;
    private _contentByDocUri = new Map<string, string>();    // full file content (with frontmatter)
    private _dirtyByDocUri = new Map<string, boolean>();
    private _mdUriByDocUri = new Map<string, string>();      // resolved .md URI string
    private _frontmatterByDocUri = new Map<string, string>(); // preserved frontmatter per doc

    private _isGenerating = false;
    private _fileWatcher?: vscode.FileSystemWatcher;
    private _suppressFileWatch = false;

    constructor(private readonly _context: vscode.ExtensionContext) {
        this._extensionUri = _context.extensionUri;
        MarkdownViewProvider.current = this;
        this._setupFileWatcher();
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
            ]
        };

        webviewView.onDidDispose(() => {
            this._view = undefined;
        });

        webviewView.webview.html = this._getWebviewContent(webviewView.webview);

        webviewView.webview.onDidReceiveMessage(message => {
            switch (message.command) {
                case 'viewReady':
                    this._updateViewForActiveDoc();
                    return;

                case 'contentChanged':
                    if (this._activeDocUri) {
                        const frontmatter = this._frontmatterByDocUri.get(this._activeDocUri) || '';
                        const fullContent = frontmatter + message.data.content;
                        this._contentByDocUri.set(this._activeDocUri, fullContent);
                        this._dirtyByDocUri.set(this._activeDocUri, true);
                    }
                    return;

                case 'save':
                    this.save();
                    return;

                case 'reload':
                    this.reloadFromDisk();
                    return;

                case 'createMarkdown':
                    if (this._activeDocUri) {
                        this._createEmptyMarkdown(vscode.Uri.parse(this._activeDocUri));
                    }
                    return;

                case 'generateMarkdown':
                    if (this._activeDocUri && this._isNotebookUri(this._activeDocUri)) {
                        vscode.commands.executeCommand('dp.notebook.generateMarkdownDoc');
                    } else {
                        vscode.commands.executeCommand('dp.query.generateMarkdownDoc');
                    }
                    return;
            }
        });
    }

    /** Passive follow — called on active editor change. Does not force-focus the panel. */
    public async show(sqlDocUri: vscode.Uri) {
        const uriStr = sqlDocUri.toString();

        // Auto-save dirty content before switching
        if (this._activeDocUri && this._activeDocUri !== uriStr && this._dirtyByDocUri.get(this._activeDocUri)) {
            await this._saveToDisk(this._activeDocUri);
        }

        this._activeDocUri = uriStr;
        const mdUri = this._resolveMdUri(sqlDocUri);
        this._mdUriByDocUri.set(uriStr, mdUri.toString());

        await this._loadFromDisk(uriStr, mdUri);
        this._updateViewForActiveDoc();
    }

    /** Active focus — called from generate/open commands. Reveals the panel. */
    public async showAndFocus(sqlDocUri: vscode.Uri) {
        await this.show(sqlDocUri);
        if (this._view) {
            this._view.show(true);
        } else {
            vscode.commands.executeCommand('dp.markdownView.focus');
        }
    }

    /** Called during generation streaming to update panel content. */
    public updateContent(sqlDocUri: vscode.Uri, fullContent: string) {
        const uriStr = sqlDocUri.toString();
        this._contentByDocUri.set(uriStr, fullContent);
        const { frontmatter, body } = this._splitFrontmatter(fullContent);
        this._frontmatterByDocUri.set(uriStr, frontmatter);

        if (this._activeDocUri === uriStr && this._view) {
            this._view.webview.postMessage({
                command: 'updateContent',
                data: {
                    content: body,
                    fileName: this._getFileName(uriStr),
                    dirty: false,
                }
            });
        }
    }

    /** Toggle generating state in the webview. */
    public setGenerating(sqlDocUri: vscode.Uri, generating: boolean) {
        this._isGenerating = generating;
        const uriStr = sqlDocUri.toString();
        if (this._activeDocUri === uriStr && this._view) {
            this._view.webview.postMessage({
                command: 'setGenerating',
                data: { generating }
            });
        }
    }

    /** Save current panel content to disk. */
    public async save() {
        if (!this._activeDocUri) return;
        await this._saveToDisk(this._activeDocUri);
    }

    /** Reload current panel content from disk. */
    public async reloadFromDisk() {
        if (!this._activeDocUri) return;
        const mdUriStr = this._mdUriByDocUri.get(this._activeDocUri);
        if (!mdUriStr) return;
        await this._loadFromDisk(this._activeDocUri, vscode.Uri.parse(mdUriStr));
        this._dirtyByDocUri.set(this._activeDocUri, false);
        this._updateViewForActiveDoc();
    }

    // ── Private helpers ──

    private async _saveToDisk(sqlUriStr: string) {
        const mdUriStr = this._mdUriByDocUri.get(sqlUriStr);
        if (!mdUriStr) return;

        const content = this._contentByDocUri.get(sqlUriStr);
        if (content === undefined) return;

        try {
            this._suppressFileWatch = true;
            await vscode.workspace.fs.writeFile(
                vscode.Uri.parse(mdUriStr),
                Buffer.from(content, 'utf8')
            );
            this._dirtyByDocUri.set(sqlUriStr, false);
            if (this._activeDocUri === sqlUriStr && this._view) {
                this._view.webview.postMessage({ command: 'setDirty', data: { dirty: false } });
            }
        } catch (e: unknown) {
            vscode.window.showErrorMessage(`Failed to save markdown: ${e instanceof Error ? e.message : String(e)}`);
        } finally {
            // Small delay before re-enabling file watch to avoid self-triggering
            setTimeout(() => { this._suppressFileWatch = false; }, 200);
        }
    }

    private async _loadFromDisk(sqlUriStr: string, mdUri: vscode.Uri) {
        if (await fileExists(mdUri)) {
            const bytes = await vscode.workspace.fs.readFile(mdUri);
            const fullContent = new TextDecoder().decode(bytes);
            this._contentByDocUri.set(sqlUriStr, fullContent);
            const { frontmatter } = this._splitFrontmatter(fullContent);
            this._frontmatterByDocUri.set(sqlUriStr, frontmatter);
            this._dirtyByDocUri.set(sqlUriStr, false);
        } else {
            // No markdown file exists
            this._contentByDocUri.delete(sqlUriStr);
            this._frontmatterByDocUri.delete(sqlUriStr);
            this._dirtyByDocUri.delete(sqlUriStr);
        }
    }

    private _updateViewForActiveDoc() {
        if (!this._view || !this._activeDocUri) return;

        const content = this._contentByDocUri.get(this._activeDocUri);
        if (content === undefined) {
            // No markdown file — show empty state
            this._view.webview.postMessage({
                command: 'showEmpty',
                data: { reason: 'no-markdown' }
            });
            return;
        }

        const { body } = this._splitFrontmatter(content);
        const dirty = this._dirtyByDocUri.get(this._activeDocUri) ?? false;
        this._view.webview.postMessage({
            command: 'updateContent',
            data: {
                content: body,
                fileName: this._getFileName(this._activeDocUri),
                dirty,
            }
        });
    }

    /** Show empty state when no SQL editor is active. */
    public showNoEditor() {
        this._activeDocUri = undefined;
        if (this._view) {
            this._view.webview.postMessage({
                command: 'showEmpty',
                data: { reason: 'no-sql-editor' }
            });
        }
    }

    private async _createEmptyMarkdown(docUri: vscode.Uri) {
        const mdUri = this._resolveMdUri(docUri);
        const isNotebook = this._isNotebookUri(docUri.toString());
        const ext = isNotebook ? /\.dpnb$/i : /\.sql$/i;
        const title = path.basename(docUri.fsPath).replace(ext, '');
        const prettyTitle = title.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
        const today = new Date().toISOString().split('T')[0];

        let content: string;
        if (isNotebook) {
            content = [
                "---",
                `title: "${prettyTitle}"`,
                `created_at: "${today}"`,
                `type: "notebook_doc"`,
                "tags: []",
                "---",
                "",
                "<!-- DO NOT EDIT ABOVE THIS LINE - SYSTEM MANAGED -->",
                ""
            ].join("\n");
        } else {
            const sourcePath = vscode.workspace.asRelativePath(docUri, false);
            content = [
                "---",
                `title: "${prettyTitle}"`,
                `created_at: "${today}"`,
                `connection: "none"`,
                `dialect: "unknown"`,
                "tags: []",
                `source_path: "${sourcePath}"`,
                `source_hash: ""`,
                "---",
                "",
                "<!-- DO NOT EDIT ABOVE THIS LINE - SYSTEM MANAGED -->",
                "",
                "<!-- DuckPiper:content:start -->",
                "",
                "<!-- DuckPiper:content:end -->",
                ""
            ].join("\n");
        }

        await vscode.workspace.fs.writeFile(mdUri, Buffer.from(content, 'utf8'));
        await this.show(docUri);
        this._updateViewForActiveDoc();
    }

    private _resolveMdUri(docUri: vscode.Uri): vscode.Uri {
        if (/\.dpnb$/i.test(docUri.path)) {
            return docUri.with({ path: docUri.path.replace(/\.dpnb$/i, '.md') });
        }
        return docUri.with({ path: docUri.path.replace(/\.sql$/i, '.md') });
    }

    private _isNotebookUri(uriStr: string): boolean {
        return /\.dpnb$/i.test(uriStr);
    }

    private _splitFrontmatter(text: string): { frontmatter: string; body: string } {
        const lines = text.split(/\r?\n/);
        if (lines[0] !== '---') {
            return { frontmatter: '', body: text };
        }

        const endIndex = lines.findIndex((line, idx) => idx > 0 && line.trim() === '---');
        if (endIndex === -1) {
            return { frontmatter: '', body: text };
        }

        // Include everything up to and including the "DO NOT EDIT" comment line
        let splitAt = endIndex + 1;
        for (let i = endIndex + 1; i < lines.length && i <= endIndex + 3; i++) {
            if (lines[i].includes('DO NOT EDIT')) {
                splitAt = i + 1;
                break;
            }
            if (lines[i].trim() === '') {
                continue;
            }
        }

        const frontmatter = lines.slice(0, splitAt).join('\n') + '\n';
        const body = lines.slice(splitAt).join('\n');
        // Trim leading blank line from body if present
        return { frontmatter, body: body.replace(/^\n/, '') };
    }

    private _getFileName(sqlUriStr: string): string {
        const mdUriStr = this._mdUriByDocUri.get(sqlUriStr);
        if (!mdUriStr) return '';
        return path.basename(vscode.Uri.parse(mdUriStr).fsPath);
    }

    private _setupFileWatcher() {
        this._fileWatcher = vscode.workspace.createFileSystemWatcher('**/*.md');

        const handleChange = (changedUri: vscode.Uri) => {
            if (this._suppressFileWatch) return;
            if (!this._activeDocUri) return;

            const mdUriStr = this._mdUriByDocUri.get(this._activeDocUri);
            if (!mdUriStr || changedUri.toString() !== mdUriStr) return;

            const isDirty = this._dirtyByDocUri.get(this._activeDocUri) ?? false;
            if (isDirty) {
                // Notify webview of external change
                if (this._view) {
                    this._view.webview.postMessage({ command: 'externalChange' });
                }
            } else {
                // Auto-reload
                this.reloadFromDisk();
            }
        };

        this._fileWatcher.onDidChange(handleChange);
        this._fileWatcher.onDidCreate(handleChange);
        this._fileWatcher.onDidDelete((deletedUri) => {
            if (!this._activeDocUri) return;
            const mdUriStr = this._mdUriByDocUri.get(this._activeDocUri);
            if (!mdUriStr || deletedUri.toString() !== mdUriStr) return;

            this._contentByDocUri.delete(this._activeDocUri);
            this._frontmatterByDocUri.delete(this._activeDocUri);
            this._dirtyByDocUri.delete(this._activeDocUri);
            if (this._view) {
                this._view.webview.postMessage({
                    command: 'showEmpty',
                    data: { reason: 'file-deleted' }
                });
            }
        });

        this._context.subscriptions.push(this._fileWatcher);
    }

    private _getWebviewContent(webview: vscode.Webview) {
        const scriptUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'dist', 'markdownViewApp.js'));
        const cssUri = webview.asWebviewUri(vscode.Uri.joinPath(this._extensionUri, 'dist', 'markdownViewApp.css'));

        return `<!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="Content-Security-Policy"
                content="default-src 'none';
                style-src ${webview.cspSource} 'unsafe-inline';
                script-src ${webview.cspSource} 'unsafe-inline' 'unsafe-eval';">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Markdown</title>
            <link rel="stylesheet" href="${cssUri}">
        </head>
        <body>
            <div id="root" style="height: 100vh; width: 100vw; overflow: hidden;"></div>
            <script src="${scriptUri}"></script>
        </body>
        </html>`;
    }
}
