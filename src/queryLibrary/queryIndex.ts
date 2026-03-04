import * as vscode from 'vscode';
import { canonicalizeSql } from '../core/hashing';
import { ensureDPDirs, readJson, writeJson, fileExists } from '../core/fsWorkspace';
import { QueryIndexEntry, QueryIndexFile } from './queryIndexer';
import { parseMdMetadata, buildSearchText } from './mdParser';
import { Logger } from '../core/logger';

export { QueryIndexEntry }; // Re-export for convenience

export class QueryIndex {
    // Map hash -> list of locations
    private index = new Map<string, QueryIndexEntry[]>();

    // Map path -> entry (PRIMARY lookup for persistence)
    private pathIndex = new Map<string, QueryIndexEntry>();

    private initialized = false;
    private persistencePending = false;

    // Event emitter for search index changes
    private _onDidChange = new vscode.EventEmitter<void>();
    readonly onDidChange = this._onDidChange.event;

    constructor() { }

    async initialize() {
        if (this.initialized) return;

        // 1. Load existing JSON
        await this.loadFromDisk();

        // 2. Find all SQL files to sync/add new ones
        const files = await vscode.workspace.findFiles('**/*.{sql,postgres}', '**/node_modules/**');


        for (const file of files) {
            await this.updateFile(file, true); // true = skip save, we'll save once at end
        }

        // 3. Watch for SQL changes
        const watcher = vscode.workspace.createFileSystemWatcher('**/*.{sql,postgres}');
        watcher.onDidChange(uri => { this.updateFile(uri); });
        watcher.onDidCreate(uri => { this.updateFile(uri); });
        watcher.onDidDelete(uri => { this.removeFile(uri); });

        // 4. Watch for companion markdown changes
        const mdWatcher = vscode.workspace.createFileSystemWatcher('**/*.md');
        mdWatcher.onDidChange(uri => { this.handleMdChange(uri); });
        mdWatcher.onDidCreate(uri => { this.handleMdChange(uri); });
        mdWatcher.onDidDelete(uri => { this.handleMdDelete(uri); });

        this.initialized = true;


        // Initial persist to cleanup stale entries or add new ones
        await this.persist();
    }

    private async loadFromDisk() {
        const dpDir = await ensureDPDirs();
        const indexUri = vscode.Uri.joinPath(dpDir, "system", "queries", "queryIndex.json");

        if (await fileExists(indexUri)) {
            try {
                const data = await readJson<QueryIndexFile>(indexUri);
                if (data && data.queries) {
                    for (const q of data.queries) {
                        this.pathIndex.set(q.path, q);
                        // Also populate hash index
                        if (!this.index.has(q.sqlHash)) {
                            this.index.set(q.sqlHash, []);
                        }
                        this.index.get(q.sqlHash)?.push(q);
                    }
                }
            } catch (e) {
                Logger.error("[QueryIndex] Failed to load queryIndex.json", e);
            }
        }
    }

    private async persist() {
        if (this.persistencePending) return;
        this.persistencePending = true;

        // Debounce slightly
        setTimeout(async () => {
            try {
                const dpDir = await ensureDPDirs();
                const indexUri = vscode.Uri.joinPath(dpDir, "system", "queries", "queryIndex.json");

                const entries = Array.from(this.pathIndex.values())
                    .sort((a, b) => a.path.localeCompare(b.path))
                    .map(e => ({
                        connectionId: e.connectionId,
                        connectionName: e.connectionName,
                        createdAt: e.createdAt,
                        dialect: e.dialect,
                        docPath: e.docPath,
                        lastRunAt: e.lastRunAt,
                        mdBodyText: e.mdBodyText,
                        mdSummary: e.mdSummary,
                        mdTags: e.mdTags,
                        mdTitle: e.mdTitle,
                        path: e.path,
                        searchText: e.searchText,
                        searchUpdatedAt: e.searchUpdatedAt,
                        sqlHash: e.sqlHash,
                        title: e.title,
                        updatedAt: e.updatedAt
                    }));

                const file: QueryIndexFile = {
                    version: "0.1",
                    generatedAt: new Date().toISOString(),
                    queries: entries
                };

                await writeJson(indexUri, file);
                this.persistencePending = false;
                this._onDidChange.fire();
            } catch (e) {
                Logger.error("[QueryIndex] Failed to save queryIndex.json", e);
                this.persistencePending = false;
            }
        }, 500);
    }

    private isTracked(uri: vscode.Uri): boolean {
        // Only track file scheme
        if (uri.scheme !== 'file') return false;

        const path = uri.path.toLowerCase();
        return path.endsWith('.sql') || path.endsWith('.postgres');
    }

    async updateFile(uri: vscode.Uri, skipSave = false) {
        if (!this.isTracked(uri)) return;

        try {
            const document = await vscode.workspace.openTextDocument(uri);
            const text = document.getText();
            const { sqlHash } = canonicalizeSql(text);
            const wsRelative = vscode.workspace.asRelativePath(uri, false);
            const title = this.extractTitle(text);



            // Check for companion md and extract search metadata
            let docPath: string | undefined;
            let mdMeta: ReturnType<typeof parseMdMetadata> | undefined;
            try {
                const mdUri = uri.with({ path: uri.path.replace(/\.sql$/i, '.md') });
                try {
                    await vscode.workspace.fs.stat(mdUri);
                    docPath = vscode.workspace.asRelativePath(mdUri, false);
                    // Read and parse markdown for search metadata
                    const mdBytes = await vscode.workspace.fs.readFile(mdUri);
                    const mdContent = Buffer.from(mdBytes).toString('utf8');
                    mdMeta = parseMdMetadata(mdContent);
                } catch {
                    // No markdown file
                }
            } catch (_e) {
                // ignore
            }

            let entry = this.pathIndex.get(wsRelative);

            // Get stats for timestamps
            let createdAt = new Date().toISOString(); // fallback
            let updatedAt = new Date().toISOString();
            try {
                const stat = await vscode.workspace.fs.stat(uri);
                createdAt = new Date(stat.ctime).toISOString();
                updatedAt = new Date(stat.mtime).toISOString();
            } catch { }

            if (entry) {
                // Update existing
                entry.sqlHash = sqlHash;
                entry.updatedAt = updatedAt;
                entry.title = title;
                entry.docPath = docPath;
            } else {
                // New entry
                entry = {
                    path: wsRelative,
                    sqlHash,
                    createdAt: createdAt,
                    updatedAt: updatedAt,
                    title,
                    docPath
                };
                this.pathIndex.set(wsRelative, entry);
            }

            // Apply markdown metadata if available
            if (mdMeta) {
                entry.mdTitle = mdMeta.title;
                entry.mdTags = mdMeta.tags;
                entry.mdSummary = mdMeta.summary;
                entry.mdBodyText = mdMeta.bodyText;
                if (mdMeta.connectionId) entry.connectionId = mdMeta.connectionId;
                if (mdMeta.connectionName) entry.connectionName = mdMeta.connectionName;
                if (mdMeta.dialect) entry.dialect = mdMeta.dialect;
            } else {
                entry.mdTitle = undefined;
                entry.mdTags = undefined;
                entry.mdSummary = undefined;
                entry.mdBodyText = undefined;
            }

            // Build searchText
            entry.searchText = buildSearchText({
                title: entry.title,
                mdTitle: entry.mdTitle,
                mdTags: entry.mdTags,
                mdBodyText: entry.mdBodyText,
                path: entry.path,
                connectionName: entry.connectionName ?? undefined,
                dialect: entry.dialect ?? undefined,
                sqlText: text,
            });
            entry.searchUpdatedAt = new Date().toISOString();

            this.rebuildHashIndex();

            if (!skipSave) await this.persist();

        } catch (e) {
            Logger.error(`[QueryIndex] Failed to update ${uri.toString()}`, e);
        }
    }

    /**
     * Handle companion markdown file change — find associated SQL entry and update search fields.
     */
    private async handleMdChange(mdUri: vscode.Uri) {
        if (mdUri.scheme !== 'file') return;
        // Find the associated SQL file path
        const sqlPath = mdUri.path.replace(/\.md$/i, '.sql');
        const sqlUri = mdUri.with({ path: sqlPath });

        // Check if we track a SQL file with this companion
        const wsRelative = vscode.workspace.asRelativePath(sqlUri, false);
        const entry = this.pathIndex.get(wsRelative);
        if (!entry) return;

        // Re-parse the markdown and update entry
        try {
            const mdBytes = await vscode.workspace.fs.readFile(mdUri);
            const mdContent = Buffer.from(mdBytes).toString('utf8');
            const mdMeta = parseMdMetadata(mdContent);

            entry.docPath = vscode.workspace.asRelativePath(mdUri, false);
            entry.mdTitle = mdMeta.title;
            entry.mdTags = mdMeta.tags;
            entry.mdSummary = mdMeta.summary;
            entry.mdBodyText = mdMeta.bodyText;
            if (mdMeta.connectionId) entry.connectionId = mdMeta.connectionId;
            if (mdMeta.connectionName) entry.connectionName = mdMeta.connectionName;
            if (mdMeta.dialect) entry.dialect = mdMeta.dialect;

            // Re-read SQL text for searchText rebuild
            let sqlText = '';
            try {
                const sqlBytes = await vscode.workspace.fs.readFile(sqlUri);
                sqlText = Buffer.from(sqlBytes).toString('utf8');
            } catch { }

            entry.searchText = buildSearchText({
                title: entry.title,
                mdTitle: entry.mdTitle,
                mdTags: entry.mdTags,
                mdBodyText: entry.mdBodyText,
                path: entry.path,
                connectionName: entry.connectionName ?? undefined,
                dialect: entry.dialect ?? undefined,
                sqlText,
            });
            entry.searchUpdatedAt = new Date().toISOString();

            await this.persist();
        } catch (e) {
            Logger.error(`[QueryIndex] Failed to update md metadata for ${mdUri.toString()}`, e);
        }
    }

    /**
     * Handle companion markdown file deletion — clear search metadata from associated entry.
     */
    private handleMdDelete(mdUri: vscode.Uri) {
        if (mdUri.scheme !== 'file') return;
        const sqlPath = mdUri.path.replace(/\.md$/i, '.sql');
        const sqlUri = mdUri.with({ path: sqlPath });

        const wsRelative = vscode.workspace.asRelativePath(sqlUri, false);
        const entry = this.pathIndex.get(wsRelative);
        if (!entry) return;

        entry.docPath = undefined;
        entry.mdTitle = undefined;
        entry.mdTags = undefined;
        entry.mdSummary = undefined;
        entry.mdBodyText = undefined;
        // Rebuild searchText without md fields
        entry.searchText = buildSearchText({
            title: entry.title,
            path: entry.path,
            connectionName: entry.connectionName ?? undefined,
            dialect: entry.dialect ?? undefined,
        });
        entry.searchUpdatedAt = new Date().toISOString();

        this.persist();
    }

    private extractTitle(sql: string): string | undefined {
        const lines = sql.split(/\r?\n/);
        for (const raw of lines) {
            const line = raw.trim();
            if (!line) continue;
            if (line.startsWith("--")) return line.replace(/^--\s?/, "").trim() || undefined;
            if (line.startsWith("/*")) return undefined;
            return undefined;
        }
        return undefined;
    }

    async handleRename(oldUri: vscode.Uri, newUri: vscode.Uri) {
        const oldRel = vscode.workspace.asRelativePath(oldUri, false);
        const newRel = vscode.workspace.asRelativePath(newUri, false);

        // If new extension is not supported, treat as deletion
        if (!newRel.endsWith('.sql') && !newRel.endsWith('.postgres')) {
            this.removeFile(oldUri);
            return;
        }



        const entry = this.pathIndex.get(oldRel);
        if (entry) {
            // Update path
            entry.path = newRel;
            if (entry.docPath) {
                const oldMd = oldRel.replace(/\.sql$/i, '.md');
                // Only update docPath if it matched the old pattern
                if (entry.docPath === oldMd) {
                    entry.docPath = newRel.replace(/\.sql$/i, '.md');
                }
            }
            entry.updatedAt = new Date().toISOString();

            // Move in map
            this.pathIndex.delete(oldRel);
            this.pathIndex.set(newRel, entry);

            // Rebuild hash index (could be optimized)
            this.rebuildHashIndex();
            await this.persist();
        } else {
            Logger.warn(`[QueryIndex] Rename source not found in index: ${oldRel}. Treating as new file.`);
            await this.updateFile(newUri);
        }
    }

    removeFile(uri: vscode.Uri) {
        const wsRelative = vscode.workspace.asRelativePath(uri, false);
        if (this.pathIndex.delete(wsRelative)) {
            this.rebuildHashIndex();
            this.persist();
        }
    }

    // Updates metadata when connection changes
    async updateConnectionContext(uri: vscode.Uri, connId: string | null, connName: string | null, dialect: string | null) {
        if (!this.isTracked(uri)) return;

        const wsRelative = vscode.workspace.asRelativePath(uri, false);
        const entry = this.pathIndex.get(wsRelative);

        if (entry) {
            entry.connectionId = connId;
            entry.connectionName = connName;
            entry.dialect = dialect;
            entry.updatedAt = new Date().toISOString();
            await this.persist();

        } else {
            // Should exist if file is open, but if not, force update?
            await this.updateFile(uri, true);
            // recursive retry once?
            const retry = this.pathIndex.get(wsRelative);
            if (retry) {
                retry.connectionId = connId;
                retry.connectionName = connName;
                retry.dialect = dialect;
                await this.persist();
            }
        }
    }

    async updateLastRun(uri: vscode.Uri) {
        if (!this.isTracked(uri)) return;

        const wsRelative = vscode.workspace.asRelativePath(uri, false);
        let entry = this.pathIndex.get(wsRelative);

        if (!entry) {
            // Ensure it exists
            await this.updateFile(uri, true);
            entry = this.pathIndex.get(wsRelative);
        }

        if (entry) {
            entry.lastRunAt = new Date().toISOString();
            await this.persist();
        }
    }

    getEntry(uri: vscode.Uri): QueryIndexEntry | undefined {
        const wsRelative = vscode.workspace.asRelativePath(uri, false);
        return this.pathIndex.get(wsRelative);
    }

    /**
     * Returns all index entries (for search).
     */
    getAllEntries(): QueryIndexEntry[] {
        return Array.from(this.pathIndex.values());
    }

    /**
     * Force rebuild search metadata for all entries.
     */
    async rebuildSearchMetadata(): Promise<void> {
        const entries = this.getAllEntries();
        for (const entry of entries) {
            const root = vscode.workspace.workspaceFolders?.[0]?.uri;
            if (!root) continue;

            const sqlUri = vscode.Uri.joinPath(root, entry.path);
            await this.updateFile(sqlUri, true);
        }
        await this.persist();
    }

    private rebuildHashIndex() {
        this.index.clear();
        for (const entry of this.pathIndex.values()) {
            if (!this.index.has(entry.sqlHash)) {
                this.index.set(entry.sqlHash, []);
            }
            this.index.get(entry.sqlHash)?.push(entry);
        }
    }

    /**
     * Returns all entries matching the given hash
     */
    getMatches(hash: string): QueryIndexEntry[] {
        return this.index.get(hash) || [];
    }
}

export const queryIndex = new QueryIndex();
