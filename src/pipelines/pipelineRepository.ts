import * as vscode from 'vscode';
import * as path from 'path';
import { Logger } from '../core/logger';
import * as crypto from 'crypto';
import { ensureDPDirs, readJson, writeJson, fileExists } from '../core/fsWorkspace';
import { isProjectInitialized } from '../core/isProjectInitialized';
import { PipelineIndex, PipelineIndexEntry, PipelineLineage } from './types';

export class PipelineRepository {
    private static instance: PipelineRepository;

    public static getInstance(): PipelineRepository {
        if (!PipelineRepository.instance) {
            PipelineRepository.instance = new PipelineRepository();
        }
        return PipelineRepository.instance;
    }

    async initialize() {
        if (!(await isProjectInitialized())) return;

        await ensureDPDirs();
        const index = await this.loadIndex();

        // Scan for all .dpnb files
        const files = await vscode.workspace.findFiles('**/*.dpnb', '**/node_modules/**');
        const foundPaths = new Set<string>();
        let _changed = false;

        for (const file of files) {
            const relPath = vscode.workspace.asRelativePath(file, false);
            foundPaths.add(relPath);

            let entry = index.pipelines.find(p => p.notebookPath === relPath);
            if (!entry) {
                entry = await this.createEntryFromFile(file);
                index.pipelines.push(entry);
                _changed = true;
            }
        }

        // Cleanup deleted
        const initialCount = index.pipelines.length;
        index.pipelines = index.pipelines.filter(p => foundPaths.has(p.notebookPath));
        if (index.pipelines.length !== initialCount) _changed = true;

        // Force save if changed OR if file doesn't exist (implied by loadIndex returning default but likely not saved if empty)
        // Actually loadIndex checks existence. If it didn't exist, we got default. 
        // We want to ensure it exists on disk.
        // If changed is false, effectively we might still want to write if it was missing.
        // A simple way is to check if we should write.
        // Let's just always save on initialize to ensure freshness and existence.
        await this.saveIndex(index);
    }

    private async createEntryFromFile(uri: vscode.Uri): Promise<PipelineIndexEntry> {
        const relPath = vscode.workspace.asRelativePath(uri, false);
        const name = path.basename(relPath, '.dpnb');
        const pipelineId = crypto.randomUUID();
        const lineagePath = `DP/system/pipelines/${name}.lineage.json`;

        let createdAt = new Date().toISOString();
        try {
            const stat = await vscode.workspace.fs.stat(uri);
            createdAt = new Date(stat.ctime).toISOString();
        } catch { }

        // Check for companion markdown
        const mdUri = uri.with({ path: uri.path.replace(/\.dpnb$/i, '.md') });
        let docPath: string | undefined;
        if (await fileExists(mdUri)) {
            docPath = vscode.workspace.asRelativePath(mdUri, false);
        }

        return {
            pipelineId,
            name,
            notebookPath: relPath,
            docPath,
            lineagePath: lineagePath,
            duckdbPath: 'DP/system/piper.duckdb',
            updatedAt: new Date().toISOString(),
            createdAt
        };
    }

    async addPipeline(uri: vscode.Uri): Promise<void> {
        const index = await this.loadIndex();
        const relPath = vscode.workspace.asRelativePath(uri, false);

        // Check if exists
        if (index.pipelines.some(p => p.notebookPath === relPath)) return;

        const entry = await this.createEntryFromFile(uri);
        index.pipelines.push(entry);
        await this.saveIndex(index);
    }

    /**
     * Load the pipeline index from system/pipelines/pipelineIndex.json
     */
    async loadIndex(): Promise<PipelineIndex> {
        // Safe check without side effects
        if (!(await isProjectInitialized())) {
            return {
                version: "0.1",
                generatedAt: new Date().toISOString(),
                pipelines: []
            };
        }

        const dpDir = await ensureDPDirs();
        const indexUri = vscode.Uri.joinPath(dpDir, 'system', 'pipelines', 'pipelineIndex.json');

        if (await fileExists(indexUri)) {
            try {
                return await readJson<PipelineIndex>(indexUri);
            } catch (e) {
                Logger.warn('Failed to parse pipeline index', e);
            }
        }

        // Return default empty index
        return {
            version: "0.1",
            generatedAt: new Date().toISOString(),
            pipelines: []
        };
    }

    /**
     * Save the pipeline index
     */
    async saveIndex(index: PipelineIndex): Promise<void> {
        if (!(await isProjectInitialized())) return;

        const dpDir = await ensureDPDirs();
        const indexUri = vscode.Uri.joinPath(dpDir, 'system', 'pipelines', 'pipelineIndex.json');
        index.generatedAt = new Date().toISOString();
        await writeJson(indexUri, index);
    }

    /**
     * Update or add a pipeline entry in the index
     */
    async updatePipelineEntry(entry: PipelineIndexEntry): Promise<void> {
        const index = await this.loadIndex();
        const existingIdx = index.pipelines.findIndex(p => p.pipelineId === entry.pipelineId);

        // helper to get creation time
        const enrichWithCreatedAt = async (e: PipelineIndexEntry): Promise<PipelineIndexEntry> => {
            if (e.createdAt) return e;

            // Try to find file stats
            try {
                if (vscode.workspace.workspaceFolders) {
                    const root = vscode.workspace.workspaceFolders[0].uri;
                    const notebookUri = vscode.Uri.joinPath(root, e.notebookPath);
                    const stat = await vscode.workspace.fs.stat(notebookUri);
                    e.createdAt = new Date(stat.ctime).toISOString();
                } else {
                    e.createdAt = new Date().toISOString();
                }
            } catch (_err) {
                // If checking fails (deleted file?), use current time or keep undefined if interface allowed (but it's required string)
                e.createdAt = new Date().toISOString();
            }
            return e;
        };

        const enrichedEntry = await enrichWithCreatedAt(entry);

        if (existingIdx !== -1) {
            // Update existing
            // specific logic: if existing entry has createdAt, preserve it unless new entry explicitly has it (which enrichedEntry covers)
            // But wait, enrichedEntry will define it.
            // Actually, we should check if *stored* entry has it to avoid re-statting every time? 
            // The request says "createdAt... work the same way query works". 
            // in QueryIndex.ts: it stats file every time updateFile is called.
            // here updatePipelineEntry is called mainly by LineageTracker which builds a fresh entry object.

            index.pipelines[existingIdx] = {
                ...index.pipelines[existingIdx],
                ...enrichedEntry,
                updatedAt: new Date().toISOString()
            };
        } else {
            // Add new
            index.pipelines.push({
                ...enrichedEntry,
                updatedAt: new Date().toISOString()
            });
        }

        await this.saveIndex(index);
    }

    /**
     * Delete a pipeline entry and its lineage file
     */
    async deletePipeline(notebookUri: vscode.Uri): Promise<void> {
        const index = await this.loadIndex();
        const relativePath = vscode.workspace.asRelativePath(notebookUri);
        const idx = index.pipelines.findIndex(p => p.notebookPath === relativePath);

        if (idx === -1) return;

        const entry = index.pipelines[idx];

        // 1. Delete companion markdown doc
        const mdUri = notebookUri.with({ path: notebookUri.path.replace(/\.dpnb$/i, '.md') });
        if (await fileExists(mdUri)) {
            try {
                await vscode.workspace.fs.delete(mdUri);
            } catch (e) {
                Logger.warn(`Failed to delete companion markdown file ${mdUri.fsPath}`, e);
            }
        }

        // 2. Delete lineage file
        if (entry.lineagePath && vscode.workspace.workspaceFolders) {
            const root = vscode.workspace.workspaceFolders[0].uri;
            const lineageUri = vscode.Uri.joinPath(root, entry.lineagePath);
            if (await fileExists(lineageUri)) {
                try {
                    await vscode.workspace.fs.delete(lineageUri);
                } catch (e) {
                    Logger.warn(`Failed to delete lineage file ${lineageUri.fsPath}`, e);
                }
            }
        }

        // 2. Remove from index
        index.pipelines.splice(idx, 1);
        await this.saveIndex(index);
    }

    /**
     * Get the URI for a lineage file based on notebook basename
     */
    async getLineageFileUri(notebookUri: vscode.Uri): Promise<vscode.Uri> {
        const root = vscode.workspace.workspaceFolders?.[0]?.uri;
        if (!root) {
            const { formatFileSystemError } = require('../core/errorHandler');
            throw new Error(formatFileSystemError(
                'Workspace access',
                'No workspace folder open',
                'Open a folder in VS Code to use pipeline features'
            ));
        }
        const baseName = path.basename(notebookUri.fsPath, '.dpnb');
        return vscode.Uri.joinPath(root, 'DP', 'system', 'pipelines', `${baseName}.lineage.json`);
    }

    /**
     * Load lineage for a specific pipeline
     */
    async loadLineage(pipelineId: string): Promise<PipelineLineage | null> {
        const index = await this.loadIndex();
        const entry = index.pipelines.find(p => p.pipelineId === pipelineId);

        if (!entry) return null;

        // Construct absolute URI from workspace-relative path
        // lineagePath is "DP/system/pipelines/foo.lineage.json" relative to workspace root
        if (!vscode.workspace.workspaceFolders) return null;

        const root = vscode.workspace.workspaceFolders[0].uri;
        const lineageUri = vscode.Uri.joinPath(root, entry.lineagePath);

        if (await fileExists(lineageUri)) {
            try {
                return await readJson<PipelineLineage>(lineageUri);
            } catch (e) {
                Logger.warn(`Failed to read lineage file ${lineageUri.fsPath}`, e);
            }
        }
        return null;
    }

    /**
     * Save lineage to file
     */
    async saveLineage(lineage: PipelineLineage): Promise<void> {
        if (!vscode.workspace.workspaceFolders) return;
        const root = vscode.workspace.workspaceFolders[0].uri;

        // Find correct path from notebook name in lineage object? 
        // Or rely on the fact we usually update index alongside.
        // Let's use getLineageFileUri logic derived from notebookPath provided in lineage
        const notebookUri = vscode.Uri.joinPath(root, lineage.notebookPath);
        const lineageUri = await this.getLineageFileUri(notebookUri);

        await writeJson(lineageUri, lineage);
    }

    /**
     * Handle notebook rename
     */
    async renameNotebook(oldUri: vscode.Uri, newUri: vscode.Uri): Promise<void> {
        // 0. Handle Markdown Doc Rename (Generic for all notebooks)
        const oldMdUri = oldUri.with({ path: oldUri.path.replace(/\.dpnb$/i, '.md') });
        const newMdUri = newUri.with({ path: newUri.path.replace(/\.dpnb$/i, '.md') });

        if (await fileExists(oldMdUri)) {
            try {
                // Rename file
                await vscode.workspace.fs.rename(oldMdUri, newMdUri, { overwrite: false });

                // Update Title in Frontmatter
                // Read as buffer, decode, patch, write
                const bytes = await vscode.workspace.fs.readFile(newMdUri);
                const content = new TextDecoder("utf-8").decode(bytes);

                const newTitle = path.basename(newUri.fsPath, '.dpnb')
                    .split(/[_-]/) // split on underscore or dash
                    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
                    .join(' ');

                // Replace title: "..." with new title
                // We use multiline regex to hit the frontmatter property
                const updated = content.replace(/^title: ".*"$/m, `title: "${newTitle}"`);

                if (updated !== content) {
                    await vscode.workspace.fs.writeFile(newMdUri, Buffer.from(updated, "utf8"));
                }
            } catch (e) {
                Logger.warn("DuckPiper: Failed to rename associated markdown doc", e);
            }
        }

        const index = await this.loadIndex();
        const oldPath = vscode.workspace.asRelativePath(oldUri);

        const entry = index.pipelines.find(p => p.notebookPath === oldPath);
        if (!entry) return;

        // 1. Rename lineage file
        const oldLineageUri = await this.getLineageFileUri(oldUri);
        const newLineageUri = await this.getLineageFileUri(newUri);

        if (await fileExists(oldLineageUri)) {
            try {
                // Check if target exists (conflict rule: "rename to Y_2" - simplified here to overwrite for now or error?)
                // Spec: "if Y.lineage.json exists... rename to Y_2".
                // Let's simple move for V2 alpha.
                await vscode.workspace.fs.rename(oldLineageUri, newLineageUri, { overwrite: false });

                // Update lineage content (notebookPath)
                const lineage = await readJson<PipelineLineage>(newLineageUri);
                lineage.notebookPath = vscode.workspace.asRelativePath(newUri);
                await writeJson(newLineageUri, lineage);

            } catch (e) {
                Logger.warn(`Failed to move lineage file`, e);
            }
        }

        // 2. Update Index
        entry.notebookPath = vscode.workspace.asRelativePath(newUri);
        entry.name = path.basename(newUri.fsPath, '.dpnb');
        entry.lineagePath = vscode.workspace.asRelativePath(newLineageUri);

        // Update docPath if it existed/renamed
        if (entry.docPath || await fileExists(newMdUri)) {
            entry.docPath = vscode.workspace.asRelativePath(newMdUri, false);
        }

        entry.updatedAt = new Date().toISOString();

        await this.saveIndex(index);
    }
    /**
     * List all known pipelines
     */
    async listPipelines(): Promise<PipelineIndexEntry[]> {
        const index = await this.loadIndex();
        return index.pipelines;
    }

    /**
     * Find pipeline entry by notebook URI
     */
    async findPipelineByUri(notebookUri: vscode.Uri): Promise<PipelineIndexEntry | undefined> {
        const index = await this.loadIndex();
        const relativePath = vscode.workspace.asRelativePath(notebookUri);
        return index.pipelines.find(p => p.notebookPath === relativePath);
    }

    /**
     * Update status of a table in ALL known pipelines
     */
    async updateTableStatusInAllPipelines(fqn: string, status: 'present' | 'missing' | 'deleted', by: 'user' | 'system'): Promise<void> {
        const pipelines = await this.listPipelines();

        for (const p of pipelines) {
            // Check if linage file exists
            if (!p.lineagePath) continue;

            try {
                // Determine absolute path
                // p.lineagePath is relative to workspace root
                if (!vscode.workspace.workspaceFolders) continue;
                const root = vscode.workspace.workspaceFolders[0].uri;
                const lineageUri = vscode.Uri.joinPath(root, p.lineagePath);

                if (await fileExists(lineageUri)) {
                    const lineage = await readJson<PipelineLineage>(lineageUri);
                    let changed = false;

                    for (const table of lineage.tables) {
                        if (table.fqn === fqn) {
                            table.status = status;
                            if (status === 'deleted') {
                                table.deletedAt = new Date().toISOString();
                                table.deletedBy = by;
                            }
                            changed = true;
                        }
                    }

                    if (changed) {
                        await writeJson(lineageUri, lineage);
                    }
                }
            } catch (e) {
                Logger.error(`Failed to update table status for pipeline ${p.pipelineId}`, e);
            }
        }
    }

    /**
     * Update status of all tables in a schema/layer in ALL known pipelines
     */
    async updateLayerStatusInAllPipelines(layer: string, status: 'present' | 'missing' | 'deleted', by: 'user' | 'system'): Promise<void> {
        const pipelines = await this.listPipelines();

        for (const p of pipelines) {
            try {
                if (!vscode.workspace.workspaceFolders) continue;
                const root = vscode.workspace.workspaceFolders[0].uri;
                const lineageUri = vscode.Uri.joinPath(root, p.lineagePath);

                if (await fileExists(lineageUri)) {
                    const lineage = await readJson<PipelineLineage>(lineageUri);
                    let changed = false;

                    for (const table of lineage.tables) {
                        if (table.schema === layer) {
                            table.status = status;
                            if (status === 'deleted') {
                                table.deletedAt = new Date().toISOString();
                                table.deletedBy = by;
                            }
                            changed = true;
                        }
                    }

                    if (changed) {
                        await writeJson(lineageUri, lineage);
                    }
                }
            } catch (e) {
                Logger.error(`Failed to update layer status for pipeline ${p.pipelineId}`, e);
            }
        }
    }
}
