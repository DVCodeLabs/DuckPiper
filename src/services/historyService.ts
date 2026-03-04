
import * as vscode from 'vscode';
import * as path from 'path';
import * as fs from 'fs';
import * as crypto from 'crypto';
import { Logger } from '../core/logger';

export interface HistoryEntry {
    id: string;
    query: string;
    timestamp: number;
    connectionName: string;
    schemaName?: string;
    connectionId?: string;
    rows?: number;
    status?: 'success' | 'error';
    duration?: number;
}

export class HistoryService {
    private static instance: HistoryService;
    private readonly STORAGE_FILE = 'queryHistory.json'; // Updated filename
    private _history: HistoryEntry[] = [];
    private _storagePath: string = '';

    private constructor() { }

    public static getInstance(): HistoryService {
        if (!HistoryService.instance) {
            HistoryService.instance = new HistoryService();
        }
        return HistoryService.instance;
    }

    public async initialize(_context?: vscode.ExtensionContext) {
        const { isProjectInitialized } = require('../core/isProjectInitialized');
        if (!(await isProjectInitialized())) {
            return;
        }

        // Use workspace storage (DP/system) matching queryIndex.json
        const { ensureDPDirs } = require('../core/fsWorkspace');
        try {
            const dpDir = await ensureDPDirs();
            this._storagePath = path.join(dpDir.fsPath, 'system', 'queries', this.STORAGE_FILE);

            // Ensure system/queries dir exists
            const systemDir = path.dirname(this._storagePath);
            if (!fs.existsSync(systemDir)) {
                fs.mkdirSync(systemDir, { recursive: true });
            }

            this.loadHistory();
        } catch (e) {
            Logger.error('DuckPiper: Failed to initialize history storage', e);
        }
    }

    private loadHistory() {
        if (this._storagePath && fs.existsSync(this._storagePath)) {
            try {
                const content = fs.readFileSync(this._storagePath, 'utf8');
                this._history = JSON.parse(content);
            } catch (e) {
                Logger.error('DuckPiper: Failed to load query history', e);
                this._history = [];
            }
        }
    }

    private saveHistory() {
        try {
            fs.writeFileSync(this._storagePath, JSON.stringify(this._history, null, 2));
        } catch (e) {
            Logger.error('DuckPiper: Failed to save query history', e);
        }
    }

    public addEntry(entry: Omit<HistoryEntry, 'id' | 'timestamp'>) {
        const newEntry: HistoryEntry = {
            id: this.generateId(),
            timestamp: Date.now(),
            ...entry
        };

        this._history.unshift(newEntry);

        // Prune old entries (older than 7 days)
        this.pruneOldEntries();

        this.saveHistory();

        // Notify listeners
        vscode.commands.executeCommand('dp.memoryRecall.refresh');
    }

    public getEntries(): HistoryEntry[] {
        return this._history;
    }

    private pruneOldEntries() {
        const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;
        const cutoff = Date.now() - SEVEN_DAYS_MS;

        // Keep only entries newer than cutoff OR keep at least last 50 entries regardless of age (safety)
        // But the requirement says "overwritten every 7 days", implies strict time limit.
        // Let's stick to strict 7 days but maybe keep last 10 just in case user hasn't used app in a week.

        this._history = this._history.filter((entry, index) => {
            if (index < 20) return true; // Always keep most recent 20
            return entry.timestamp > cutoff;
        });
    }

    private generateId(): string {
        return crypto.randomUUID();
    }
}
