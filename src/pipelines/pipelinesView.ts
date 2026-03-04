import * as vscode from 'vscode';
import { Logger } from '../core/logger';
import { PipelineRepository } from './pipelineRepository';
import { PipelineIndexEntry } from './types';

export class PipelinesViewProvider implements vscode.TreeDataProvider<PipelineItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<PipelineItem | void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;
    private repo: PipelineRepository;

    constructor() {
        this.repo = PipelineRepository.getInstance();
    }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: PipelineItem): vscode.TreeItem {
        return element;
    }

    async getChildren(element?: PipelineItem): Promise<PipelineItem[]> {
        if (element && element.children) return element.children;
        if (element) return [];

        try {
            const index = await this.repo.loadIndex();

            if (index.pipelines.length === 0) {
                return [new PipelineItem(
                    "No pipelines found",
                    "empty",
                    vscode.TreeItemCollapsibleState.None,
                    undefined
                )];
            }

            const pipelines = index.pipelines;

            // Grouping Logic
            const groups: Record<string, PipelineIndexEntry[]> = {
                "Today": [],
                "Yesterday": [],
                "Last 7 Days": [],
                "Last 30 Days": [],
                "Older": []
            };

            const now = new Date();
            const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
            const yesterday = new Date(today);
            yesterday.setDate(today.getDate() - 1);
            const lastWeek = new Date(today);
            lastWeek.setDate(today.getDate() - 7);
            const lastMonth = new Date(today);
            lastMonth.setDate(today.getDate() - 30);

            for (const p of pipelines) {
                const dateStr = p.lastRunAt || p.updatedAt;
                const d = new Date(dateStr);

                if (d >= today) groups["Today"].push(p);
                else if (d >= yesterday) groups["Yesterday"].push(p);
                else if (d >= lastWeek) groups["Last 7 Days"].push(p);
                else if (d >= lastMonth) groups["Last 30 Days"].push(p);
                else groups["Older"].push(p);
            }

            const result: PipelineItem[] = [];
            const order = ["Today", "Yesterday", "Last 7 Days", "Last 30 Days", "Older"];

            for (const key of order) {
                const groupEntries = groups[key];
                if (groupEntries.length > 0) {
                    // Sort within group (newest first)
                    groupEntries.sort((a, b) => {
                        const da = new Date(a.lastRunAt || a.updatedAt);
                        const db = new Date(b.lastRunAt || b.updatedAt);
                        return da < db ? 1 : -1;
                    });

                    const children = groupEntries.map(entry => {
                        const item = new PipelineItem(
                            entry.name || "Untitled Pipeline",
                            entry.pipelineId,
                            vscode.TreeItemCollapsibleState.None,
                            entry
                        );

                        // Adjust description
                        const dStr = entry.lastRunAt || entry.updatedAt;
                        if (dStr) {
                            if (key === "Today" || key === "Yesterday") {
                                item.description = new Date(dStr).toLocaleTimeString();
                            } else {
                                item.description = new Date(dStr).toLocaleString();
                            }
                        }
                        return item;
                    });

                    const groupItem = PipelineItem.group(key, children);
                    result.push(groupItem);
                }
            }

            return result;

        } catch (e) {
            Logger.warn('Failed to load pipeline items', e);
            return [];
        }
    }
}

export class PipelineItem extends vscode.TreeItem {
    constructor(
        public readonly name: string,
        public readonly pipelineId: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly entry?: PipelineIndexEntry,
        public readonly children?: PipelineItem[]
    ) {
        super(name, children ? vscode.TreeItemCollapsibleState.Expanded : collapsibleState);

        if (pipelineId === "empty") {
            this.iconPath = new vscode.ThemeIcon("info");
            this.contextValue = "empty";
        } else if (children) {
            // Group Item
            this.iconPath = new vscode.ThemeIcon("calendar");
            this.contextValue = "group";
        } else {
            // Pipeline Item
            this.iconPath = new vscode.ThemeIcon("git-commit");
            this.contextValue = "pipeline";

            if (entry) {
                const root = vscode.workspace.workspaceFolders?.[0]?.uri;
                if (root && entry.notebookPath) {
                    const uri = vscode.Uri.joinPath(root, entry.notebookPath);
                    this.command = {
                        command: "vscode.open",
                        title: "Open Pipeline",
                        arguments: [uri]
                    };
                }

                // Tooltip info
                const updated = new Date(entry.updatedAt).toLocaleString();
                const lastRun = entry.lastRunAt ? new Date(entry.lastRunAt).toLocaleString() : "Never";
                this.tooltip = new vscode.MarkdownString(`**${name}**\n\nPath: ${entry.notebookPath}\nUpdated: ${updated}\nLast Run: ${lastRun}`);

                // Description is set by caller (grouping logic) or defaults here
                if (!this.description) {
                    this.description = entry.lastRunAt ? "Active" : "";
                }
            }
        }
    }

    static group(label: string, children: PipelineItem[]): PipelineItem {
        const state = label === "Today" ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed;
        return new PipelineItem(label, "group", state, undefined, children);
    }
}
