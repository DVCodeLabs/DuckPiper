
import * as vscode from 'vscode';
import { HistoryService, HistoryEntry } from '../services/historyService';

// Union type for tree items
type RecallItem = HistoryEntry | HistoryGroup;

interface HistoryGroup {
    label: string;
    entries: HistoryEntry[];
    type: 'group';
}

export class MemoryRecallProvider implements vscode.TreeDataProvider<RecallItem> {
    public static readonly viewType = 'dp.memoryRecallView';

    private _onDidChangeTreeData: vscode.EventEmitter<RecallItem | undefined | null | void> = new vscode.EventEmitter<RecallItem | undefined | null | void>();
    readonly onDidChangeTreeData: vscode.Event<RecallItem | undefined | null | void> = this._onDidChangeTreeData.event;

    constructor() { }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: RecallItem): vscode.TreeItem {
        if (this.isGroup(element)) {
            const item = new vscode.TreeItem(element.label,
                element.label === "Today" ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed);
            item.contextValue = 'group';
            item.iconPath = new vscode.ThemeIcon('calendar');
            return item;
        } else {
            // HistoryEntry
            const item = new vscode.TreeItem(
                this.truncateQuery(element.query),
                vscode.TreeItemCollapsibleState.None
            );

            item.description = this.formatDescription(element);
            item.tooltip = new vscode.MarkdownString(`**Query**\n\n\`\`\`sql\n${element.query}\n\`\`\`\n\nExecuted: ${new Date(element.timestamp).toLocaleString()}\nConnection: ${element.connectionName}\nSchema: ${element.schemaName || 'N/A'}`);
            item.iconPath = new vscode.ThemeIcon('history');
            item.contextValue = 'queryItem';

            item.command = {
                command: 'dp.memoryRecall.openQuery',
                title: 'Open Query',
                arguments: [element]
            };
            return item;
        }
    }

    getChildren(element?: RecallItem): vscode.ProviderResult<RecallItem[]> {
        if (element) {
            // Group children
            if (this.isGroup(element)) {
                return element.entries;
            }
            return [];
        }

        // Root
        const service = HistoryService.getInstance();
        const entries = service.getEntries();
        if (entries.length === 0) return [];

        // Bucketing logic
        const groups: Record<string, HistoryEntry[]> = {
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

        for (const entry of entries) {
            const d = new Date(entry.timestamp);
            if (d >= today) groups["Today"].push(entry);
            else if (d >= yesterday) groups["Yesterday"].push(entry);
            else if (d >= lastWeek) groups["Last 7 Days"].push(entry);
            else if (d >= lastMonth) groups["Last 30 Days"].push(entry);
            else groups["Older"].push(entry);
        }

        const result: RecallItem[] = [];
        const order = ["Today", "Yesterday", "Last 7 Days", "Last 30 Days", "Older"];

        for (const key of order) {
            const groupEntries = groups[key];
            if (groupEntries.length > 0) {
                // Sort descending
                groupEntries.sort((a, b) => b.timestamp - a.timestamp);
                result.push({
                    label: key,
                    entries: groupEntries,
                    type: 'group'
                });
            }
        }

        return result;
    }

    private isGroup(element: RecallItem): element is HistoryGroup {
        return (element as HistoryGroup).type === 'group';
    }

    private truncateQuery(query: string): string {
        const flatten = query.replace(/\s+/g, ' ').trim();
        if (flatten.length > 50) {
            return flatten.substring(0, 50) + '...';
        }
        return flatten;
    }

    private formatDescription(element: HistoryEntry): string {
        const timeStr = new Date(element.timestamp).toLocaleTimeString();
        return `${timeStr} • ${element.connectionName} • ${element.schemaName}`;
    }
}
