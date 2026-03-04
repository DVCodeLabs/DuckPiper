import * as vscode from 'vscode';
import * as path from 'path';

export class PipelinesSidebarProvider implements vscode.TreeDataProvider<NotebookItem> {
    private _onDidChangeTreeData = new vscode.EventEmitter<NotebookItem | void>();
    readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

    constructor() {
        // Watch for .dpnb changes in the workspace
        const watcher = vscode.workspace.createFileSystemWatcher('**/*.dpnb');
        watcher.onDidCreate(() => this.refresh());
        watcher.onDidDelete(() => this.refresh());
        watcher.onDidChange(() => this.refresh());
    }

    refresh(): void {
        this._onDidChangeTreeData.fire();
    }

    getTreeItem(element: NotebookItem): vscode.TreeItem {
        return element;
    }

    async getChildren(element?: NotebookItem): Promise<NotebookItem[]> {
        if (element && element.children) {
            return element.children;
        }
        if (element) return [];

        // 1. Find all .dpnb files
        const uris = await vscode.workspace.findFiles('**/*.dpnb');
        if (uris.length === 0) {
            return [new NotebookItem("No notebooks found", vscode.TreeItemCollapsibleState.None, { type: 'empty' })];
        }

        // 2. Get stats for sorting
        const itemsWithStat = await Promise.all(uris.map(async uri => {
            try {
                const stat = await vscode.workspace.fs.stat(uri);
                return { uri, stat };
            } catch (_e) {
                return null;
            }
        }));

        const validItems = itemsWithStat.filter(i => i !== null) as { uri: vscode.Uri, stat: vscode.FileStat }[];

        // 3. Group by Creation Date (ctime matches "creation" on many sys, or birthtime on Node fs)
        // VSCode API FileStat: ctime is creation, mtime is modification.

        const groups: Record<string, { uri: vscode.Uri, stat: vscode.FileStat }[]> = {
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

        for (const item of validItems) {
            const d = new Date(item.stat.ctime); // Creation time

            if (d >= today) groups["Today"].push(item);
            else if (d >= yesterday) groups["Yesterday"].push(item);
            else if (d >= lastWeek) groups["Last 7 Days"].push(item);
            else if (d >= lastMonth) groups["Last 30 Days"].push(item);
            else groups["Older"].push(item);
        }

        // 4. Build Tree
        const result: NotebookItem[] = [];
        const order = ["Today", "Yesterday", "Last 7 Days", "Last 30 Days", "Older"];

        for (const key of order) {
            const groupEntries = groups[key];
            if (groupEntries.length > 0) {
                // Sort within group (newest first)
                groupEntries.sort((a, b) => b.stat.ctime - a.stat.ctime);

                const children = groupEntries.map(entry => {
                    return new NotebookItem(
                        path.basename(entry.uri.fsPath, '.dpnb'),
                        vscode.TreeItemCollapsibleState.None,
                        { type: 'notebook', uri: entry.uri }
                    );
                });

                result.push(NotebookItem.group(key, children));
            }
        }

        return result;
    }
}

export interface NotebookItemContext {
    type: 'empty' | 'group' | 'notebook';
    uri?: vscode.Uri;
}

export class NotebookItem extends vscode.TreeItem {
    constructor(
        public readonly label: string,
        public readonly collapsibleState: vscode.TreeItemCollapsibleState,
        public readonly context: NotebookItemContext,
        public readonly children?: NotebookItem[]
    ) {
        super(label, children ? vscode.TreeItemCollapsibleState.Expanded : collapsibleState);

        if (context.type === 'empty') {
            this.iconPath = new vscode.ThemeIcon("info");
            this.contextValue = "empty";
        } else if (context.type === 'group') {
            this.iconPath = new vscode.ThemeIcon("calendar");
            this.contextValue = "group";
        } else if (context.type === 'notebook' && context.uri) {
            this.iconPath = new vscode.ThemeIcon("book");
            this.contextValue = "notebookItem"; // Used for package.json menus
            this.resourceUri = context.uri; // Helps with standard file commands

            this.command = {
                command: "vscode.open",
                title: "Open Notebook",
                arguments: [context.uri]
            };

            this.tooltip = context.uri.fsPath;
        }
    }

    static group(label: string, children: NotebookItem[]): NotebookItem {
        const state = (label === "Today" || label === "Yesterday") ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed;
        return new NotebookItem(label, state, { type: 'group' }, children);
    }
}
