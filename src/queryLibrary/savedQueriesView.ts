import * as vscode from "vscode";
import { ensureDPDirs, fileExists, readJson } from "../core/fsWorkspace";
import { parseMdMetadata } from "./mdParser";

export interface QueryIndexFile {
  version: "0.1";
  generatedAt: string;
  queries: QueryIndexEntry[];
}

export interface QueryIndexEntry {
  path: string;        // workspace-relative path
  docPath?: string;    // companion markdown relative path
  title?: string;      // optional title
  sqlHash: string;     // sha256(canonicalSql)
  tables?: string[];   // optional best-effort
  createdAt: string;   // ISO string - when first indexed
  updatedAt: string;   // ISO string - when last modified
  connectionId?: string;
  mdTitle?: string;
  mdBodyText?: string;
}

export class SavedQueriesViewProvider implements vscode.TreeDataProvider<SavedQueryItem> {
  private _onDidChangeTreeData = new vscode.EventEmitter<SavedQueryItem | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: SavedQueryItem): vscode.TreeItem {
    return element;
  }

  async resolveTreeItem(
    item: vscode.TreeItem,
    element: SavedQueryItem,
    _token: vscode.CancellationToken
  ): Promise<SavedQueryItem> {
    if (!element.entry) return element;

    const root = vscode.workspace.workspaceFolders?.[0]?.uri;
    if (!root) return element;

    const mdRelPath = element.entry.path.replace(/\.(sql|postgres)$/i, '.md');
    const mdUri = vscode.Uri.joinPath(root, mdRelPath);

    try {
      const mdBytes = await vscode.workspace.fs.readFile(mdUri);
      const mdContent = Buffer.from(mdBytes).toString('utf8');
      const mdMeta = parseMdMetadata(mdContent);

      // Extract "# What this query answers" section from raw body
      let rawBody = '';
      if (mdContent.startsWith('---')) {
        const endIdx = mdContent.indexOf('\n---', 3);
        if (endIdx !== -1) {
          rawBody = mdContent.slice(endIdx + 4);
        } else {
          rawBody = mdContent;
        }
      } else {
        rawBody = mdContent;
      }

      let answersSection = '';
      const sectionStart = rawBody.indexOf('# What this query answers');
      if (sectionStart !== -1) {
        const afterHeading = rawBody.slice(sectionStart + '# What this query answers'.length);
        const nextSection = afterHeading.search(/^#\s/m);
        answersSection = (nextSection !== -1 ? afterHeading.slice(0, nextSection) : afterHeading).trim();
      }

      if (mdMeta.title || answersSection) {
        const md = new vscode.MarkdownString();
        if (mdMeta.title) {
          md.appendMarkdown(`**${mdMeta.title}**\n\n`);
        }
        if (answersSection) {
          md.appendMarkdown(answersSection);
        }
        item.tooltip = md;
      }
    } catch {
      // No companion .md — fall back to path
      item.tooltip = element.entry.path;
    }

    return element;
  }

  async getChildren(element?: SavedQueryItem): Promise<SavedQueryItem[]> {
    // If element has children pre-calculated (Group item), return them
    if (element && element.children) {
      return element.children;
    }
    // If element is a query item (leaf), return empty
    if (element) return [];

    // Check initialization first to avoid creating DP folder
    const { isProjectInitialized } = require('../core/isProjectInitialized');
    if (!(await isProjectInitialized())) {
      return [
        SavedQueryItem.placeholder(
          "Not initialized",
          "Initialize project to save queries."
        )
      ];
    }

    const dpDir = await ensureDPDirs();
    const indexUri = vscode.Uri.joinPath(dpDir, "system", "queries", "queryIndex.json");

    if (!(await fileExists(indexUri))) {
      return [
        SavedQueryItem.placeholder(
          "No query index yet",
          "Run the query indexer to populate DP/queryIndex.json."
        )
      ];
    }

    const index = await readJson<QueryIndexFile>(indexUri);
    const queries = index.queries ?? [];

    if (queries.length === 0) {
      return [
        SavedQueryItem.placeholder(
          "No saved queries found",
          "Add .sql files to your workspace (in DP/queries)."
        )
      ];
    }

    // Sort bucket logic
    const groups: Record<string, QueryIndexEntry[]> = {
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

    for (const q of queries) {
      // Group by createdAt (when the query was first added)
      const dateStr = q.createdAt || q.updatedAt;
      const d = new Date(dateStr);
      if (d >= today) groups["Today"].push(q);
      else if (d >= yesterday) groups["Yesterday"].push(q);
      else if (d >= lastWeek) groups["Last 7 Days"].push(q);
      else if (d >= lastMonth) groups["Last 30 Days"].push(q);
      else groups["Older"].push(q);
    }

    const result: SavedQueryItem[] = [];
    const order = ["Today", "Yesterday", "Last 7 Days", "Last 30 Days", "Older"];

    for (const key of order) {
      const groupQueries = groups[key];
      if (groupQueries.length > 0) {
        // Sort within group by createdAt (newest first)
        groupQueries.sort((a, b) => {
          const dA = new Date(a.createdAt || a.updatedAt).getTime();
          const dB = new Date(b.createdAt || b.updatedAt).getTime();
          return dA < dB ? 1 : -1;
        });

        const children = groupQueries.map(q => {
          const item = SavedQueryItem.fromEntry(q);
          const dateStr = q.createdAt || q.updatedAt;
          // Adjust description based on group
          if (key === "Today" || key === "Yesterday") {
            item.description = new Date(dateStr).toLocaleTimeString();
          } else {
            item.description = new Date(dateStr).toLocaleString();
          }
          return item;
        });

        const groupItem = SavedQueryItem.group(key, children);
        result.push(groupItem);
      }
    }

    return result;
  }
}

export class SavedQueryItem extends vscode.TreeItem {
  constructor(
    label: string,
    public readonly entry?: QueryIndexEntry,
    tooltip?: string,
    isPlaceholder?: boolean,
    public readonly children?: SavedQueryItem[],
    collapsibleState?: vscode.TreeItemCollapsibleState
  ) {
    super(label, collapsibleState ?? (children ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.None));
    this.tooltip = tooltip;
    this.contextValue = isPlaceholder ? "dp.savedQueries.placeholder" : (children ? "dp.savedQueries.group" : "queryItem");
  }

  static placeholder(label: string, tooltip: string): SavedQueryItem {
    const item = new SavedQueryItem(label, undefined, tooltip, true);
    item.iconPath = new vscode.ThemeIcon("info");
    return item;
  }

  static group(label: string, children: SavedQueryItem[]): SavedQueryItem {
    const state = label === "Today" ? vscode.TreeItemCollapsibleState.Expanded : vscode.TreeItemCollapsibleState.Collapsed;
    const item = new SavedQueryItem(label, undefined, undefined, false, children, state);
    item.iconPath = new vscode.ThemeIcon("calendar");
    return item;
  }

  static fromEntry(q: QueryIndexEntry): SavedQueryItem {
    const label = q.title ?? q.path.split("/").pop() ?? q.path;
    const item = new SavedQueryItem(label, q);

    const dateStr = q.createdAt || q.updatedAt;
    item.description = new Date(dateStr).toLocaleTimeString(); // Show time since grouped by date
    item.iconPath = new vscode.ThemeIcon("file-code");

    // ✅ Robust URI join
    const root = vscode.workspace.workspaceFolders?.[0]?.uri;
    if (root) {
      const uri = vscode.Uri.joinPath(root, q.path);
      item.command = {
        command: "dp.query.openSaved",
        title: "Open Query",
        arguments: [uri, q.connectionId]
      };
    }

    return item;
  }
}
