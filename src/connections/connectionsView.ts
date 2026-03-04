import * as vscode from "vscode";
import { ConnectionProfile } from "../core/types";
import { loadConnectionProfiles } from "./connectionStore";

export class ConnectionsViewProvider implements vscode.TreeDataProvider<ConnectionItem> {
  private _onDidChangeTreeData = new vscode.EventEmitter<ConnectionItem | void>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: ConnectionItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: ConnectionItem): Promise<ConnectionItem[]> {
    if (element) return [];
    const connections = await loadConnectionProfiles();

    if (connections.length === 0) {
      return [new ConnectionItem("No connections yet", undefined, "Add a connection to get started.", true)];
    }

    // Get active ID from workspace state? 
    // Actually, view provider doesn't have reference to context easily unless passed or strict singleton.
    // Ideally pass context to constructor. For now, rely on a static/global or pass in constructor.
    // Let's reload active ID from workspaceState if we can access it.
    // or we can use a callback/event listener. 
    // Simpler v0: check vscode.workspaceState? No, that's on context.

    // We will make ConnectionsViewProvider accept context in constructor.

    return connections.map((c) => ConnectionItem.fromProfile(c, this._activeId));
  }

  private _activeId?: string;
  setActiveId(id: string | undefined) {
    this._activeId = id;
    this.refresh();
  }
}

export class ConnectionItem extends vscode.TreeItem {
  constructor(
    label: string,
    public readonly profile?: ConnectionProfile,
    tooltip?: string,
    isPlaceholder?: boolean
  ) {
    super(label, vscode.TreeItemCollapsibleState.None);
    this.tooltip = tooltip;
    this.contextValue = isPlaceholder ? "dp.connection.placeholder" : "dp.connection.item";
  }

  static fromProfile(p: ConnectionProfile, activeId?: string): ConnectionItem {
    const isActive = p.id === activeId;
    const label = `${p.name}${isActive ? ' (Active)' : ''}`;
    // Safe access to dialect
    const dialect = p.dialect || String(((p as unknown) as Record<string, unknown>).type ?? '?');
    const description = `${dialect}${p.database ? ` • ${p.database}` : ""}${p.host ? ` • ${p.host}` : ""}`;
    const item = new ConnectionItem(label, p, description);
    item.description = description;

    // Clicking selects the active connection
    item.command = {
      command: "dp.connection.select",
      title: "Select Connection",
      arguments: [p]
    };

    item.iconPath = new vscode.ThemeIcon(isActive ? "pass-filled" : "plug");

    if (p.id === 'duck-piper-local-data-work') {
      item.contextValue = 'dp.connection.item.locked';
    }

    return item;
  }
}
