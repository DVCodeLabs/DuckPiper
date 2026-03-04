import * as vscode from "vscode";

export interface Watchers {
  dispose(): void;
}

export function registerDPWatchers(
  onConnectionsChanged: () => void,
  onSchemasChanged: () => void,
  onQueryIndexChanged: () => void
): Watchers {
  const subs: vscode.Disposable[] = [];

  // connections.json
  const connWatcher = vscode.workspace.createFileSystemWatcher("**/DP/system/connections.json");
  subs.push(connWatcher);
  subs.push(connWatcher.onDidChange(onConnectionsChanged));
  subs.push(connWatcher.onDidCreate(onConnectionsChanged));
  subs.push(connWatcher.onDidDelete(onConnectionsChanged));

  // schemas/*.json (exclude Meta/Relationships? v0: refresh all on any schema file change)
  const schemaWatcher = vscode.workspace.createFileSystemWatcher("**/DP/schemas/*.json");
  subs.push(schemaWatcher);
  subs.push(schemaWatcher.onDidChange(onSchemasChanged));
  subs.push(schemaWatcher.onDidCreate(onSchemasChanged));
  subs.push(schemaWatcher.onDidDelete(onSchemasChanged));

  // queryIndex.json
  const qWatcher = vscode.workspace.createFileSystemWatcher("**/DP/**/queryIndex.json");
  subs.push(qWatcher);
  subs.push(qWatcher.onDidChange(onQueryIndexChanged));
  subs.push(qWatcher.onDidCreate(onQueryIndexChanged));
  subs.push(qWatcher.onDidDelete(onQueryIndexChanged));

  return {
    dispose: () => subs.forEach((s) => s.dispose())
  };
}
