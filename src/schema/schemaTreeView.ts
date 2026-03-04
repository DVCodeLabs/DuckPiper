import * as vscode from 'vscode';

export class SchemaTreeView implements vscode.TreeDataProvider<vscode.TreeItem> {
    getTreeItem(_element: vscode.TreeItem): vscode.TreeItem {
        return _element;
    }

    getChildren(_element?: vscode.TreeItem): vscode.ProviderResult<vscode.TreeItem[]> {
        return [];
    }
}
