import * as vscode from 'vscode';

export class CodelensProvider implements vscode.CodeLensProvider {
    provideCodeLenses(_document: vscode.TextDocument): vscode.CodeLens[] {
        return [];
    }
}
