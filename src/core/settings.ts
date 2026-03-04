import * as vscode from 'vscode';

export class SettingsManager {
    constructor() { }

    get<T = unknown>(key: string): T | undefined {
        return vscode.workspace.getConfiguration('dp').get<T>(key);
    }
}
