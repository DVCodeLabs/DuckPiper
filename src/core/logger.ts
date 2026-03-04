import * as vscode from 'vscode';

export class Logger {
    private static _outputChannel: vscode.OutputChannel;

    static initialize(name: string) {
        this._outputChannel = vscode.window.createOutputChannel(name);
    }

    static info(message: string) {
        if (this._outputChannel) {
            const timestamp = new Date().toISOString();
            this._outputChannel.appendLine(`[${timestamp}] [INFO] ${message}`);
        }
    }

    static log(message: string) {
        this.info(message);
    }

    static warn(message: string, error?: unknown) {
        if (this._outputChannel) {
            const timestamp = new Date().toISOString();
            const errorMsg = error ? ` - ${this.formatError(error)}` : '';
            this._outputChannel.appendLine(`[${timestamp}] [WARN] ${message}${errorMsg}`);
        }
    }

    static error(message: string, error?: unknown) {
        if (this._outputChannel) {
            const timestamp = new Date().toISOString();
            const errorMsg = error ? ` - ${this.formatError(error)}` : '';
            this._outputChannel.appendLine(`[${timestamp}] [ERROR] ${message}${errorMsg}`);
        }
    }

    static debug(message: string) {
        if (this._outputChannel) {
            const timestamp = new Date().toISOString();
            this._outputChannel.appendLine(`[${timestamp}] [DEBUG] ${message}`);
        }
    }

    private static formatError(error: unknown): string {
        if (error instanceof Error) {
            return `${error.message}\n${error.stack || ''}`;
        }
        if (typeof error === 'object') {
            try {
                return JSON.stringify(error);
            } catch {
                return String(error);
            }
        }
        return String(error);
    }

    static show() {
        if (this._outputChannel) {
            this._outputChannel.show();
        }
    }
}
