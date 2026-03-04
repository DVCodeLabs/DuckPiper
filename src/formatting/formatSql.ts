
import * as vscode from 'vscode';
import { format, SqlLanguage } from 'sql-formatter';
import { DPDocConnectionStore } from '../ui/sqlCodelens';
import { ConnectionProfile } from '../core/types';

export class SqlFormattingProvider {
    constructor(
        private codeLensStore: DPDocConnectionStore,
        private context: vscode.ExtensionContext
    ) { }

    async formatDocument(editor: vscode.TextEditor): Promise<void> {
        const doc = editor.document;
        const config = vscode.workspace.getConfiguration('dp.format');

        if (!config.get<boolean>('enabled', true)) {
            vscode.window.showInformationMessage('SQL formatting is disabled in settings.');
            return;
        }

        const indentSize = config.get<number>('indentSize', 2);
        const keywordCase = config.get<string>('keywordCase', 'upper');
        const dialectFallback = config.get<string>('dialectFallback', 'postgresql');

        let dialect: SqlLanguage = 'postgresql'; // Default

        // Determine dialect from active connection
        const connId = this.codeLensStore.get(doc) ||
            this.context.workspaceState.get<string>("dp.activeConnectionId");

        if (connId) {
            const { getConnection } = require('../connections/connectionStore');
            const profile: ConnectionProfile | undefined = await getConnection(connId);
            if (profile) {
                dialect = this.mapDialect(profile.dialect);
            }
        } else {
            // Fallback
            dialect = this.mapDialect(dialectFallback);
        }

        const text = doc.getText();

        try {
            const formatted = format(text, {
                language: dialect,
                tabWidth: indentSize,
                keywordCase: keywordCase as 'upper' | 'lower' | 'preserve',
                linesBetweenQueries: 2
            });

            // Apply edit
            const fullRange = new vscode.Range(
                doc.positionAt(0),
                doc.positionAt(text.length)
            );

            const edit = new vscode.WorkspaceEdit();
            edit.replace(doc.uri, fullRange, formatted);
            await vscode.workspace.applyEdit(edit);

            // Helpful status message?
            // vscode.window.setStatusBarMessage('SQL formatted', 2000);

        } catch (e: unknown) {
            vscode.window.showErrorMessage(`Formatting failed: ${e instanceof Error ? e.message : String(e)}`);
        }
    }

    private mapDialect(dpDialect: string): SqlLanguage {
        // sql-formatter supports: sql, bigquery, db2, hive, mariadb, mysql, n1ql, plsql, postgresql, redshift, spark, sqlite, snowflake, trino, transactsql
        const d = dpDialect.toLowerCase();

        if (d.includes('postgres')) return 'postgresql';
        if (d.includes('mysql')) return 'mysql';
        if (d.includes('maria')) return 'mariadb';
        if (d.includes('sqlite')) return 'sqlite';
        if (d.includes('snowflake')) return 'snowflake';
        if (d.includes('redshift')) return 'redshift';
        if (d.includes('bigquery')) return 'bigquery';
        if (d.includes('duckdb')) return 'postgresql'; // Treat duckdb as postgres for now (v1)
        if (d.includes('mssql') || d.includes('sqlserver')) return 'transactsql';

        return 'sql'; // Generic SQL fallback
    }
}
