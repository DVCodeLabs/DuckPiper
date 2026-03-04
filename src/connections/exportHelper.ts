import * as vscode from 'vscode';
import * as fs from 'fs';
import { ConnectionProfile, ConnectionSecrets } from '../core/types';
import { getAdapter } from './adapterFactory';

export async function defaultExportTable(
    profile: ConnectionProfile,
    secrets: ConnectionSecrets,
    schema: string,
    table: string,
    format: 'csv',
    outputUri: vscode.Uri
): Promise<void> {
    const adapter = getAdapter(profile.dialect);

    // 1. Construct Query
    // Simple robust quoting. Ideally adapter should provide 'quoteIdentifier' but for v1 we do basic double-quote.
    // If dialect is mysql and we are not sure about ANSI_QUOTES, we might want to use backticks,
    // but standard ANSI is " for identifiers.
    const quote = (id: string) => {
        if (profile.dialect === 'mysql') return `\`${id}\``;
        return `"${id}"`;
    };

    // We assume schema/table provided are clean or we just quote them.
    const fullTableName = schema ? `${quote(schema)}.${quote(table)}` : quote(table);
    const sql = `SELECT * FROM ${fullTableName}`;

    // 2. Run Query
    vscode.window.setStatusBarMessage('Exporting table...', 4000);

    const result = await adapter.runQuery(profile, secrets, sql, { maxRows: Number.MAX_SAFE_INTEGER });

    // 3. Convert to CSV
    if (!result.rows || result.rows.length === 0) {
        // write headers only if columns exist?
        // if no rows, we might not have columns unless we did describe.
        // runQuery implementation for duckdb returns columns even if empty?
        // Let's just write empty file or headers if we have them.
        if (result.columns && result.columns.length > 0) {
            const header = result.columns.map(c => escapeCsv(c.name)).join(',') + '\n';
            await fs.promises.writeFile(outputUri.fsPath, header, 'utf8');
        } else {
            await fs.promises.writeFile(outputUri.fsPath, '', 'utf8');
        }
        return;
    }

    const headers = result.columns.map(c => escapeCsv(c.name)).join(',');

    // Chunked write or simple join? For 100k rows, memory might be heavy.
    // Let's use stream writing.
    const stream = fs.createWriteStream(outputUri.fsPath, { encoding: 'utf8' });

    stream.write(headers + '\n');

    for (const row of result.rows) {
        let values: unknown[];
        if (Array.isArray(row)) {
            values = row;
        } else {
            // It's an object (e.g. Postgres adapter returns { col: val })
            // Map keys based on columns order
            values = result.columns.map(c => (row as Record<string, unknown>)[c.name]);
        }

        const line = values.map((val: unknown) => escapeCsv(val)).join(',');
        stream.write(line + '\n');
    }

    stream.end();

    await new Promise<void>((resolve, reject) => {
        stream.on('finish', () => resolve());
        stream.on('error', reject);
    });

}

function escapeCsv(val: unknown): string {
    if (val === null || val === undefined) return '';
    const str = String(val);
    // If contains quote, comma, or newline, escape it
    if (/[",\n\r]/.test(str)) {
        return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
}
