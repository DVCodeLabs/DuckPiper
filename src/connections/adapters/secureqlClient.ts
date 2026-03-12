import * as https from 'https';
import * as http from 'http';
import { randomUUID } from 'crypto';

export interface SecureQLRequestOptions {
    baseUrl: string;
    apiKey: string;
    connectionId: string | number;
}

export interface SecureQLErrorPayload {
    title?: string;
    message?: string;
    code?: number;
}

export class SecureQLApiError extends Error {
    constructor(
        public readonly statusCode: number,
        public readonly userMessage: string,
        public readonly serverMessage?: string,
    ) {
        super(userMessage);
        this.name = 'SecureQLApiError';
    }
}

const LOCALHOST_HOSTS = ['localhost', '127.0.0.1', '::1'];

/**
 * Normalize a user-entered base URL:
 * - Strip trailing slashes
 * - Prepend protocol if missing:
 *   - `http://` for localhost addresses
 *   - `https://` for everything else
 */
export function normalizeBaseUrl(raw: string): string {
    let url = raw.trim().replace(/\/+$/, '');

    // If no protocol, add one based on whether it looks like localhost
    if (!url.startsWith('http://') && !url.startsWith('https://')) {
        const hostPart = url.split(':')[0].split('/')[0].toLowerCase();
        if (LOCALHOST_HOSTS.includes(hostPart)) {
            url = `http://${url}`;
        } else {
            url = `https://${url}`;
        }
    }

    return url;
}

function isLocalhostUrl(url: string): boolean {
    try {
        const u = new URL(url);
        return LOCALHOST_HOSTS.includes(u.hostname);
    } catch {
        return false;
    }
}

function enforceHttps(baseUrl: string): void {
    if (baseUrl.startsWith('https://')) return;
    if (baseUrl.startsWith('http://') && isLocalhostUrl(baseUrl)) return;
    throw new SecureQLApiError(0, 'SecureQL connections require HTTPS. HTTP is only allowed for localhost.');
}

function redactSensitive(text: string, apiKey: string): string {
    if (apiKey && text.includes(apiKey)) {
        return text.replace(new RegExp(apiKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'g'), '[REDACTED]');
    }
    return text.replace(/Bearer\s+\S+/g, 'Bearer [REDACTED]');
}

export function mapSecureQLError(statusCode: number, body: SecureQLErrorPayload | string, apiKey: string): SecureQLApiError {
    const msg = typeof body === 'object' ? (body.message ?? '') : String(body);

    if (statusCode === 401) {
        return new SecureQLApiError(401, 'API key is invalid or missing for this SecureQL connection.');
    }

    if (statusCode === 403) {
        if (msg.includes('User account is disabled')) {
            return new SecureQLApiError(403, 'Your SecureQL user account is disabled.');
        }
        if (msg.includes('Membership is disabled')) {
            return new SecureQLApiError(403, 'Your access to this SecureQL connection has been disabled by admin.');
        }
        if (msg.includes('Connection is disabled')) {
            return new SecureQLApiError(403, 'This SecureQL connection is disabled by admin.');
        }
        if (msg.includes('not authorized for this connection')) {
            return new SecureQLApiError(403, 'API key is not authorized for this connection ID.');
        }
        return new SecureQLApiError(403, 'Access denied for this SecureQL connection.');
    }

    if (statusCode === 404) {
        return new SecureQLApiError(404, 'SecureQL connection ID not found.');
    }

    const safeMsg = redactSensitive(msg, apiKey);
    return new SecureQLApiError(statusCode, `SecureQL server error (${statusCode}): ${safeMsg}`);
}

function makeRequest(
    url: string,
    method: 'GET' | 'POST',
    headers: Record<string, string>,
    body?: string,
): Promise<{ statusCode: number; body: string }> {
    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(url);
        const transport = parsedUrl.protocol === 'https:' ? https : http;
        const options = {
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
            path: parsedUrl.pathname + parsedUrl.search,
            method,
            headers,
        };

        const req = transport.request(options, (res) => {
            const chunks: Buffer[] = [];
            res.on('data', (chunk: Buffer) => chunks.push(chunk));
            res.on('end', () => {
                resolve({
                    statusCode: res.statusCode ?? 500,
                    body: Buffer.concat(chunks).toString('utf8'),
                });
            });
        });

        req.on('error', (err) => reject(new SecureQLApiError(0, `Network error: ${err.message}`)));

        if (body) {
            req.write(body);
        }
        req.end();
    });
}

/**
 * Make a POST request expecting an NDJSON streaming response.
 *
 * Reconstructs the original `{ results, log }` shape from streaming events so
 * that callers (mapQueryResponse) can consume it identically to a buffered JSON
 * response. Falls back to JSON parsing for error responses from middleware
 * (e.g. 401/403/404 returned before the streaming handler runs).
 */
function makeNdjsonRequest(
    url: string,
    headers: Record<string, string>,
    body: string,
): Promise<{ statusCode: number; parsed: any }> {
    return new Promise((resolve, reject) => {
        const parsedUrl = new URL(url);
        const transport = parsedUrl.protocol === 'https:' ? https : http;
        const options = {
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
            path: parsedUrl.pathname + parsedUrl.search,
            method: 'POST',
            headers,
        };

        const req = transport.request(options, (res) => {
            const statusCode = res.statusCode ?? 500;
            const contentType = res.headers['content-type'] ?? '';
            const isNdjson = contentType.includes('application/x-ndjson');

            const chunks: Buffer[] = [];
            res.on('data', (chunk: Buffer) => chunks.push(chunk));
            res.on('end', () => {
                const raw = Buffer.concat(chunks).toString('utf8');

                // Error responses from middleware (401/403/404) are JSON, not NDJSON.
                if (!isNdjson) {
                    resolve({ statusCode, parsed: parseResponseBody(raw) });
                    return;
                }

                try {
                    resolve({ statusCode, parsed: reassembleNdjson(raw) });
                } catch (err) {
                    reject(new SecureQLApiError(
                        0,
                        `Failed to parse streaming response: ${err instanceof Error ? err.message : String(err)}`,
                    ));
                }
            });
        });

        req.on('error', (err) => reject(new SecureQLApiError(0, `Network error: ${err.message}`)));
        req.write(body);
        req.end();
    });
}

/**
 * Parse an NDJSON response body and reconstruct the `{ results, log }` shape
 * that mapQueryResponse() expects.
 *
 * Event types:
 *   result       → complete non-tabular QueryReturn (INSERT/UPDATE/DELETE)
 *   result_start → begins a tabular result; carries fields + metadata
 *   row          → single row of data within a tabular result
 *   result_end   → ends a tabular result; carries affectedRows, runtime
 *   error        → query error within a statement
 *   log          → final log metadata (always last)
 */
function reassembleNdjson(raw: string): { results: any[]; log?: any } {
    const lines = raw.split('\n').filter((l) => l.length > 0);
    const results: any[] = [];
    let log: any;
    let currentResult: any = null;

    for (const line of lines) {
        const event = JSON.parse(line);

        switch (event.type) {
            case 'result':
                results.push({
                    affectedRows: event.affectedRows ?? 0,
                    dbms: event.dbms,
                    queriesRun: event.queriesRun ?? 1,
                    query: event.query,
                    runtime: event.runtime,
                    timestamp: event.timestamp,
                    message: event.message,
                });
                break;

            case 'result_start':
                currentResult = {
                    fields: event.fields,
                    query: event.query,
                    dbms: event.dbms,
                    timestamp: event.timestamp,
                    rows: [],
                    affectedRows: 0,
                    queriesRun: 1,
                };
                break;

            case 'row':
                if (currentResult) {
                    currentResult.rows.push(event.data);
                }
                break;

            case 'result_end':
                if (currentResult) {
                    currentResult.affectedRows = event.affectedRows ?? 0;
                    currentResult.queriesRun = event.queriesRun ?? 1;
                    currentResult.runtime = event.runtime;
                    results.push(currentResult);
                    currentResult = null;
                }
                break;

            case 'error':
                results.push({
                    error: event.error,
                    query: event.query,
                    dbms: event.dbms,
                    timestamp: event.timestamp,
                    affectedRows: 0,
                    queriesRun: 0,
                });
                break;

            case 'log': {
                const { type: _, ...logData } = event;
                log = logData;
                break;
            }
        }
    }

    return { results, log };
}

function buildHeaders(apiKey: string): Record<string, string> {
    return {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'X-Request-Id': randomUUID(),
    };
}

function parseResponseBody(raw: string): any {
    try {
        return JSON.parse(raw);
    } catch {
        return raw;
    }
}

export interface KeyInfo {
    connection_id: number;
    connection_name: string;
    dbms: string;
    database_name: string;
    allow_csv_export: boolean;
    user_id: number;
}

/**
 * GET /v1/key/me — resolve connection metadata from just the API key.
 * Does not require a connection ID (it's derived from the key).
 */
export async function getKeyInfo(baseUrl: string, apiKey: string): Promise<KeyInfo> {
    const base = normalizeBaseUrl(baseUrl);
    enforceHttps(base);

    const url = `${base}/v1/key/me`;
    const resp = await makeRequest(url, 'GET', buildHeaders(apiKey));
    const body = parseResponseBody(resp.body);

    if (resp.statusCode !== 200) {
        throw mapSecureQLError(resp.statusCode, body, apiKey);
    }

    return body as KeyInfo;
}

export async function getSchema(opts: SecureQLRequestOptions): Promise<any> {
    const base = normalizeBaseUrl(opts.baseUrl);
    enforceHttps(base);

    const url = `${base}/v1/key/connections/${opts.connectionId}/schema`;
    const resp = await makeRequest(url, 'GET', buildHeaders(opts.apiKey));
    const body = parseResponseBody(resp.body);

    if (resp.statusCode !== 200) {
        throw mapSecureQLError(resp.statusCode, body, opts.apiKey);
    }

    return body;
}

/**
 * Execute a SQL query via the SecureQL API.
 *
 * The server streams results as NDJSON (row-by-row). The response is reassembled
 * into the standard `{ results, log }` shape before returning, so callers don't
 * need to know about the streaming protocol.
 */
export async function executeQuery(opts: SecureQLRequestOptions, sql: string): Promise<any> {
    const base = normalizeBaseUrl(opts.baseUrl);
    enforceHttps(base);

    const url = `${base}/v1/key/connections/${opts.connectionId}/query`;
    const payload = JSON.stringify({ sql });

    const headers = buildHeaders(opts.apiKey);
    headers['Accept'] = 'application/x-ndjson';

    const resp = await makeNdjsonRequest(url, headers, payload);

    if (resp.statusCode !== 200) {
        throw mapSecureQLError(resp.statusCode, resp.parsed, opts.apiKey);
    }

    return resp.parsed;
}
