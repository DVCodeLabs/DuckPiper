/**
 * Parses companion markdown files for search metadata.
 * Tolerates malformed frontmatter and extracts what it can.
 */

export interface MdMetadata {
    title?: string;
    tags?: string[];
    connectionId?: string;
    connectionName?: string;
    dialect?: string;
    bodyText: string;       // plain text body (after frontmatter)
    summary?: string;       // first meaningful paragraph/line
}

/**
 * Parse YAML-like frontmatter from markdown text.
 * Tolerant of malformed content — extracts what it can.
 */
export function parseMdMetadata(mdContent: string): MdMetadata {
    const result: MdMetadata = { bodyText: '' };

    if (!mdContent.startsWith('---')) {
        // No frontmatter — entire content is body
        result.bodyText = normalizeBody(mdContent);
        result.summary = extractSummary(mdContent);
        return result;
    }

    const endIdx = mdContent.indexOf('\n---', 3);
    if (endIdx === -1) {
        // Malformed frontmatter — treat entire content as body
        result.bodyText = normalizeBody(mdContent);
        result.summary = extractSummary(mdContent);
        return result;
    }

    const fmBlock = mdContent.slice(4, endIdx); // between opening and closing ---
    const body = mdContent.slice(endIdx + 4);

    // Parse frontmatter fields
    try {
        result.title = extractFmString(fmBlock, 'title');
        result.tags = extractFmArray(fmBlock, 'tags');
        result.connectionId = extractFmString(fmBlock, 'connection_id');
        result.connectionName = extractFmString(fmBlock, 'connection');
        result.dialect = extractFmString(fmBlock, 'dialect');
    } catch {
        // Tolerate parse errors — partial extraction is fine
    }

    result.bodyText = normalizeBody(body);
    result.summary = extractSummary(body);

    return result;
}

function extractFmString(fm: string, key: string): string | undefined {
    const re = new RegExp(`^${key}:\\s*(.+)$`, 'm');
    const match = fm.match(re);
    if (!match) return undefined;
    let val = match[1].trim();
    // Strip surrounding quotes
    if ((val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))) {
        val = val.slice(1, -1);
    }
    return val || undefined;
}

function extractFmArray(fm: string, key: string): string[] | undefined {
    // Handle inline array: tags: [foo, bar, baz]
    const inlineRe = new RegExp(`^${key}:\\s*\\[([^\\]]*)]`, 'm');
    const inlineMatch = fm.match(inlineRe);
    if (inlineMatch) {
        const items = inlineMatch[1].split(',').map(s => {
            let v = s.trim();
            if ((v.startsWith('"') && v.endsWith('"')) ||
                (v.startsWith("'") && v.endsWith("'"))) {
                v = v.slice(1, -1);
            }
            return v;
        }).filter(Boolean);
        return items.length > 0 ? items : undefined;
    }

    // Handle YAML list style:
    // tags:
    //   - foo
    //   - bar
    const listRe = new RegExp(`^${key}:\\s*$`, 'm');
    if (!listRe.test(fm)) return undefined;

    const lines = fm.split('\n');
    const idx = lines.findIndex(l => listRe.test(l));
    if (idx === -1) return undefined;

    const items: string[] = [];
    for (let i = idx + 1; i < lines.length; i++) {
        const line = lines[i];
        const itemMatch = line.match(/^\s+-\s+(.+)/);
        if (itemMatch) {
            let v = itemMatch[1].trim();
            if ((v.startsWith('"') && v.endsWith('"')) ||
                (v.startsWith("'") && v.endsWith("'"))) {
                v = v.slice(1, -1);
            }
            items.push(v);
        } else if (line.match(/^\s*\S+:/)) {
            break; // next key
        }
    }

    return items.length > 0 ? items : undefined;
}

function normalizeBody(text: string): string {
    return text
        .replace(/^#+\s+/gm, '')   // strip markdown headings markers
        .replace(/[*_~`]/g, '')      // strip markdown emphasis/code markers
        .replace(/\r\n/g, '\n')
        .replace(/\n{3,}/g, '\n\n') // collapse excessive newlines
        .trim();
}

function extractSummary(text: string): string | undefined {
    const lines = text.split(/\r?\n/);
    for (const raw of lines) {
        const line = raw.replace(/^#+\s+/, '').replace(/[*_~`]/g, '').trim();
        if (!line) continue;
        if (line.startsWith('---')) continue;
        // Return first meaningful line (up to 200 chars)
        return line.length > 200 ? line.slice(0, 200) + '...' : line;
    }
    return undefined;
}

/**
 * Build normalized searchText from all searchable fields.
 */
export function buildSearchText(fields: {
    title?: string;
    mdTitle?: string;
    mdTags?: string[];
    mdBodyText?: string;
    path?: string;
    connectionName?: string;
    dialect?: string;
    sqlText?: string;
}): string {
    const parts: string[] = [];

    if (fields.mdTitle) parts.push(fields.mdTitle);
    if (fields.title) parts.push(fields.title);
    if (fields.mdTags?.length) parts.push(fields.mdTags.join(' '));
    if (fields.mdBodyText) parts.push(fields.mdBodyText);
    if (fields.path) parts.push(fields.path);
    if (fields.connectionName) parts.push(fields.connectionName);
    if (fields.dialect) parts.push(fields.dialect);
    if (fields.sqlText) parts.push(fields.sqlText);

    return parts.join(' ').toLowerCase();
}
