import { QueryIndexEntry } from './queryIndexer';

export type SearchFieldScope = 'all' | 'title' | 'tags' | 'notes' | 'sql';

export interface SearchResult {
    entry: QueryIndexEntry;
    score: number;
    snippet?: string;   // short matched context
}

/**
 * Tokenize a search query string.
 * Supports quoted phrase matching and whitespace-separated AND tokens.
 */
export function tokenize(query: string): string[] {
    const tokens: string[] = [];
    const re = /"([^"]+)"|(\S+)/g;
    let match: RegExpExecArray | null;
    while ((match = re.exec(query)) !== null) {
        const token = (match[1] ?? match[2]).toLowerCase().trim();
        if (token) tokens.push(token);
    }
    return tokens;
}

/**
 * Search the query index entries with ranking.
 *
 * Ranking (highest to lowest):
 *  1. Exact title match
 *  2. Prefix title match
 *  3. Tag match
 *  4. Markdown note/body match
 *  5. Path/file-name match
 *  6. SQL text match (via searchText)
 *  7. Recency tie-break (lastRunAt, then updatedAt)
 */
export function searchEntries(
    entries: QueryIndexEntry[],
    query: string,
    scope: SearchFieldScope = 'all',
    maxResults = 200
): SearchResult[] {
    const trimmed = query.trim();
    if (!trimmed) return [];

    const tokens = tokenize(trimmed);
    if (tokens.length === 0) return [];

    const results: SearchResult[] = [];

    for (const entry of entries) {
        const scoreResult = scoreEntry(entry, tokens, scope);
        if (scoreResult.score > 0) {
            results.push({
                entry,
                score: scoreResult.score,
                snippet: scoreResult.snippet,
            });
        }
    }

    // Sort by score descending, then recency
    results.sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;
        // Recency tie-break
        const aTime = recencyTime(a.entry);
        const bTime = recencyTime(b.entry);
        return bTime - aTime;
    });

    return results.slice(0, maxResults);
}

/**
 * Get recent entries sorted by lastRunAt then updatedAt (for empty query display).
 */
export function getRecentEntries(entries: QueryIndexEntry[], max = 50): QueryIndexEntry[] {
    return [...entries]
        .sort((a, b) => recencyTime(b) - recencyTime(a))
        .slice(0, max);
}

function recencyTime(entry: QueryIndexEntry): number {
    if (entry.lastRunAt) return new Date(entry.lastRunAt).getTime();
    return new Date(entry.updatedAt).getTime();
}

function scoreEntry(
    entry: QueryIndexEntry,
    tokens: string[],
    scope: SearchFieldScope
): { score: number; snippet?: string } {
    let totalScore = 0;
    let snippet: string | undefined;

    // All tokens must match (AND semantics)
    for (const token of tokens) {
        const tokenScore = scoreToken(entry, token, scope);
        if (tokenScore.score === 0) return { score: 0 };
        totalScore += tokenScore.score;
        if (!snippet && tokenScore.snippet) snippet = tokenScore.snippet;
    }

    return { score: totalScore, snippet };
}

function scoreToken(
    entry: QueryIndexEntry,
    token: string,
    scope: SearchFieldScope
): { score: number; snippet?: string } {
    let bestScore = 0;
    let snippet: string | undefined;

    const displayTitle = (entry.mdTitle ?? entry.title ?? '').toLowerCase();
    const tags = entry.mdTags?.map(t => t.toLowerCase()) ?? [];
    const bodyText = (entry.mdBodyText ?? '').toLowerCase();
    const pathLower = entry.path.toLowerCase();
    const fileName = pathLower.split('/').pop() ?? '';
    // searchText includes SQL content and everything else
    const searchText = (entry.searchText ?? '').toLowerCase();

    if (scope === 'all' || scope === 'title') {
        // 1. Exact title match (100)
        if (displayTitle === token) {
            bestScore = Math.max(bestScore, 100);
        }
        // 2. Prefix title match (80)
        else if (displayTitle.startsWith(token)) {
            bestScore = Math.max(bestScore, 80);
        }
        // Title contains (60)
        else if (displayTitle.includes(token)) {
            bestScore = Math.max(bestScore, 60);
        }
    }

    if (scope === 'all' || scope === 'tags') {
        // 3. Tag match (50)
        if (tags.some(t => t === token)) {
            bestScore = Math.max(bestScore, 50);
        } else if (tags.some(t => t.includes(token))) {
            bestScore = Math.max(bestScore, 40);
        }
    }

    if (scope === 'all' || scope === 'notes') {
        // 4. Markdown body match (30)
        if (bodyText.includes(token)) {
            if (bestScore < 30) {
                bestScore = 30;
                snippet = extractSnippet(entry.mdBodyText ?? '', token);
            }
        }
    }

    if (scope === 'all') {
        // 5. Path/filename match (20)
        if (fileName.includes(token)) {
            bestScore = Math.max(bestScore, 25);
        } else if (pathLower.includes(token)) {
            bestScore = Math.max(bestScore, 20);
        }
    }

    if (scope === 'all' || scope === 'sql') {
        // 6. SQL text match (10) — matched via searchText which includes SQL content
        if (bestScore === 0 && searchText.includes(token)) {
            bestScore = 10;
        }
    }

    return { score: bestScore, snippet };
}

function extractSnippet(text: string, token: string, contextChars = 80): string | undefined {
    const lower = text.toLowerCase();
    const idx = lower.indexOf(token.toLowerCase());
    if (idx === -1) return undefined;

    const start = Math.max(0, idx - contextChars);
    const end = Math.min(text.length, idx + token.length + contextChars);
    let snip = text.slice(start, end).trim();

    if (start > 0) snip = '...' + snip;
    if (end < text.length) snip = snip + '...';

    // Collapse whitespace
    return snip.replace(/\s+/g, ' ');
}
