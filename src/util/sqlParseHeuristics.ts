export function extractTables(sql: string): string[] {
    const results = new Set<string>();
    const cleaned = sql
        .replace(/--.*$/gm, '')
        .replace(/\/\*[\s\S]*?\*\//g, ' ');

    const tableRef = /\b(from|join|into|update|table)\s+([a-zA-Z_][\w]*)(?:\s*\.\s*([a-zA-Z_][\w]*))?(?:\s*\.\s*([a-zA-Z_][\w]*))?/gi;

    let match: RegExpExecArray | null;
    while ((match = tableRef.exec(cleaned)) !== null) {
        const part1 = match[2];
        const part2 = match[3];
        const part3 = match[4];

        if (part2 && part3) {
            // three-part ref: source.schema.table -> take schema.table
            results.add(`${part2}.${part3}`);
        } else if (part2) {
            // two-part ref: schema.table
            results.add(`${part1}.${part2}`);
        }
    }

    return Array.from(results);
}
