

export function heuristicComments(text: string): { line: number; text: string }[] {
    const comments: { line: number; text: string }[] = [];
    const lines = text.split(/\r?\n/);

    const patterns: Array<{ re: RegExp; label: string }> = [
        { re: /^\s*select\b/i, label: "Select fields for the output." },
        { re: /^\s*from\b/i, label: "Choose the primary table(s) and set grain." },
        { re: /^\s*join\b/i, label: "Join additional tables for enrichment." },
        { re: /^\s*where\b/i, label: "Apply filters and constraints." },
        { re: /^\s*group\s+by\b/i, label: "Aggregate results by grouping keys." },
        { re: /^\s*having\b/i, label: "Filter aggregated results." },
        { re: /^\s*order\s+by\b/i, label: "Sort output for readability/consumption." }
    ];

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        for (const p of patterns) {
            if (p.re.test(line)) {
                comments.push({ line: i + 1, text: p.label });
                break;
            }
        }
    }

    return comments.filter((c, idx, arr) => arr.findIndex((x) => x.line === c.line) === idx);
}
