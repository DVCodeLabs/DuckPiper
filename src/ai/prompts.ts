import * as vscode from "vscode";
import { ensureDPDirs, fileExists } from "../core/fsWorkspace";

type PromptKey = "markdownDoc" | "inlineComments" | "describeSchema" | "notebookMarkdownDoc";

const PROMPT_DIR = "system/prompts";

const DEFAULT_PROMPTS: Record<PromptKey, string> = {
    markdownDoc: [
        "# Query Documentation",
        "",
        "> **Role:** You are a data analyst documenting a complex query for other data analysts and business analysts.",
        "> **Output:** Return **markdown only** (no surrounding code fences).",
        "> **Style:** Be concise, specific, and use the same table/column names as the SQL. Avoid filler.",
        "",
        "Dialect: {{dialect}}",
        "Connection: {{connection}}",
        "{{schemaContext}}",
        "",
        "---",
        "",
        "# What this query answers",
        "- **Primary business question:**",
        "- **Why it matters:** (decision or workflow this supports)",
        "- **Definitions (if needed):** (e.g., what “active”, “churned”, “qualified” means in this query)",
        "",
        "# Inputs",
        "## Source tables / views",
        "- `<table_or_view>`: (what it represents, in business terms)",
        "",
        "## Parameters / filters",
        "- **Date range:** (where it comes from — literal, parameter, or inferred)",
        "- **Key filters:** (important WHERE conditions and what they mean)",
        "",
        "## Join keys",
        "- `<left_table>.<key>` ↔ `<right_table>.<key>`: (purpose of the join)",
        "",
        "# Business logic",
        "## Step-by-step flow",
        "- **1.** (CTE/subquery name): purpose + what it outputs",
        "- **2.** …",
        "",
        "## Calculations & rules",
        "- **Metric:** `<metric_name>` = (plain-English explanation + formula reference)",
        "- **Classification/flags:** (CASE logic → what each label means)",
        "- **Deduping / ranking:** (ROW_NUMBER/RANK + partition/order intent)",
        "",
        "## Grain management",
        "- **Intended grain:** (what one row represents at the end)",
        "- **How grain changes:** (where aggregation happens and why)",
        "",
        "# Output",
        "## Result grain",
        "- (e.g., one row per customer per month)",
        "",
        "## Columns returned",
        "- `<column>`: meaning (and source table/CTE if not obvious)",
        "- `<metric>`: meaning + how computed",
        "",
        "## Expected value characteristics (optional)",
        "- Typical ranges / examples (only if confident from context)",
        "",
        "# Caveats",
        "- **Data quality assumptions:** (e.g., keys unique, late-arriving data, NULL handling)",
        "- **Join risks:** (row multiplication, missing matches, LEFT→INNER pitfalls)",
        "- **Time semantics:** (timezone, inclusive/exclusive boundaries, snapshot timing)",
        "- **Edge cases:** (division by zero, missing dimensions, duplicates)",
        "",
        "# Performance notes",
        "- **Likely expensive operations:** (large joins, window functions, DISTINCT, unbounded scans)",
        "- **Partition pruning / clustering opportunities:** (date filters, partition columns)",
        "- **Recommended indexes / keys:** (if applicable to dialect/warehouse)",
        "- **Possible optimizations:** (push filters earlier, reduce columns, pre-aggregate, materialize CTE)",
        "",
        "---",
        "",
        "## Appendix: SQL",
        "{{sql}}"
    ].join("\n"),
    notebookMarkdownDoc: [
        "# Notebook Documentation",
        "",
        "> **Role:** You are a data analyst documenting a SQL notebook data pipeline.",
        "> **Goal:** Explain the end-to-end data transformation flow.",
        "> **Output:** Return **markdown only** (no surrounding code fences).",
        "",
        "---",
        "",
        "# Overview",
        "- **Objective:** (What is this notebook trying to achieve?)",
        "- **Key Outputs:** (List the main tables/views created)",
        "",
        "# Data Flow Steps",
        "Describe the pipeline step-by-step based on the cells below.",
        "",
        "{{cellContent}}",
        "",
        "---",
        "",
        "# Assumptions & Logic",
        "- **Core Transformation Logic:** (Summarize key business rules applied)",
        "- **Dependencies:** (What source tables are required)",
        ""
    ].join("\n"),
    inlineComments: [
        "# Inline SQL Commenter",
        "",
        "## Objective",
        "Add inline comments to an existing SQL query so other analysts can understand the intent and tricky parts **without changing the SQL or formatting**.",
        "",
        "## Output Requirements",
        "- Output **VALID SQL only** (no markdown fences, no extra prose).",
        "- Preserve the query **exactly**:",
        "  - Do **not** add/remove/reorder lines.",
        "  - Do **not** change indentation, spacing, capitalization, or punctuation.",
        "  - Do **not** change identifiers, literals, or clause structure.",
        "- You may **only** append comments to existing non-empty lines using: ` -- comment`",
        "- Comments must be on the **same line** as the SQL they describe (append-only).",
        "- Do **not** add comments on empty lines.",
        "- Do **not** introduce `#` comments.",
        "- Do **not** comment out SQL (no leading `--` before SQL).",
        "- Do **not** add generic comments (e.g., don’t comment `LIMIT 100`).",
        "",
        "## What to Comment",
        "Add comments only where it helps comprehension:",
        "- Non-obvious joins (join keys + why this join exists)",
        "- Complex filters (business meaning + edge cases)",
        "- Window functions (partition/order + what it achieves)",
        "- CASE logic (business rule / classification)",
        "- Aggregations (grain + what is being rolled up)",
        "- Deduping patterns (row_number qualify, distinct used for a reason)",
        "- Time logic (timezone, date boundaries, “as of” logic)",
        "",
        "## Comment Style",
        "- Keep comments concise (aim for < 80 characters if possible).",
        "- Prefer business intent over restating syntax.",
        "- If a line already has a `--` comment, keep it and append nothing further.",
        "- Do not add comments inside string literals.",
        "",
        "## Context",
        "Dialect: {{dialect}}  ",
        "Connection: {{connection}}  ",
        "{{schemaContext}}",
        "",
        "## Sacred SQL (append-only comments)",
        "{{sql}}"
    ].join("\n"),
    describeSchema: [
        "# Table & Column Metadata Generator",
        "",
        "You are a **database and analytics expert** responsible for producing high-quality metadata that will be used by humans *and* downstream systems (LLMs, query generators, documentation tools).",
        "",
        "Your task is to generate **concise, accurate descriptions** and **realistic sample values** for a table and each of its columns.",
        "",
        "---",
        "",
        "## Output Rules (Strict)",
        "- Return **strictly valid JSON only**.",
        "- Do **not** include markdown, comments, explanations, or extra text.",
        "- The JSON must conform **exactly** to the format specified below.",
        "- Every column listed in the input **must** appear once in the output.",
        "",
        "---",
        "",
        "## Output Format",
        "```json",
        '{',
        '  "schemaDescription": "string (1-2 sentence description of what this schema contains)",',
        '  "table": {',
        '    "key": "{{schemaName}}.{{tableName}}",',
        '    "description": "string"',
        '  },',
        '  "columns": [',
        '    {',
        '      "key": "{{schemaName}}.{{tableName}}.<columnName>",',
        '      "description": "string",',
        '      "sampleValue": "string"',
        '    }',
        '  ]',
        '}',
        "",
        "## Input Context",
        "",
        "**Schema:** {{schemaName}}  ",
        "**Table:** {{tableName}}",
        "",
        "**Columns (name + type if available):**  ",
        "{{columns}}",
        "",
        "**Additional constraints (if provided):**  ",
        "{{tableConstraint}}  ",
        "{{columnsConstraint}}",
        "",
        "---",
        "",
        "## Description Guidelines",
        "- Write descriptions for **data analysts and business analysts**.",
        "- Describe **what the data represents**, not how SQL works.",
        "- Be precise but concise (typically **1 sentence**).",
        "- Avoid vague phrases like *“stores information about”* or *“used to track”*.",
        "",
        "---",
        "",
        "## Sample Value Guidelines",
        "Provide **one realistic example value per column** that matches:",
        "- The column’s data type",
        "- The column’s business meaning",
        "- How values would realistically appear in production data",
        "",
        "### Conventions",
        "- **Strings:** realistic names, emails, codes, labels  ",
        "  _e.g._ \"john.doe@company.com\", \"PAID\"",
        "- **Numbers:** plausible values with correct scale  ",
        "  _e.g._ 42, 199.99, 0.873",
        "- **Dates:** ISO date format  ",
        "  _e.g._ \"2024-03-15\"",
        "- **Timestamps:** ISO 8601 UTC  ",
        "  _e.g._ \"2024-03-15T10:30:00Z\"",
        "- **Booleans:** \"true\" or \"false\"",
        "- **Tinyint flags / status fields:** \"0\" or \"1\"",
        "- **IDs / keys:** realistic identifiers (UUIDs, numeric IDs, or codes as appropriate)",
        "",
        "---",
        "",
        "## Quality Bar",
        "- Assume this metadata will be reused for:",
        "  - Query generation",
        "  - Query explanations",
        "  - Inline SQL comments",
        "  - Documentation",
        "- Optimize for **clarity, correctness, and reusability**.",
        "- If column intent is ambiguous, infer the **most likely real-world meaning** based on name and context.",
        "",
        "**Return the JSON object only.**"
    ].join("\n")
};

export async function loadPromptTemplate(key: PromptKey): Promise<string> {
    const dpDir = await ensureDPDirs();
    const dirUri = vscode.Uri.joinPath(dpDir, PROMPT_DIR);
    const fileUri = vscode.Uri.joinPath(dirUri, `${key}.txt`);

    if (!(await fileExists(fileUri))) {
        await vscode.workspace.fs.createDirectory(dirUri);
        await vscode.workspace.fs.writeFile(fileUri, Buffer.from(DEFAULT_PROMPTS[key], "utf8"));
        return DEFAULT_PROMPTS[key];
    }

    const bytes = await vscode.workspace.fs.readFile(fileUri);
    return new TextDecoder("utf-8").decode(bytes);
}

export function renderPrompt(template: string, vars: Record<string, string>): string {
    return template.replace(/\{\{(\w+)\}\}/g, (_, key) => vars[key] ?? "");
}

/**
 * Initialize all prompt files on extension activation.
 * Creates any missing prompt files with default content.
 */
export async function initializePromptFiles(): Promise<void> {
    const DPDir = await ensureDPDirs();
    const dirUri = vscode.Uri.joinPath(DPDir, PROMPT_DIR);

    // Ensure directory exists
    try {
        await vscode.workspace.fs.createDirectory(dirUri);
    } catch {
        // Directory may already exist
    }

    // Create all default prompt files if they don't exist
    const promptKeys = Object.keys(DEFAULT_PROMPTS) as PromptKey[];
    for (const key of promptKeys) {
        const fileUri = vscode.Uri.joinPath(dirUri, `${key}.txt`);
        if (!(await fileExists(fileUri))) {
            await vscode.workspace.fs.writeFile(fileUri, Buffer.from(DEFAULT_PROMPTS[key], "utf8"));
        }
    }
}
