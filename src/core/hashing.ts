import * as crypto from "crypto";

export interface CanonicalSql {
  canonicalText: string;
  sqlHash: string;
}

/**
 * v0 canonicalization:
 * - strip block comments /* ... *\/
 * - strip line comments -- ...
 * - normalize whitespace to single spaces
 * - trim
 * - remove trailing semicolons
 * - lowercase all
 */
export function canonicalizeSql(input: string): CanonicalSql {
  // Remove block comments (non-greedy, across lines)
  let s = input.replace(/\/\*[\s\S]*?\*\//g, " ");

  // Remove line comments (--) to end of line
  s = s.replace(/--.*$/gm, " ");

  // Normalize whitespace
  s = s.replace(/\s+/g, " ").trim();

  // Remove trailing semicolons (one or many)
  s = s.replace(/;+\s*$/g, "");

  // Lowercase
  s = s.toLowerCase();

  const sqlHash = crypto.createHash("sha256").update(s, "utf8").digest("hex");
  return { canonicalText: s, sqlHash };
}
