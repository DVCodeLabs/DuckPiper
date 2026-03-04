
/**
 * Sanitizes a string to be safe for use as a filename.
 * Replaces invalid characters (including path separators) with underscores.
 * 
 * @param name The file name to sanitize
 * @returns The sanitized file name
 */
export function sanitizeFilename(name: string): string {
    // Replace invalid characters with underscore
    // Windows: < > : " / \ | ? *
    // Linux/Mac: / (and null byte, though unlikely in string)
    // We also want to prevent directory traversal (../)
    return name.replace(/[/\\?%*:|"<>]/g, '_');
}
