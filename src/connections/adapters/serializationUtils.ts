/**
 * Utilities for ensuring database query results can be safely serialized to JSON.
 * Different database drivers return values in different formats (e.g., BigInt, Buffer, Date),
 * which may not be directly JSON-serializable.
 */

/**
 * Recursively converts BigInt values to Numbers or strings for JSON serialization.
 * 
 * - Converts BigInt to Number if within JavaScript's safe integer range
 * - Converts BigInt to string if outside safe range to preserve precision
 * - Handles nested objects and arrays
 * 
 * @param obj - Any value that may contain BigInt values
 * @returns The same structure with BigInt values converted
 */
export function convertBigIntForSerialization(obj: unknown): unknown {
    if (obj === null || obj === undefined) {
        return obj;
    }

    if (typeof obj === 'bigint') {
        // Convert to number if safe, otherwise to string to preserve precision
        return obj <= Number.MAX_SAFE_INTEGER && obj >= Number.MIN_SAFE_INTEGER
            ? Number(obj)
            : obj.toString();
    }

    // Handle Date objects (prevent them from being treated as generic objects)
    if (obj instanceof Date) {
        return isNaN(obj.getTime()) ? null : obj.toISOString();
    }

    // Handle Buffers (optional, but good safety)
    if (Buffer.isBuffer(obj)) {
        return obj.toString('base64'); // or generic buffer handling
    }

    if (Array.isArray(obj)) {
        return obj.map(convertBigIntForSerialization);
    }

    if (typeof obj === 'object') {
        const converted: Record<string, unknown> = {};
        for (const key in obj) {
            if (Object.prototype.hasOwnProperty.call(obj, key)) {
                converted[key] = convertBigIntForSerialization((obj as Record<string, unknown>)[key]);
            }
        }
        return converted;
    }

    return obj;
}
