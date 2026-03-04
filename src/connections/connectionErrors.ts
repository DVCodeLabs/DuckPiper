/**
 * Utility module for mapping database connection errors to user-friendly messages.
 */

/**
 * Maps a database connection error to a user-friendly reason string.
 * Handles common error codes (ECONNREFUSED, ETIMEDOUT, etc.) and AggregateError.
 * Returns just the reason portion - use with formatConnectionError() for full formatting.
 */
export function mapDatabaseError(error: unknown): string {
    if (!error) return 'Connection test failed';

    const e = error as { code?: string; message?: string; name?: string; constructor?: { name?: string }; errors?: unknown[] };
    const code = e.code;
    const message = e.message || '';

    // Handle AggregateError (multiple connection attempts failed, e.g., IPv4 + IPv6)
    if (e.name === 'AggregateError' || e.constructor?.name === 'AggregateError') {
        // Try to get details from the first error in the array
        const firstError = e.errors?.[0];
        if (firstError) {
            return mapDatabaseError(firstError);
        }
    }

    // SSH tunnel errors (from sshTunnel.ts)
    switch (code) {
        case 'SSH_VALIDATION':
            return message || 'SSH tunnel configuration is incomplete. Check SSH settings.';
        case 'SSH_AUTH_FAILED':
            return 'SSH authentication failed. Check SSH username and password/key.';
        case 'SSH_INVALID_KEY':
            return 'Invalid SSH private key or passphrase mismatch.';
        case 'SSH_HOST_UNREACHABLE':
            return 'SSH host unreachable. Check SSH host and network connectivity.';
        case 'SSH_TIMEOUT':
            return 'SSH connection timed out. Check SSH host and port.';
        case 'SSH_TUNNEL_FAILED':
            return 'SSH tunnel established but port forwarding failed. Check database host and port.';
    }

    // Map common error codes to friendly messages with suggestions
    switch (code) {
        case 'ECONNREFUSED':
            return 'Connection refused. Is the database server running on the specified host and port?';
        case 'ETIMEDOUT':
            return 'Connection timed out. Check the host, port, and network connectivity.';
        case 'ENOTFOUND':
            return 'Host not found. Check that the hostname is correct.';
        case 'ECONNRESET':
            return 'Connection was reset by the server. Check network stability.';
        case 'EHOSTUNREACH':
            return 'Host is unreachable. Check network connectivity.';
        case '28P01': // PostgreSQL invalid password
            return 'Authentication failed. Check username and password.';
        case '28000': // PostgreSQL invalid authorization
            return 'Authorization failed. Check username and database permissions.';
        case '3D000': // PostgreSQL invalid database
            return 'Database does not exist. Check the database name.';
        case 'EACCES':
            return 'Permission denied. Check file or socket permissions.';
        case 'EADDRINUSE':
            return 'Address already in use. Another process may be using the port.';
        default:
            // Return original message if available, otherwise generic
            return message || 'Connection test failed. Check your connection settings.';
    }
}

/**
 * Formats a database connection error with standardized messaging.
 * @param error The error object from the database connection attempt
 * @returns Formatted error message following the standard template
 */
export function formatDatabaseConnectionError(error: unknown): string {
    return mapDatabaseError(error);
}
