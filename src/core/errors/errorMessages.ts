/**
 * Error Message Formatting Utilities
 *
 * Provides standardized error message formatting across the DuckPiper extension.
 * All messages follow the template: "Operation failed: reason. Suggestion."
 */

/**
 * Error category for internal organization
 */
export enum ErrorCategory {
	CONNECTION = 'Connection',
	AI = 'AI',
	TRANSFORM = 'Transform',
	SCHEMA = 'Schema',
	QUERY = 'Query',
	ERD = 'ERD',
	FILESYSTEM = 'FileSystem',
	GENERAL = 'General'
}

/**
 * Formats a standardized error message
 * @param operation What operation was being attempted
 * @param reason Why it failed
 * @param suggestion Optional suggestion for how to resolve (omit period if provided)
 * @returns Formatted error message
 */
export function formatErrorMessage(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	const base = `${operation}: ${reason}`;
	if (suggestion) {
		// Ensure suggestion ends with a period
		const formattedSuggestion = suggestion.endsWith('.') ? suggestion : `${suggestion}.`;
		return `${base}. ${formattedSuggestion}`;
	}
	return `${base}.`;
}

/**
 * Formats connection-related error messages
 * @param operation Connection operation (e.g., "Connection test", "Query execution")
 * @param reason Why the operation failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted connection error message
 */
export function formatConnectionError(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	return formatErrorMessage(operation, reason, suggestion);
}

/**
 * Formats AI/LLM-related error messages
 * @param operation AI operation (e.g., "Documentation generation", "Chart generation")
 * @param provider AI provider name (e.g., "OpenAI", "Anthropic")
 * @param reason Why the operation failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted AI error message
 */
export function formatAIError(
	operation: string,
	provider: string,
	reason: string,
	suggestion?: string
): string {
	const fullReason = `${provider} - ${reason}`;
	return formatErrorMessage(operation, fullReason, suggestion);
}

/**
 * Formats transform/pipeline-related error messages
 * @param operation Transform operation (e.g., "Cell execution", "Layer validation")
 * @param reason Why the operation failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted transform error message
 */
export function formatTransformError(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	return formatErrorMessage(operation, reason, suggestion);
}

/**
 * Formats schema-related error messages
 * @param operation Schema operation (e.g., "Schema introspection", "Description generation")
 * @param reason Why the operation failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted schema error message
 */
export function formatSchemaError(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	return formatErrorMessage(operation, reason, suggestion);
}

/**
 * Formats query library-related error messages
 * @param operation Query operation (e.g., "Query save", "Bundle creation")
 * @param reason Why the operation failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted query error message
 */
export function formatQueryError(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	return formatErrorMessage(operation, reason, suggestion);
}

/**
 * Formats ERD-related error messages
 * @param operation ERD operation (e.g., "ERD generation", "Relationship save")
 * @param reason Why the operation failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted ERD error message
 */
export function formatERDError(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	return formatErrorMessage(operation, reason, suggestion);
}

/**
 * Formats file system-related error messages
 * @param operation File system operation (e.g., "Workspace access", "File read")
 * @param reason Why the operation failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted file system error message
 */
export function formatFileSystemError(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	return formatErrorMessage(operation, reason, suggestion);
}

/**
 * Formats general error messages that don't fit other categories
 * @param operation What operation was being attempted
 * @param reason Why it failed
 * @param suggestion Optional suggestion for resolution
 * @returns Formatted error message
 */
export function formatGeneralError(
	operation: string,
	reason: string,
	suggestion?: string
): string {
	return formatErrorMessage(operation, reason, suggestion);
}

/**
 * Note: For extracting error messages from unknown error types,
 * use ErrorHandler.extractErrorMessage() instead of creating duplicates.
 */
