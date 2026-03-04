import * as vscode from 'vscode';
import { Logger } from './logger';
import {
	formatConnectionError,
	formatAIError,
	formatTransformError,
	formatSchemaError,
	formatQueryError,
	formatERDError,
	formatFileSystemError,
	formatGeneralError,
	ErrorCategory
} from './errors/errorMessages';

/**
 * Error severity levels for determining user notification strategy
 */
export enum ErrorSeverity {
    /** Silent - log only, no user notification */
    Silent = 'silent',
    /** Info - show information message */
    Info = 'info',
    /** Warning - show warning message */
    Warning = 'warning',
    /** Error - show error message */
    Error = 'error',
    /** Critical - show modal error message */
    Critical = 'critical'
}

/**
 * Options for error handling
 */
export interface ErrorHandlerOptions {
    /** Error severity level */
    severity: ErrorSeverity;
    /** User-facing message (if not provided, uses error message) */
    userMessage?: string;
    /** Additional context for logging */
    context?: string;
    /** Whether to show modal dialog */
    modal?: boolean;
    /** Action buttons to show in notification */
    actions?: ErrorAction[];
}

/**
 * Action button for error notifications
 */
export interface ErrorAction {
    /** Button label */
    label: string;
    /** Action to execute when clicked */
    action: () => void | Promise<void>;
}

/**
 * Centralized error handler for consistent error handling across the extension
 */
export class ErrorHandler {
    /**
     * Handle an error with appropriate logging and user notification
     */
    static async handle(error: unknown, options: ErrorHandlerOptions): Promise<void> {
        const errorMessage = this.extractErrorMessage(error);
        const contextPrefix = options.context ? `[${options.context}] ` : '';
        const logMessage = `${contextPrefix}${errorMessage}`;

        // Always log the error
        switch (options.severity) {
            case ErrorSeverity.Silent:
            case ErrorSeverity.Info:
                Logger.info(logMessage);
                break;
            case ErrorSeverity.Warning:
                Logger.warn(logMessage, error);
                break;
            case ErrorSeverity.Error:
            case ErrorSeverity.Critical:
                Logger.error(logMessage, error);
                break;
        }

        // Show user notification based on severity
        const userMessage = options.userMessage || errorMessage;
        const notificationOptions = options.modal ? { modal: true } : {};

        switch (options.severity) {
            case ErrorSeverity.Silent:
                // No user notification
                break;

            case ErrorSeverity.Info:
                await this.showNotification(
                    vscode.window.showInformationMessage,
                    userMessage,
                    notificationOptions,
                    options.actions
                );
                break;

            case ErrorSeverity.Warning:
                await this.showNotification(
                    vscode.window.showWarningMessage,
                    userMessage,
                    notificationOptions,
                    options.actions
                );
                break;

            case ErrorSeverity.Error:
                await this.showNotification(
                    vscode.window.showErrorMessage,
                    userMessage,
                    notificationOptions,
                    options.actions
                );
                break;

            case ErrorSeverity.Critical:
                await this.showNotification(
                    vscode.window.showErrorMessage,
                    userMessage,
                    { modal: true },
                    options.actions
                );
                break;
        }
    }

    /**
     * Show notification with action buttons
     */
    private static async showNotification(
        showFn: (message: string, options: vscode.MessageOptions, ...items: string[]) => Thenable<string | undefined>,
        message: string,
        options: vscode.MessageOptions,
        actions?: ErrorAction[]
    ): Promise<void> {
        if (!actions || actions.length === 0) {
            await showFn(message, options);
            return;
        }

        const actionLabels = actions.map(a => a.label);
        const result = await showFn(message, options, ...actionLabels);

        if (result) {
            const action = actions.find(a => a.label === result);
            if (action) {
                await action.action();
            }
        }
    }

    /**
     * Extract error message from various error types
     * Public to allow reuse across the extension
     */
    static extractErrorMessage(error: unknown): string {
        if (error instanceof Error) {
            return error.message;
        }
        if (typeof error === 'string') {
            return error;
        }
        if (error && typeof error === 'object') {
            // Try common error properties
            const err = error as { message?: unknown; error?: unknown; toString?: () => string };
            if (err.message) return String(err.message);
            if (err.error) return this.extractErrorMessage(err.error);
            if (err.toString && err.toString !== Object.prototype.toString) {
                return err.toString();
            }
        }
        return 'An unknown error occurred';
    }

    /**
     * Wrap a function with error handling
     */
    static wrap<A extends unknown[], R>(
        fn: (...args: A) => R | Promise<R>,
        options: ErrorHandlerOptions
    ): (...args: A) => Promise<R | undefined> {
        return async (...args: A) => {
            try {
                return await fn(...args);
            } catch (error) {
                await this.handle(error, options);
                return undefined;
            }
        };
    }

    /**
     * Handle error with retry logic
     */
    static async handleWithRetry<T>(
        operation: () => Promise<T>,
        options: ErrorHandlerOptions & {
            maxRetries?: number;
            retryDelay?: number;
        }
    ): Promise<T | undefined> {
        const maxRetries = options.maxRetries || 3;
        const retryDelay = options.retryDelay || 1000;
        let lastError: unknown;

        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                return await operation();
            } catch (error) {
                lastError = error;
                Logger.warn(`Attempt ${attempt + 1}/${maxRetries} failed`, error);

                if (attempt < maxRetries - 1) {
                    await new Promise(resolve => setTimeout(resolve, retryDelay));
                }
            }
        }

        // All retries failed
        await this.handle(lastError, {
            ...options,
            userMessage: options.userMessage || `Operation failed after ${maxRetries} attempts`
        });

        return undefined;
    }
}

/**
 * Convenience functions for common error handling patterns
 */

/**
 * Handle error silently (log only, no user notification)
 */
export async function handleSilentError(error: unknown, context?: string): Promise<void> {
    await ErrorHandler.handle(error, {
        severity: ErrorSeverity.Silent,
        context
    });
}

/**
 * Handle error as warning (show warning notification)
 */
export async function handleWarning(error: unknown, userMessage?: string, context?: string): Promise<void> {
    await ErrorHandler.handle(error, {
        severity: ErrorSeverity.Warning,
        userMessage,
        context
    });
}

/**
 * Handle error (show error notification)
 */
export async function handleError(error: unknown, userMessage?: string, context?: string): Promise<void> {
    await ErrorHandler.handle(error, {
        severity: ErrorSeverity.Error,
        userMessage,
        context
    });
}

/**
 * Handle critical error (show modal error notification)
 */
export async function handleCriticalError(error: unknown, userMessage?: string, context?: string): Promise<void> {
    await ErrorHandler.handle(error, {
        severity: ErrorSeverity.Critical,
        userMessage,
        context
    });
}

/**
 * Re-export error message formatting utilities for convenience
 */
export {
	formatConnectionError,
	formatAIError,
	formatTransformError,
	formatSchemaError,
	formatQueryError,
	formatERDError,
	formatFileSystemError,
	formatGeneralError,
	ErrorCategory
};
