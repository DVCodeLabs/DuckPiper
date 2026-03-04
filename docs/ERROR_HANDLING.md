# Error Handling Guidelines

This document outlines error handling best practices for the DuckPiper extension.

## Error Handler Utility

The extension includes a centralized `ErrorHandler` class in `src/core/errorHandler.ts` that provides consistent error handling across the codebase.

### Usage

```typescript
import { ErrorHandler, ErrorSeverity, handleError, handleWarning } from '../core/errorHandler';

// Simple error handling with user notification
try {
    await someOperation();
} catch (error) {
    await handleError(error, 'Failed to perform operation', 'OperationContext');
}

// Advanced error handling with action buttons
try {
    await connectToDatabase();
} catch (error) {
    await ErrorHandler.handle(error, {
        severity: ErrorSeverity.Error,
        userMessage: 'Failed to connect to database',
        context: 'DatabaseConnection',
        actions: [
            {
                label: 'Retry',
                action: async () => await connectToDatabase()
            },
            {
                label: 'Settings',
                action: async () => await openSettings()
            }
        ]
    });
}
```

## Error Severity Levels

| Severity | When to Use | User Notification | Example |
|----------|-------------|-------------------|---------|
| `Silent` | Expected failures, fallback scenarios | None (logged only) | File doesn't exist, directory already exists |
| `Info` | Informational, non-critical | Info message | Operation completed with warnings |
| `Warning` | Degraded functionality, recoverable | Warning message | Missing optional configuration |
| `Error` | Operation failed, user action may be needed | Error message | Query execution failed, connection error |
| `Critical` | System-level failure, immediate attention | Modal error | Extension activation failed, data corruption |

## Best Practices

### 1. Always Log Errors

Even if you don't show a user notification, always log the error for debugging:

```typescript
try {
    await operation();
} catch (error) {
    Logger.error('Operation failed', error);
    // Handle gracefully
}
```

### 2. Provide Context

Include contextual information in error messages:

```typescript
// ❌ Bad - Generic message
await handleError(error, 'Operation failed');

// ✅ Good - Specific context
await handleError(error, `Failed to load schema for connection '${connectionName}'`, 'SchemaLoad');
```

### 3. Avoid Empty Catch Blocks

Never use empty catch blocks without explanation:

```typescript
// ❌ Bad - Silent failure with no context
try {
    await operation();
} catch { }

// ✅ Good - Documented intentional ignore
try {
    await vscode.workspace.fs.createDirectory(dir);
} catch {
    // Directory already exists - this is expected and safe to ignore
}

// ✅ Better - Log for debugging
try {
    await operation();
} catch (error) {
    Logger.debug('Optional operation failed, continuing', error);
}
```

### 4. User-Facing vs Internal Errors

Distinguish between errors that users can act on vs internal errors:

```typescript
// User can fix this
await handleError(error, 'Connection failed. Please check your credentials.');

// Internal error - log details but show simple message
Logger.error('Introspection failed', error);
await vscode.window.showErrorMessage('Failed to load database schema');
```

### 5. Provide Actionable Feedback

When possible, suggest actions the user can take:

```typescript
await ErrorHandler.handle(error, {
    severity: ErrorSeverity.Error,
    userMessage: 'Database connection failed',
    actions: [
        { label: 'Test Connection', action: () => testConnection() },
        { label: 'Edit Connection', action: () => editConnection() },
        { label: 'View Logs', action: () => Logger.show() }
    ]
});
```

### 6. Graceful Degradation

When possible, continue with reduced functionality rather than failing completely:

```typescript
try {
    const metadata = await loadMetadata();
    return { data, metadata };
} catch (error) {
    Logger.warn('Failed to load metadata, continuing without it', error);
    return { data, metadata: null };
}
```

### 7. Retry Logic for Transient Failures

Use `ErrorHandler.handleWithRetry()` for operations that might fail transiently:

```typescript
const result = await ErrorHandler.handleWithRetry(
    () => fetchDataFromAPI(),
    {
        severity: ErrorSeverity.Error,
        userMessage: 'Failed to fetch data from API',
        context: 'APIFetch',
        maxRetries: 3,
        retryDelay: 1000
    }
);
```

## Common Patterns

### Connection Errors

```typescript
try {
    await adapter.testConnection(profile, secrets);
    await vscode.window.showInformationMessage(
        `Connected to '${profile.name}' successfully!`
    );
} catch (error) {
    await handleError(
        error,
        `Connection to '${profile.name}' failed: ${formatConnectionError(error)}`,
        'ConnectionTest'
    );
}
```

### Query Execution Errors

```typescript
try {
    const result = await adapter.runQuery(profile, secrets, sql, options);
    return result;
} catch (error) {
    await handleError(
        error,
        'Query execution failed. Check the query syntax and connection status.',
        'QueryExecution'
    );
    return undefined;
}
```

### File Operations

```typescript
try {
    const data = await readJson(uri);
    return data;
} catch (error) {
    Logger.warn(`Failed to read ${uri.fsPath}`, error);
    return getDefaultData();
}
```

## Migration Guide

When updating existing error handling:

1. **Identify the error severity** - Is this user-facing? Critical?
2. **Add proper logging** - Use Logger instead of console
3. **Provide user feedback** - Use ErrorHandler for user-facing errors
4. **Add context** - Include operation name and relevant details
5. **Consider recovery** - Can the operation continue? Should it retry?

## Examples

### Before

```typescript
try {
    await saveData();
} catch (e) {
    console.error(e);
}
```

### After

```typescript
try {
    await saveData();
} catch (error) {
    await handleError(
        error,
        'Failed to save data. Please try again.',
        'DataSave'
    );
}
```

## Testing Error Handling

When testing, verify:

1. ✅ Error is logged with appropriate level
2. ✅ User receives appropriate notification (if applicable)
3. ✅ Error message is clear and actionable
4. ✅ Application continues to function (graceful degradation)
5. ✅ No sensitive information is exposed in error messages
