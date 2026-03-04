# Testing Guide

This document describes the testing strategy and practices for the DuckPiper VSCode extension.

## Overview

DuckPiper uses [Jest](https://jestjs.io/) as its testing framework with TypeScript support via `ts-jest`. The test suite includes unit tests for core utilities, connection adapters, SQL parsing, and error handling.

## Running Tests

### Basic Commands

```bash
# Run all tests
npm test

# Run tests in watch mode (useful during development)
npm run test:watch

# Run tests with coverage report
npm run test:coverage

# Run tests in CI mode (used by GitHub Actions)
npm run test:ci
```

### Test Output

- Test results are displayed in the terminal
- Coverage reports are generated in the `coverage/` directory
- Coverage includes HTML reports for easy browsing: `coverage/lcov-report/index.html`

## Test Structure

Tests are organized using the `__tests__` directory pattern:

```
src/
├── core/
│   ├── __tests__/
│   │   ├── hashing.test.ts
│   │   ├── sqlUtils.test.ts
│   │   ├── utils.test.ts
│   │   ├── sqlLimitHelper.test.ts
│   │   └── errorHandler.test.ts
│   ├── hashing.ts
│   ├── sqlUtils.ts
│   └── ...
├── connections/
│   ├── __tests__/
│   │   └── connectionErrors.test.ts
│   └── ...
├── pipelines/
│   ├── __tests__/
│   │   └── sqlParser.test.ts
│   └── ...
└── __tests__/
    └── __mocks__/
        └── vscode.ts
```

## VSCode Mock

The extension uses a comprehensive VSCode mock located at `src/__tests__/__mocks__/vscode.ts`. This mock provides stubs for:

- `workspace` API (configuration, file system)
- `window` API (messages, UI elements)
- `commands` API (command registration and execution)
- `languages` API (providers and diagnostics)
- Common VSCode types (Uri, Range, Position, etc.)

The mock is automatically loaded by Jest for all tests through the `moduleNameMapper` configuration in `jest.config.js`.

## Test Categories

### Core Utilities Tests

Tests for pure utility functions that don't depend on VSCode APIs:

- **Hashing** (`core/__tests__/hashing.test.ts`): SQL canonicalization and hash generation
- **SQL Utils** (`core/__tests__/sqlUtils.test.ts`): SQL identifier quoting, literal escaping, sanitization
- **Utils** (`core/__tests__/utils.test.ts`): File name sanitization
- **SQL Limit Helper** (`core/__tests__/sqlLimitHelper.test.ts`): Query result limiting logic

### Error Handling Tests

Tests for error detection, mapping, and user-friendly error messages:

- **Connection Errors** (`connections/__tests__/connectionErrors.test.ts`): Database connection error mapping
- **Error Handler** (`core/__tests__/errorHandler.test.ts`): Central error handling with severity levels

### SQL Parsing Tests

Tests for SQL query parsing and lineage extraction:

- **SQL Parser** (`pipelines/__tests__/sqlParser.test.ts`): Heuristic SQL parser for column lineage

## Writing Tests

### Basic Test Structure

```typescript
import { functionToTest } from '../module';

describe('ModuleName', () => {
  describe('functionToTest', () => {
    it('should do something specific', () => {
      const result = functionToTest('input');
      expect(result).toBe('expected output');
    });

    it('should handle edge case', () => {
      const result = functionToTest('edge case input');
      expect(result).toBeNull();
    });
  });
});
```

### Testing with VSCode APIs

When testing code that uses VSCode APIs, the mock is automatically available:

```typescript
import * as vscode from 'vscode';

describe('MyFeature', () => {
  it('should show error message', async () => {
    await myFunction();

    expect(vscode.window.showErrorMessage).toHaveBeenCalledWith(
      expect.stringContaining('error occurred')
    );
  });
});
```

### Mocking Dependencies

```typescript
// Mock a module
jest.mock('../logger');

// Mock specific functions
const mockFunction = jest.fn().mockReturnValue('mocked value');
```

## Coverage Goals

The project aims for:

- **Overall coverage**: > 70%
- **Core utilities**: > 90%
- **Critical paths**: 100% (error handling, SQL parsing)

To view coverage report:

```bash
npm run test:coverage
open coverage/lcov-report/index.html
```

## Continuous Integration

Tests run automatically on:

- Every push to `main` or `develop` branches
- Every pull request to `main` or `develop` branches
- Multiple platforms: Ubuntu, macOS, and Windows
- Node.js version: 20.x

See `.github/workflows/test.yml` for the complete CI configuration.

## Best Practices

1. **Write focused tests**: Each test should verify one specific behavior
2. **Use descriptive names**: Test names should clearly describe what they test
3. **Test edge cases**: Don't just test the happy path
4. **Keep tests independent**: Tests should not depend on each other
5. **Mock external dependencies**: Use mocks for VSCode APIs, file system, network calls
6. **Test error conditions**: Verify that errors are handled correctly
7. **Maintain tests**: Update tests when you update code

## Common Test Patterns

### Testing async functions

```typescript
it('should handle async operation', async () => {
  const result = await asyncFunction();
  expect(result).toBe('expected');
});
```

### Testing errors

```typescript
it('should throw error on invalid input', () => {
  expect(() => {
    functionThatThrows();
  }).toThrow('Expected error message');
});
```

### Testing with beforeEach/afterEach

```typescript
describe('Feature', () => {
  let instance: MyClass;

  beforeEach(() => {
    instance = new MyClass();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should work', () => {
    // test using instance
  });
});
```

## Debugging Tests

### Run a specific test file

```bash
npm test -- hashing.test.ts
```

### Run a specific test

```bash
npm test -- -t "should remove block comments"
```

### Debug in VSCode

Add to `.vscode/launch.json`:

```json
{
  "type": "node",
  "request": "launch",
  "name": "Jest Debug",
  "program": "${workspaceFolder}/node_modules/.bin/jest",
  "args": ["--runInBand", "${file}"],
  "console": "integratedTerminal",
  "internalConsoleOptions": "neverOpen"
}
```

## Contributing

When adding new features:

1. Write tests for new functionality
2. Update existing tests if behavior changes
3. Ensure all tests pass before submitting PR
4. Aim to maintain or improve coverage

## Resources

- [Jest Documentation](https://jestjs.io/docs/getting-started)
- [Testing TypeScript with Jest](https://jestjs.io/docs/getting-started#using-typescript)
- [VSCode Extension Testing](https://code.visualstudio.com/api/working-with-extensions/testing-extension)
