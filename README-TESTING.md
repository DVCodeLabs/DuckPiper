# Testing Summary

This document provides a quick overview of the testing infrastructure added to DuckPiper.

## What Was Added

### 1. Jest Testing Framework
- **Configuration**: `jest.config.js`
- **Test runner**: Jest with TypeScript support via ts-jest
- **VSCode Mock**: Comprehensive mock for VSCode APIs at `src/__tests__/__mocks__/vscode.ts`

### 2. Test Scripts (package.json)
```bash
npm test                # Run all tests
npm run test:watch      # Run tests in watch mode
npm run test:coverage   # Run tests with coverage report
npm run test:ci         # Run tests in CI mode
```

### 3. Unit Tests Created

#### Core Utilities (`src/core/__tests__/`)
- **hashing.test.ts** - 12 tests for SQL canonicalization and hashing
- **sqlUtils.test.ts** - 28 tests for SQL identifier quoting and sanitization
- **utils.test.ts** - 12 tests for filename sanitization
- **sqlLimitHelper.test.ts** - 22 tests for query result limiting
- **errorHandler.test.ts** - 8 tests for centralized error handling

#### Connection Layer (`src/connections/__tests__/`)
- **connectionErrors.test.ts** - 15 tests for database error mapping

#### Pipelines (`src/pipelines/__tests__/`)
- **sqlParser.test.ts** - 58 tests for SQL parsing and lineage extraction

**Total: 153 passing tests** across 7 test suites

### 4. CI/CD Pipeline
- **GitHub Actions workflow**: `.github/workflows/test.yml`
- Runs on: Ubuntu, macOS, and Windows
- Triggers: On push/PR to main or develop branches
- Steps:
  1. Lint code
  2. Run tests
  3. Build extension
  4. Upload coverage (optional)

### 5. Documentation
- **TESTING.md** - Comprehensive testing guide
- **CONTRIBUTING.md** - Contributor guidelines including testing
- **README-TESTING.md** - This file (quick reference)

### 6. Configuration Updates
- Updated `.gitignore` to exclude test artifacts
- Added test dependencies to `package.json`
- Configured Jest with TypeScript and coverage reporting

## Quick Start for Contributors

### Running Tests Locally

```bash
# First time setup
npm install

# Run all tests
npm test

# Run tests in watch mode (useful during development)
npm run test:watch

# Run specific test file
npm test -- hashing.test.ts

# Run tests with a specific pattern
npm test -- -t "should canonicalize"
```

### Writing New Tests

1. Create a test file next to the code you're testing:
   ```
   src/feature/myModule.ts
   src/feature/__tests__/myModule.test.ts
   ```

2. Write tests using Jest:
   ```typescript
   import { myFunction } from '../myModule';

   describe('myFunction', () => {
     it('should do something', () => {
       expect(myFunction('input')).toBe('expected');
     });
   });
   ```

3. Run your tests:
   ```bash
   npm test -- myModule.test.ts
   ```

### Test Coverage

View coverage reports:
```bash
npm run test:coverage
open coverage/lcov-report/index.html
```

## Test Organization

Tests are organized by feature/module:

```
src/
├── core/
│   ├── __tests__/
│   │   ├── hashing.test.ts
│   │   ├── sqlUtils.test.ts
│   │   └── ...
│   ├── hashing.ts
│   ├── sqlUtils.ts
│   └── ...
├── connections/
│   ├── __tests__/
│   │   └── connectionErrors.test.ts
│   └── ...
└── __tests__/
    └── __mocks__/
        └── vscode.ts (shared mock)
```

## What Gets Tested

### ✅ Currently Tested
- SQL canonicalization and hashing
- SQL identifier quoting (MySQL, PostgreSQL, MSSQL, etc.)
- SQL string literal escaping
- Identifier and filename sanitization
- Query result limiting logic
- Database connection error mapping
- Error handling with retry logic
- SQL parsing for lineage extraction
- Table alias resolution
- Column reference extraction

### 📝 Recommended Next Steps
- Database adapter integration tests
- Schema introspection tests
- Query execution tests (with test database)
- ERD generation tests
- AI service integration tests
- UI component tests (for React components)
- End-to-end tests for complete workflows

## Continuous Integration

### GitHub Actions
Tests run automatically on:
- Every push to `main` or `develop`
- Every pull request
- Multiple operating systems (Linux, macOS, Windows)

### Pre-commit Checks
Consider adding:
- Pre-commit hooks to run tests locally
- Husky for Git hooks
- Lint-staged for staged file linting

## Troubleshooting

### Tests fail locally but pass in CI
- Check Node.js version (should be 20.x)
- Clear node_modules and reinstall: `rm -rf node_modules && npm install`
- Clear Jest cache: `npx jest --clearCache`

### Mock issues
- VSCode mock is at `src/__tests__/__mocks__/vscode.ts`
- Add new VSCode APIs to the mock as needed
- Mock is automatically loaded via `moduleNameMapper` in jest.config.js

### Slow tests
- Use `jest --maxWorkers=4` to limit parallelism
- Mock expensive operations (database calls, file I/O)
- Use `jest.setTimeout()` for tests that need more time

## Resources

- [Jest Documentation](https://jestjs.io/docs/getting-started)
- [VSCode Extension Testing](https://code.visualstudio.com/api/working-with-extensions/testing-extension)
- [TESTING.md](./TESTING.md) - Full testing guide
- [CONTRIBUTING.md](./CONTRIBUTING.md) - Contribution guidelines

## Statistics

- **Test Files**: 7
- **Total Tests**: 153
- **Pass Rate**: 100%
- **Test Duration**: ~0.3s (without coverage)
- **Platforms Tested**: Ubuntu, macOS, Windows

---

For detailed testing guidelines, see [TESTING.md](./TESTING.md).
For contribution guidelines, see [CONTRIBUTING.md](./CONTRIBUTING.md).
