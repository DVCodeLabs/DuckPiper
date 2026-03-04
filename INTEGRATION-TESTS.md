# Integration Testing Guide

This document describes the integration testing requirements for DuckPiper.

## Overview

While DuckPiper has comprehensive unit tests for core utilities (153 tests), some components require **integration testing** with actual dependencies:

- Database adapters (DuckDB, PostgreSQL, MySQL, Snowflake)
- Schema introspection
- File system operations
- AI service integrations

## Why Integration Tests are Needed

### Database Adapters

The database adapter layer interfaces with actual database drivers:

- **DuckDB Adapter** (`src/connections/adapters/duckdb.ts`)
  - Uses `duckdb` npm package
  - Requires actual DuckDB instance or file
  - Tests connection, queries, and schema introspection

- **PostgreSQL Adapter** (`src/connections/adapters/postgres.ts`)
  - Uses `pg` npm package
  - Requires PostgreSQL server
  - Tests connection pooling, type mapping, transactions

- **MySQL Adapter** (`src/connections/adapters/mysql.ts`)
  - Uses `mysql2` npm package
  - Requires MySQL server
  - Tests connection handling, result conversion

### Schema Store

The schema persistence layer requires file system access:

- **SchemaStore** (`src/schema/schemaStore.ts`)
  - Saves/loads JSON schema files
  - Manages description and relationship metadata
  - Requires actual VSCode workspace

## Current Test Status

| Component | Unit Tests | Integration Tests | Status |
|-----------|------------|-------------------|--------|
| Core Utilities | ✅ 82 tests | N/A | Complete |
| SQL Parsing | ✅ 58 tests | N/A | Complete |
| Error Handling | ✅ 23 tests | N/A | Complete |
| AI Services | ✅ 8 tests | ⏳ Planned | Unit tests complete |
| DB Adapters | ⏳ Scaffolded | ❌ Not implemented | Needs test databases |
| Schema Store | ⏳ Scaffolded | ❌ Not implemented | Needs file system |
| **TOTAL** | **171 passing** | **0** | **Good foundation** |

## Recommended Integration Test Setup

### 1. Docker-based Test Databases

Create `docker-compose.test.yml`:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: test_db
      POSTGRES_USER: test_user
      POSTGRES_PASSWORD: test_pass
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U test_user"]
      interval: 5s
      timeout: 5s
      retries: 5

  mysql:
    image: mysql:8
    environment:
      MYSQL_DATABASE: test_db
      MYSQL_USER: test_user
      MYSQL_PASSWORD: test_pass
      MYSQL_ROOT_PASSWORD: root_pass
    ports:
      - "3306:3306"
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      timeout: 5s
      retries: 5
```

### 2. Test Database Fixtures

Create sample data for testing:

```sql
-- fixtures/postgres/schema.sql
CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id),
  amount DECIMAL(10, 2),
  status VARCHAR(50)
);

-- Insert test data
INSERT INTO users (name, email) VALUES
  ('Alice', 'alice@example.com'),
  ('Bob', 'bob@example.com');
```

### 3. Integration Test Structure

```typescript
// src/connections/adapters/__integration__/postgres.integration.test.ts
import { PostgresAdapter } from '../postgres';

describe('PostgreSQL Adapter Integration', () => {
  let adapter: PostgresAdapter;
  let testProfile: ConnectionProfile;

  beforeAll(async () => {
    // Wait for database to be ready
    await waitForDatabase();

    // Run fixtures
    await runSQLFixtures('postgres/schema.sql');
  });

  afterAll(async () => {
    // Cleanup
    await cleanupDatabase();
  });

  it('should connect to real PostgreSQL database', async () => {
    await expect(
      adapter.testConnection(testProfile, secrets)
    ).resolves.not.toThrow();
  });

  it('should query actual data', async () => {
    const result = await adapter.runQuery(
      testProfile,
      secrets,
      'SELECT * FROM users',
      { maxRows: 10 }
    );

    expect(result.rows).toHaveLength(2);
    expect(result.rows[0].name).toBe('Alice');
  });
});
```

### 4. Run Integration Tests Separately

Add to `package.json`:

```json
{
  "scripts": {
    "test": "jest",
    "test:unit": "jest --testPathIgnorePatterns=__integration__",
    "test:integration": "jest --testMatch='**/__integration__/**/*.test.ts'",
    "test:integration:setup": "docker-compose -f docker-compose.test.yml up -d",
    "test:integration:teardown": "docker-compose -f docker-compose.test.yml down",
    "test:all": "npm run test:unit && npm run test:integration"
  }
}
```

### 5. CI/CD Integration

Update `.github/workflows/test.yml`:

```yaml
jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run unit tests
        run: npm run test:unit

  integration-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_DB: test_db
          POSTGRES_USER: test_user
          POSTGRES_PASSWORD: test_pass
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
      mysql:
        image: mysql:8
        env:
          MYSQL_DATABASE: test_db
          MYSQL_USER: test_user
          MYSQL_PASSWORD: test_pass
          MYSQL_ROOT_PASSWORD: root_pass
    steps:
      - uses: actions/checkout@v4
      - name: Run integration tests
        run: npm run test:integration
```

## Test Coverage Goals

### Short Term (Current Release)
- ✅ Core utilities: 95%+ coverage
- ✅ Unit tests: 171 passing
- ✅ Fast test suite (< 1s)
- ✅ CI/CD for unit tests

### Medium Term (Next Release)
- ⏳ Database adapter integration tests
- ⏳ Schema store integration tests
- ⏳ AI service integration tests
- ⏳ Docker-based test environment

### Long Term
- ⏳ End-to-end tests
- ⏳ Performance benchmarks
- ⏳ Load testing
- ⏳ Cross-platform compatibility tests

## Writing Integration Tests

### Best Practices

1. **Isolate tests**: Each test should set up and tear down its own data
2. **Use transactions**: Rollback after each test to maintain clean state
3. **Test realistic scenarios**: Use actual queries and data patterns
4. **Mock external services**: API calls, AI services should be mocked
5. **Document requirements**: Specify database versions, configurations

### Example Test Suite

```typescript
describe('DuckDB Adapter Integration', () => {
  let tempDbPath: string;

  beforeEach(() => {
    // Create temporary database for each test
    tempDbPath = `/tmp/test-${Date.now()}.duckdb`;
  });

  afterEach(() => {
    // Clean up
    fs.unlinkSync(tempDbPath);
  });

  it('should handle large result sets', async () => {
    // Create table with 100k rows
    await adapter.runQuery(profile, secrets, `
      CREATE TABLE large_table AS
      SELECT i as id, 'row_' || i as name
      FROM range(100000) t(i)
    `);

    const result = await adapter.runQuery(
      profile,
      secrets,
      'SELECT * FROM large_table',
      { maxRows: 1000 }
    );

    expect(result.rows).toHaveLength(1000);
  });
});
```

## Contributing

When adding new database adapters or schema features:

1. Write unit tests for business logic
2. Write integration tests for database interactions
3. Document database version requirements
4. Add fixtures for test data
5. Update this guide with new test scenarios

## Resources

- [Jest Integration Testing](https://jestjs.io/docs/testing-frameworks)
- [Testing with Docker](https://docs.docker.com/ci-cd/best-practices/)
- [VSCode Extension Testing](https://code.visualstudio.com/api/working-with-extensions/testing-extension)
- [Database Testing Best Practices](https://martinfowler.com/articles/practical-test-pyramid.html)

---

**Current Status**: Unit tests complete (171 tests), integration tests planned for next release.
