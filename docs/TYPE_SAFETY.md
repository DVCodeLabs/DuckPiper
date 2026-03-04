# Type Safety & Code Quality

This document outlines the TypeScript type safety status and improvement strategy for DuckPiper.

## Current Status

### TypeScript Configuration

✅ **Strict mode enabled** in `tsconfig.json`:
```json
{
  "compilerOptions": {
    "strict": true,
    "esModuleInterop": true
  }
}
```

This enables all strict type-checking options:
- `strictNullChecks`: Null and undefined are distinct types
- `strictFunctionTypes`: Function parameter types are checked contravariantly
- `strictBindCallApply`: Type checking for bind, call, and apply methods
- `strictPropertyInitialization`: Class properties must be initialized
- `noImplicitThis`: Raise error on 'this' expressions with implied 'any'
- `alwaysStrict`: Parse in strict mode and emit "use strict"

### ESLint Configuration

The project uses TypeScript ESLint with the following rules:

```javascript
'@typescript-eslint/no-explicit-any': 'warn',      // 217 instances to address
'@typescript-eslint/no-unused-vars': 'warn',       // ~60 instances to address
'@typescript-eslint/no-require-imports': 'off'     // Required for some Node.js modules
```

### Current Metrics

| Metric | Count | Status |
|--------|-------|--------|
| TypeScript errors | 0 | ✅ All fixed |
| Explicit `any` types | 217 | ⚠️ Warnings enabled |
| Unused variables | ~60 | ⚠️ Warnings enabled |
| Total lint warnings | 327 | ⚠️ Non-blocking |

## Gradual Improvement Strategy

### Phase 1: Foundation (✅ Complete)

- [x] Enable TypeScript strict mode
- [x] Fix all TypeScript compilation errors
- [x] Enable ESLint warnings for `any` and unused vars
- [x] Document type safety strategy

### Phase 2: High-Impact Types (Recommended Next)

Focus on types that provide the most value for reliability:

#### 1. Public API Surfaces

Replace `any` in exported functions and interfaces:

```typescript
// ❌ Before
export function processData(data: any): any {
  return data.transform();
}

// ✅ After
export function processData(data: QueryResult): ProcessedResult {
  return data.transform();
}
```

**Files to prioritize:**
- `src/core/types.ts` - Core type definitions
- `src/connections/adapters/*.ts` - Database adapter interfaces
- `src/ai/providers/*.ts` - AI provider interfaces

#### 2. Error-Prone Areas

Add types where bugs are most likely:

```typescript
// ❌ Error-prone
function handleResponse(response: any) {
  if (response.data) { /* ... */ }
}

// ✅ Type-safe
interface ApiResponse {
  data?: QueryResult;
  error?: string;
}

function handleResponse(response: ApiResponse) {
  if (response.data) { /* ... */ }
}
```

**Focus areas:**
- Database query results
- AI API responses
- Configuration objects
- Event handlers

#### 3. Unused Variables

Address unused variables systematically:

```typescript
// Option 1: Remove if truly unused
function connect(host: string, port: number) {
  // port is unused - remove it
  return connectTo(host);
}

// Option 2: Prefix with _ if intentionally unused
function process(_context: Context, data: Data) {
  // context kept for interface compatibility
  return transform(data);
}

// Option 3: Use the variable
function validate(config: Config) {
  // Actually use all parameters
  return config.host && config.port;
}
```

### Phase 3: Comprehensive Types (Future)

Once high-impact areas are typed, expand coverage:

1. **Internal functions** - Add types to private/internal functions
2. **Complex data structures** - Create interfaces for nested objects
3. **Generic types** - Use generics for reusable functions
4. **Utility types** - Leverage TypeScript utility types (Partial, Pick, etc.)

## Type Safety Patterns

### 1. Database Results

```typescript
// Define specific result shapes
interface QueryResult {
  rows: Record<string, unknown>[];
  columns: ColumnInfo[];
  rowCount: number;
  elapsedMs: number;
}

interface ColumnInfo {
  name: string;
  type: string;
}
```

### 2. AI Provider Responses

```typescript
// Use discriminated unions for different response types
type AIResponse =
  | { type: 'success'; text: string; }
  | { type: 'error'; message: string; }
  | { type: 'stream'; stream: AsyncIterable<string>; };

function handleAIResponse(response: AIResponse) {
  switch (response.type) {
    case 'success':
      return response.text;
    case 'error':
      throw new Error(response.message);
    case 'stream':
      return consumeStream(response.stream);
  }
}
```

### 3. VSCode Extension APIs

```typescript
// Type VSCode API parameters properly
function handleCommand(item?: TreeItem): void {
  if (!item) {
    vscode.window.showWarningMessage('No item selected');
    return;
  }
  // item is now guaranteed to exist
  processItem(item);
}
```

### 4. Configuration Objects

```typescript
// Use Pick and Partial for flexible config
interface FullConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
}

type RequiredConfig = Pick<FullConfig, 'host' | 'database'>;
type OptionalConfig = Partial<Omit<FullConfig, keyof RequiredConfig>>;
type Config = RequiredConfig & OptionalConfig;
```

## Migration Guidelines

### When to Use `any`

Sometimes `any` is appropriate (but document why):

```typescript
// ✅ Acceptable: Truly dynamic external data
function parseJsonResponse(text: string): any {
  // Response structure varies by API endpoint
  return JSON.parse(text);
}

// ✅ Acceptable: Third-party library without types
function callLegacyLib(params: any): any {
  // @ts-expect-error: legacy-lib has no type definitions
  return legacyLib.process(params);
}

// ❌ Avoid: Laziness or uncertainty
function getData(): any {
  // Should define proper return type
  return { data: [], count: 0 };
}
```

### Progressive Enhancement

Don't let perfect be the enemy of good:

```typescript
// Step 1: Basic typing
function process(data: unknown): unknown {
  return transform(data);
}

// Step 2: Input typing
function process(data: InputData): unknown {
  return transform(data);
}

// Step 3: Full typing
function process(data: InputData): OutputData {
  return transform(data);
}
```

## Testing Type Safety

### 1. Enable stricter checks in tsconfig

For new code, consider:
```json
{
  "compilerOptions": {
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true
  }
}
```

### 2. Use type assertions carefully

```typescript
// ❌ Unsafe
const value = unknownData as string;

// ✅ Safer
function isString(value: unknown): value is string {
  return typeof value === 'string';
}

if (isString(unknownData)) {
  // TypeScript knows this is a string
  const value = unknownData;
}
```

### 3. Test edge cases

```typescript
// Test with actual types you expect
it('processes query results correctly', () => {
  const result: QueryResult = {
    rows: [{ id: 1, name: 'test' }],
    columns: [{ name: 'id', type: 'integer' }],
    rowCount: 1,
    elapsedMs: 45
  };

  expect(processResults(result)).toBeDefined();
});
```

## Common Patterns to Replace

### Pattern 1: `any` in catch blocks

```typescript
// ❌ Before
try {
  await operation();
} catch (e: any) {
  console.log(e.message);
}

// ✅ After
try {
  await operation();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  Logger.error('Operation failed', { message });
}
```

### Pattern 2: `any` for event handlers

```typescript
// ❌ Before
function handleEvent(event: any) {
  if (event.data) { /* ... */ }
}

// ✅ After
interface CustomEvent {
  data?: unknown;
  timestamp: number;
}

function handleEvent(event: CustomEvent) {
  if (event.data) { /* ... */ }
}
```

### Pattern 3: `any[]` arrays

```typescript
// ❌ Before
const items: any[] = getItems();

// ✅ After
interface Item {
  id: string;
  name: string;
}

const items: Item[] = getItems();
```

## Measuring Progress

Track type safety improvements over time:

```bash
# Count any types
grep -r ": any" src --include="*.ts" | wc -l

# Check for TypeScript errors
npx tsc --noEmit

# Run linter
npm run lint
```

## Resources

- [TypeScript Handbook - Strict Mode](https://www.typescriptlang.org/docs/handbook/2/basic-types.html#strictness)
- [TypeScript Deep Dive](https://basarat.gitbook.io/typescript/)
- [Effective TypeScript](https://effectivetypescript.com/)

## Next Steps

1. **Start with high-impact files** - Focus on adapters and core types
2. **Add types incrementally** - Don't try to fix everything at once
3. **Document decisions** - Use comments for non-obvious type choices
4. **Test thoroughly** - Ensure type changes don't break functionality
5. **Review regularly** - Check metrics monthly to track progress

Target: Reduce `any` usage by 50% (to ~110 instances) in next 3 months.
