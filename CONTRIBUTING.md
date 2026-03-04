# Contributing to DuckPiper

Thank you for your interest in contributing to DuckPiper! This document provides guidelines and instructions for contributing.

## Getting Started

### Prerequisites

- Node.js 20.x or higher
- npm (comes with Node.js)
- Git
- Visual Studio Code (recommended)

### Development Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/duckpiper/duckpiper.git
   cd duckpiper
   ```

2. **Install dependencies**

   ```bash
   npm install
   ```

3. **Build the extension**

   ```bash
   npm run compile
   ```

4. **Run tests**

   ```bash
   npm test
   ```

### Development Workflow

1. **Start watch mode** for automatic compilation

   ```bash
   npm run watch
   ```

2. **Run the extension** in a new VSCode window
   - Press `F5` in VSCode to launch the extension in debug mode
   - This opens a new VSCode window with your extension loaded

3. **Run tests** while developing

   ```bash
   npm run test:watch
   ```

## Code Quality

### Testing

We use [Jest](https://jestjs.io/) for unit testing. All new features and bug fixes should include tests.

**Running tests:**

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage

# Run tests for CI
npm run test:ci
```

**Writing tests:**

- Place tests in `__tests__` directories next to the code they test
- Use descriptive test names that explain what is being tested
- Test both happy paths and error cases
- Mock external dependencies (VSCode APIs, file system, network)

See [TESTING.md](./TESTING.md) for detailed testing guidelines.

### Linting

We use ESLint to maintain code quality:

```bash
npm run lint
```

### Code Style

- Use TypeScript for all new code
- Follow the existing code style
- Use meaningful variable and function names
- Add comments for complex logic
- Keep functions focused and small

## Pull Request Process

1. **Create a branch** for your feature or bugfix

   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bugfix-name
   ```

2. **Make your changes**
   - Write clear, concise commit messages
   - Include tests for new functionality
   - Update documentation as needed

3. **Run tests and linting**

   ```bash
   npm run lint
   npm test
   ```

4. **Push your branch**

   ```bash
   git push origin feature/your-feature-name
   ```

5. **Create a Pull Request**
   - Provide a clear description of the changes
   - Reference any related issues
   - Ensure all CI checks pass

### Pull Request Guidelines

- **One feature per PR**: Keep PRs focused on a single feature or fix
- **Write clear descriptions**: Explain what changes were made and why
- **Link to issues**: Reference related issues using `#issue-number`
- **Update documentation**: Include documentation updates if needed
- **Add tests**: All code changes should have corresponding tests
- **Keep it small**: Smaller PRs are easier to review and merge

## Project Structure

```
duckpiper/
├── src/                    # Source code
│   ├── __tests__/         # Test utilities and mocks
│   │   └── __mocks__/     # VSCode and other mocks
│   ├── ai/                # AI service integration
│   ├── connections/       # Database connection adapters
│   ├── core/              # Core utilities
│   ├── erd/               # ERD generation
│   ├── pipelines/         # Pipeline and lineage tracking
│   ├── queryLibrary/      # Saved queries
│   ├── results/           # Query results display
│   ├── schema/            # Schema introspection
│   ├── transform/         # Data transformation
│   ├── ui/                # UI components
│   └── extension.ts       # Extension entry point
├── media/                 # Icons and assets
├── .github/              # GitHub Actions workflows
└── docs/                 # Documentation
```

## Architecture

### Core Concepts

1. **Adapters**: Database-specific connection handlers (DuckDB, PostgreSQL, MySQL, Snowflake)
2. **Schema Store**: Caches database schema information
3. **Query Library**: Manages saved queries and templates
4. **Pipelines**: Tracks data lineage and transformations
5. **ERD Generator**: Creates entity-relationship diagrams
6. **AI Integration**: Provides AI-powered features

### Key Patterns

- **Command Pattern**: VSCode commands are registered in dedicated modules
- **Provider Pattern**: Tree views, webviews, and completion use VSCode providers
- **Repository Pattern**: Data access is abstracted through repositories
- **Error Handling**: Centralized error handling with user-friendly messages

## Testing Strategy

### Unit Tests

- Test individual functions and classes in isolation
- Mock external dependencies (VSCode APIs, databases, file system)
- Focus on edge cases and error handling
- Located in `__tests__` directories

### Integration Tests

- Test interactions between components
- Use test databases or mocks
- Verify end-to-end workflows

### Coverage Goals

- Core utilities: > 90%
- Business logic: > 80%
- Overall: > 70%

## Documentation

### Code Documentation

- Add JSDoc comments to public APIs
- Explain complex algorithms
- Document assumptions and constraints
- Include examples for non-obvious usage

### User Documentation

- Update README.md for user-facing changes
- Add feature documentation in `docs/`
- Include screenshots for UI changes
- Write clear, concise instructions

## Reporting Issues

When reporting issues, please include:

1. **Environment details**
   - VSCode version
   - Extension version
   - Operating system
   - Database type and version (if applicable)

2. **Steps to reproduce**
   - Clear, numbered steps
   - Sample SQL or data (if relevant)
   - Expected vs. actual behavior

3. **Error messages**
   - Full error messages
   - Stack traces
   - Console logs

4. **Screenshots/Videos**
   - Visual issues benefit from screenshots
   - Complex workflows benefit from screen recordings

## Feature Requests

We welcome feature requests! When suggesting new features:

1. **Check existing issues** to avoid duplicates
2. **Describe the use case** - why is this feature needed?
3. **Propose a solution** - how would it work?
4. **Consider alternatives** - are there other ways to solve this?
5. **Discuss impact** - how does this affect existing features?

## Code of Conduct

### Our Standards

- **Be respectful**: Treat everyone with respect
- **Be constructive**: Provide helpful feedback
- **Be collaborative**: Work together to improve the project
- **Be inclusive**: Welcome diverse perspectives

### Unacceptable Behavior

- Harassment or discrimination
- Trolling or insulting comments
- Personal attacks
- Publishing others' private information

## License

By contributing to DuckPiper, you agree that your contributions will be licensed under the same license as the project.

## Questions?

- **General questions**: Open a GitHub Discussion
- **Bug reports**: Open a GitHub Issue
- **Feature requests**: Open a GitHub Issue with the "enhancement" label
- **Security issues**: Follow the private reporting process in [SECURITY.md](./SECURITY.md) (do not create public issues)

## Resources

- [VSCode Extension API](https://code.visualstudio.com/api)
- [TypeScript Documentation](https://www.typescriptlang.org/docs/)
- [Jest Documentation](https://jestjs.io/docs/getting-started)
- [DuckDB Documentation](https://duckdb.org/docs/)

Thank you for contributing to DuckPiper! 🦆
