# Contributing to Kevo TypeScript SDK

Thank you for your interest in contributing to the Kevo TypeScript SDK!

## Development Setup

1. Clone the repository
2. Install dependencies: `npm install`
3. Build the project: `npm run build`
4. Run tests: `npm run test`

## Dependency Management Notes

When running `npm install`, you may notice several deprecation warnings for nested dependencies:

```
npm warn deprecated inflight@1.0.6: This module is not supported, and leaks memory...
npm warn deprecated rimraf@3.0.2: Rimraf versions prior to v4 are no longer supported
npm warn deprecated glob@7.2.3: Glob versions prior to v9 are no longer supported
npm warn deprecated @humanwhocodes/object-schema@2.0.3: Use @eslint/object-schema instead...
npm warn deprecated @humanwhocodes/config-array@0.13.0: Use @eslint/config-array instead
npm warn deprecated eslint@8.57.1: This version is no longer supported...
```

These warnings are related to transitive dependencies (dependencies of our dependencies) and don't directly affect the functionality of this SDK. They are primarily in development dependencies used for testing and linting.

When appropriate, we'll update our direct dependencies to newer versions as they become available, which may resolve these warnings over time.

## Code Style

This project uses ESLint and TypeScript's type checking to enforce code quality:

- Run `npm run lint` to check for style issues
- Run `npm run typecheck` to check for type errors

Please ensure your code passes these checks before submitting a pull request.

## Testing

- All new features should include tests
- Run tests with `npm run test`
- Maintain test coverage for existing functionality

## Building

- The project is built using `tsup`
- Run `npm run build` to create the distribution files
- Distribution files are generated in the `dist` directory

## Releasing

1. Update the version in `package.json`
2. Run `npm run build` to ensure the distribution files are up-to-date
3. Commit your changes
4. Create a tag for the version: `git tag v0.1.0`
5. Push the changes and the tag: `git push && git push --tags`
6. Publish to npm: `npm publish`