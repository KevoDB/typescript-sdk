module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/tests/**/*.test.ts'],
  collectCoverage: true,
  coverageDirectory: 'coverage',
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/proto/**',
    '!**/node_modules/**',
  ],
  coverageReporters: ['text', 'lcov'],
  testTimeout: 10000,
};