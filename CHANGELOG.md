# valkeyrie

## 0.8.4

### Patch Changes

- 8a7a48b: Add driver constructor support
- 9a8ee9a: fix(deps): bump minimatch from 9.0.5 to 9.0.7 in the npm_and_yarn group across 1 directory

## 0.8.3

### Patch Changes

- 85a9cfc: fix: cursor pagination with deep keys and non-string types

## 0.8.2

### Patch Changes

- a0abc46: Add documentation website
- a22d008: fix: list() prefix type inference now returns union of all matching schemas

## 0.8.1

### Patch Changes

- 99ba459: Remove c8 in favor of native coverage
- 8559588: Build only ESM and drop tsx
- f92c806: chore: remove benchmarks

## 0.8.0

### Minor Changes

- ef32a4b: feat: add `from` and `fromAsync` factory functions for database population

  This adds two new static factory methods to create and populate Valkeyrie databases from existing data sources:

  - **`Valkeyrie.from(iterable, options)`** - Create and populate a database from synchronous iterables (arrays, Sets, Maps, custom iterables)
  - **`Valkeyrie.fromAsync(asyncIterable, options)`** - Create and populate a database from async iterables (async generators, streams, async iterators)

  **Key Features:**

  - Flexible key extraction via property names or custom functions
  - Automatic batching (1000 items per atomic operation) for optimal performance
  - Progress tracking with optional callbacks
  - Configurable error handling (stop or continue on errors)
  - Support for all database options (TTL, custom serializers, file paths, etc.)
  - Memory efficient processing for large datasets

  **Example:**

  ```typescript
  const users = [
    { id: 1, name: "Alice" },
    { id: 2, name: "Bob" },
  ];

  const db = await Valkeyrie.from(users, {
    prefix: ["users"],
    keyProperty: "id",
    onProgress: (processed, total) => console.log(`${processed}/${total}`),
  });
  ```

  This is especially useful for data migrations, imports, seeding databases, and creating databases from API responses or streams.

### Patch Changes

- 1893805: Complete documentation overhaul with new structure and missing feature documentation

  **New Documentation Structure:**

  - Split monolithic docs into focused guides and API reference
  - Created beginner-friendly getting started guide
  - Added comprehensive guides for schema validation, factory methods, serializers, and advanced patterns
  - Complete API reference with all methods and types

  **Previously Missing Documentation:**

  - **Watch API** - Complete documentation for real-time key monitoring (added in v0.5.0)
  - **Type Inference** - Automatic TypeScript type inference from schemas
  - **Multi-instance Concurrency** - Database-level versionstamp generation improvements (v0.7.2)
  - **Symbol.asyncDispose** - Automatic resource management support

  **New Guides:**

  - `docs/guides/getting-started.md` - Complete beginner tutorial
  - `docs/guides/schema-validation.md` - Type-safe operations with Zod, Valibot, ArkType
  - `docs/guides/factory-methods.md` - Create databases from arrays and streams
  - `docs/guides/serializers.md` - Choose and configure serializers
  - `docs/guides/advanced-patterns.md` - Watch API, atomic operations, real-world patterns

  **API Reference:**

  - `docs/api/api-reference.md` - Complete method reference
  - `docs/api/types.md` - TypeScript types and interfaces

  **Improvements:**

  - Clear navigation with `docs/README.md` index
  - Real-world examples throughout
  - Decision trees for choosing options
  - Migration guides from Deno.kv
  - Troubleshooting sections

  The old `docs/documentation.md` has been archived as `docs/documentation.md.old`.

- 065368c: Implement database-level versionstamp generation for multi-instance concurrency and fix watch stream cancel bug

  **Multi-Instance Concurrency (fixes #64):**

  - Implement database-level sequence table for atomic versionstamp generation across multiple instances
  - Replace process-local versionstamp generation with SQLite sequence-based approach
  - Add proper transaction nesting support with `inTransaction` state tracking
  - Implement retry logic for versionstamp generation with exponential backoff
  - Use `BEGIN IMMEDIATE TRANSACTION` for exclusive database locks to ensure cross-process atomicity
  - Maintain 20-character versionstamp format (timestamp + sequence) for API compatibility

  **Bug Fix:**

  - Fix watch stream `cancel()` callback to correctly use closure-scoped controller
  - Remove incorrect `controller.close()` call in cancel handler (stream infrastructure handles this)

  **Test Coverage:**

  - Add test for concurrent versionstamp generation with multiple driver instances
  - Add test for watch controller close errors with unexpected error types
  - Add test for rollback failures in transaction retry logic
  - Update watch cancellation test to properly test the cancel path

  This prevents race conditions and lost updates when multiple Valkeyrie instances share the same SQLite database file.

## 0.7.1

### Patch Changes

- e53697e: adds more tests

## 0.7.0

### Minor Changes

- 7f3312d: Add destroy and clear methods

## 0.6.0

### Minor Changes

- d4f4d5d: remove size limits for values

## 0.5.1

### Patch Changes

- 686914a: named import for bson functions

## 0.5.0

### Minor Changes

- e5699d6: Implements watch method and fixes broken transactions with multiple clients

## 0.4.1

### Patch Changes

- 9041029: feat: add asyncDispose and dispose symbols

## 0.4.0

### Minor Changes

- 1214c1e: feat: add cbor-x and msgpackr serialzers

## 0.3.0

### Minor Changes

- 44c725f: feat: refactor serializers and default driver

## 0.2.2

### Patch Changes

- c2f03d6: chore: add json serializer
- efc2b66: chore: add tests for bson serializer
- f8d6235: feat: add bson serializer

## 0.2.1

### Patch Changes

- 2f50ede: Add more deno kv tests

## 0.2.0

### Minor Changes

- 7a52ee0: adds CJS compilation target

## 0.1.0

### Minor Changes

- 5f552f9: Add benchmarks

## 0.0.3

### Patch Changes

- abb607c: Fix sigstore provenance on release

## 0.0.2

### Patch Changes

- d95d315: General tweak and cleanup of code
