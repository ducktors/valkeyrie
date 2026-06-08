# Valkeyrie - Key-Value Store

<p align="center">
 <img align="center" alt="Valkeyrie" height="200" src="https://github.com/user-attachments/assets/87c60a17-0f17-42aa-9db8-993dddb08e31">
</p>

---

[![GitHub package.json version](https://img.shields.io/github/package-json/v/ducktors/Valkeyrie)](https://github.com/ducktors/Valkeyrie/releases) ![node:24](https://img.shields.io/badge/node-24-lightgreen) ![pnpm@10.20.0](https://img.shields.io/badge/pnpm-10.20.0-yellow) [![npm](https://img.shields.io/npm/dt/valkeyrie)](https://www.npmjs.com/package/valkeyrie) [![CI](https://github.com/ducktors/Valkeyrie/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ducktors/Valkeyrie/actions/workflows/ci.yml) [![Test](https://github.com/ducktors/Valkeyrie/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/ducktors/Valkeyrie/actions/workflows/test.yaml) [![Coverage Status](https://coveralls.io/repos/github/ducktors/Valkeyrie/badge.svg)](https://coveralls.io/github/ducktors/Valkeyrie) [![Maintainability](https://api.codeclimate.com/v1/badges/c1a77d6d8b158d442572/maintainability)](https://codeclimate.com/github/ducktors/valkeyrie/maintainability) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/ducktors/Valkeyrie/badge)](https://scorecard.dev/viewer/?uri=github.com/ducktors/valkeyrie) [![OpenSSF Best Practices](https://www.bestpractices.dev/projects/10163/badge)](https://www.bestpractices.dev/projects/10163)

Valkeyrie is a type-safe, key-value store for Node.js that combines runtime schema validation with pluggable storage drivers. Built with Standard Schema support, it provides automatic TypeScript type inference and first-class runtime schema validation.

This is still a work in progress, but the API and everything already implemented is stable and ready for production.

📚 **[Documentation](https://ducktors.github.io/Valkeyrie/)** | [Getting Started](https://ducktors.github.io/Valkeyrie/guides/getting-started) | [API Reference](https://ducktors.github.io/Valkeyrie/api/api-reference)

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [Hierarchical Keys](#hierarchical-keys)
  - [Value Types](#value-types)
  - [Basic Operations](#basic-operations)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Type-safe with schema validation** - Runtime validation with Zod, Valibot, ArkType, and other Standard Schema libraries
- **Automatic type inference** - Full TypeScript support with schema-based type inference across all operations
- **Atomic operations** - Perform multiple operations in a single transaction with optimistic locking
- **Real-time updates** - Watch keys for changes with the `watch()` API
- **Pluggable storage drivers** - Currently SQLite-based, with support for more drivers coming soon. Custom drivers are reachable today by passing a driver function to the public `open()` API and via the `driverFn` option in factory methods.
- **Rich data type support** - Store objects, arrays, dates, binary data, Maps, Sets, and more
- **Hierarchical keys** - Organize data with multi-part keys for efficient querying
- **Efficient querying** - List data with prefix and range queries
- **Data expiration** - Set time-to-live for values with automatic cleanup
- **Multi-instance safe** - Proper concurrency control for multiple process access
- **Simple and intuitive API** - Inspired by Deno.kv for a familiar, easy-to-learn interface

## Installation

```bash
# Using pnpm
pnpm add valkeyrie

# Using npm
npm install valkeyrie
```

## Quick Start

```typescript
import { Valkeyrie } from 'valkeyrie';

// Open a database (in-memory or file-based)
const db = await Valkeyrie.open('./my-database.db');

// Store and retrieve data
await db.set(['users', 'alice'], {
  name: 'Alice',
  email: 'alice@example.com',
  age: 30
});

const user = await db.get(['users', 'alice']);
console.log(user.value);
// { name: 'Alice', email: 'alice@example.com', age: 30 }

// List all users
for await (const entry of db.list({ prefix: ['users'] })) {
  console.log(entry.key, entry.value);
}

// Close when done
await db.close();
```

For more examples, see the sections below or check out the [complete documentation](./docs/guides/getting-started.md).

## Core Concepts

### Opening a Database

```typescript
// In-memory database (lost when closed)
const db = await Valkeyrie.open();

// File-based database (persisted to disk)
const db = await Valkeyrie.open('./my-app.db');

// With automatic cleanup on close
const db = await Valkeyrie.open('./temp.db', { destroyOnClose: true });
```

### Hierarchical Keys

Keys in Valkeyrie are arrays that create a hierarchy, similar to file paths:

```typescript
// Organize data hierarchically
await db.set(['users', 'alice', 'profile'], { name: 'Alice', bio: '...' });
await db.set(['users', 'alice', 'settings'], { theme: 'dark' });
await db.set(['users', 'bob', 'profile'], { name: 'Bob', bio: '...' });

// List all of Alice's data
for await (const entry of db.list({ prefix: ['users', 'alice'] })) {
  console.log(entry.key); // ['users', 'alice', 'profile'], ['users', 'alice', 'settings']
}

// Key parts can be strings, numbers, booleans, bigints, or Uint8Array
await db.set(['products', 42, 'name'], 'Laptop');
await db.set(['active', true, 'users'], ['alice', 'bob']);
```

### Value Types

Store any JavaScript data type:

```typescript
// Primitives
await db.set(['string'], 'hello');
await db.set(['number'], 42);
await db.set(['boolean'], true);
await db.set(['bigint'], 123456789n);
await db.set(['null'], null);

// Complex types
await db.set(['object'], { name: 'Alice', age: 30 });
await db.set(['array'], [1, 2, 3]);
await db.set(['date'], new Date());
await db.set(['map'], new Map([['a', 1], ['b', 2]]));
await db.set(['set'], new Set([1, 2, 3]));
await db.set(['buffer'], new Uint8Array([1, 2, 3]));
```

### Basic Operations

```typescript
// Set a value
await db.set(['key'], 'value');

// Set with expiration (60 seconds)
await db.set(['session', 'token'], 'abc123', { expireIn: 60_000 });

// Get a value
const entry = await db.get(['key']);
console.log(entry.value); // 'value'
console.log(entry.versionstamp); // Version identifier

// Get multiple values at once
const entries = await db.getMany([
  ['users', 'alice'],
  ['users', 'bob']
]);

// Delete a value
await db.delete(['key']);

// List with prefix
for await (const entry of db.list({ prefix: ['users'] })) {
  console.log(entry.key, entry.value);
}

// Clear all data
await db.clear();

// Destroy database (deletes file)
await db.destroy();
```

## Documentation

### Guides

- **[Getting Started](./docs/guides/getting-started.md)** - Complete beginner's guide with examples
- **[Schema Validation](./docs/guides/schema-validation.md)** - Type-safe operations with Zod, Valibot, and ArkType
- **[Factory Methods](./docs/guides/factory-methods.md)** - Create and populate databases from data sources
- **[Serializers](./docs/guides/serializers.md)** - Choose and configure the right serializer
- **[Advanced Patterns](./docs/guides/advanced-patterns.md)** - Atomic operations, watch API, and real-world patterns

### API Reference

- **[API Reference](./docs/api/api-reference.md)** - Complete API documentation
- **[Types](./docs/api/types.md)** - TypeScript types and interfaces

## Contributing

For detailed information on how to contribute to this project, please see our [Contributing Guide](./CONTRIBUTING.md).

## License

Valkeyrie is licensed under the MIT License. See the [License](./LICENSE) file for details. 
