# Getting Started with Valkeyrie

Welcome to Valkeyrie! This guide will walk you through everything you need to know to start using Valkeyrie in your Node.js applications.

## What is Valkeyrie?

Valkeyrie is a **type-safe** key-value database that provides **runtime schema validation** and **automatic TypeScript type inference** through the Standard Schema specification, working seamlessly with Zod, Valibot, ArkType, and other validation libraries.

Valkeyrie's API is inspired by [Deno.kv](https://deno.com/kv).

## Installation

```bash
# Using pnpm (recommended)
pnpm add valkeyrie

# Using npm
npm install valkeyrie

```

Valkeyrie requires Node.js 22 or higher.

## Your First Database

Let's create your first Valkeyrie database:

```typescript
import { Valkeyrie } from 'valkeyrie';

// Open an in-memory database
const db = await Valkeyrie.open();

// Store some data
await db.set(['message'], 'Hello, Valkeyrie!');

// Retrieve the data
const entry = await db.get(['message']);
console.log(entry.value); // 'Hello, Valkeyrie!'

// Close the database when done
await db.close();
```

## In-Memory vs File-Based

Out of the box, Valkeyrie supports two modes:

### In-Memory Database

Perfect for testing, caching, or temporary data:

```typescript
// No path = in-memory
const db = await Valkeyrie.open();

// Data is lost when the database is closed
await db.close();
```

### Custom Driver

Advanced users can supply a fully custom storage backend by passing a driver function to `Valkeyrie.open()` (or via the `driverFn` option in factory methods) instead of a file path. The `Driver` type and `defineDriver` helper for implementing a backend are exported from `'valkeyrie/driver'`. See the [API Reference](../api/api-reference.md#valkeyrieopen) for details. For the built-in SQLite backend, just pass a path (or nothing) to `Valkeyrie.open()`.

### File-Based Database

For persistent data that survives restarts:

```typescript
// Specify a file path
const db = await Valkeyrie.open('./my-app.db');

// Data is saved to disk
await db.set(['users', 'alice'], { name: 'Alice' });

// Close and reopen - data is still there
await db.close();

const db2 = await Valkeyrie.open('./my-app.db');
const alice = await db2.get(['users', 'alice']);
console.log(alice.value); // { name: 'Alice' }
```

## Understanding Keys

In Valkeyrie, keys are arrays, not strings. This creates a natural hierarchy:

```typescript
// Think of keys like file paths
await db.set(['users', 'alice'], { name: 'Alice' });
await db.set(['users', 'bob'], { name: 'Bob' });
await db.set(['posts', 'post-1'], { title: 'My First Post' });
await db.set(['posts', 'post-2'], { title: 'Another Post' });
```

This hierarchy makes it easy to organize and query your data:

```typescript
// Get all users (everything under ['users'])
for await (const entry of db.list({ prefix: ['users'] })) {
  console.log(entry.value);
}
// { name: 'Alice' }
// { name: 'Bob' }
```

### Key Part Types

Key parts can be:
- **String**: `['users', 'alice']`
- **Number**: `['products', 42]`
- **Boolean**: `['active', true]`
- **BigInt**: `['id', 123456789n]`
- **Uint8Array**: `['hash', new Uint8Array([1, 2, 3])]`

```typescript
// Mixing types is fine
await db.set(['users', 'alice', 'posts', 1], { title: 'First Post' });
await db.set(['products', 42, 'in-stock', true], { quantity: 10 });
```

## Working with Values

Valkeyrie can store almost any JavaScript value:

```typescript
// Primitives
await db.set(['string'], 'hello');
await db.set(['number'], 42);
await db.set(['boolean'], true);
await db.set(['null'], null);
await db.set(['undefined'], undefined);
await db.set(['bigint'], 123456789n);

// Complex types
await db.set(['object'], {
  name: 'Alice',
  age: 30,
  tags: ['admin', 'user']
});

await db.set(['array'], [1, 2, 3, 4, 5]);

await db.set(['date'], new Date());

await db.set(['map'], new Map([
  ['key1', 'value1'],
  ['key2', 'value2']
]));

await db.set(['set'], new Set([1, 2, 3]));

// Binary data
await db.set(['buffer'], new Uint8Array([72, 101, 108, 108, 111]));
await db.set(['arraybuffer'], new ArrayBuffer(8));
```

### What You Get Back

When you retrieve a value, you get an entry object:

```typescript
const entry = await db.get(['users', 'alice']);

console.log(entry.key);          // ['users', 'alice']
console.log(entry.value);        // { name: 'Alice' }
console.log(entry.versionstamp); // '00000000000000000001'
```

The `versionstamp` is a unique identifier that changes every time the value is updated. It's useful for [atomic operations](#atomic-operations).

### When a Key Doesn't Exist

If you try to get a key that doesn't exist, you get a special entry:

```typescript
const entry = await db.get(['does-not-exist']);

console.log(entry.value);        // null
console.log(entry.versionstamp); // null
console.log(entry.key);          // ['does-not-exist']
```

## Basic Operations

### Setting Values

```typescript
// Simple set
await db.set(['key'], 'value');

// Returns a result with the versionstamp
const result = await db.set(['key'], 'value');
console.log(result); // { ok: true, versionstamp: '00000000000000000001' }
```

### Getting Values

```typescript
// Get a single value
const entry = await db.get(['key']);

// Get multiple values at once
const entries = await db.getMany([
  ['users', 'alice'],
  ['users', 'bob'],
  ['users', 'charlie']
]);

// entries is an array of Entry objects
for (const entry of entries) {
  console.log(entry.key, entry.value);
}
```

### Deleting Values

```typescript
// Delete a single key
await db.delete(['users', 'alice']);

// The key no longer exists
const entry = await db.get(['users', 'alice']);
console.log(entry.value); // null
```

### Listing Values

List all values with a common prefix:

```typescript
// List all users
for await (const entry of db.list({ prefix: ['users'] })) {
  console.log(entry.key, entry.value);
}

// With pagination (limit results)
const users = db.list({ prefix: ['users'] }, { limit: 10 });
for await (const entry of users) {
  console.log(entry.value);
}

// Get the cursor for the next page
const cursor = users.cursor;

// Load next page
const nextPage = db.list({ prefix: ['users'] }, { limit: 10, cursor });
```

### Range Queries

List values within a specific range:

```typescript
// List users from 'alice' to 'charlie'
for await (const entry of db.list({
  prefix: ['users'],
  start: ['alice'],
  end: ['charlie']
})) {
  console.log(entry.value);
}

// List in reverse order
for await (const entry of db.list(
  { prefix: ['users'] },
  { reverse: true }
)) {
  console.log(entry.value);
}
```

### Converting to Array

If you want all results as an array:

```typescript
const users = await Array.fromAsync(
  db.list({ prefix: ['users'] })
);

console.log(users); // Array of Entry objects
```

## Data Expiration

Set a time-to-live (TTL) for values:

```typescript
// Expires in 60 seconds
await db.set(['session', 'token'], 'abc123', {
  expireIn: 60_000 // milliseconds
});

// After 60 seconds, the value is automatically deleted
setTimeout(async () => {
  const entry = await db.get(['session', 'token']);
  console.log(entry.value); // null
}, 61_000);
```

Expired values are cleaned up automatically when accessed, or you can manually trigger cleanup:

```typescript
await db.cleanup();
```

## Database Management

```typescript
// Clear all data (keeps the database file)
await db.clear();

// Destroy the database (deletes the file)
await db.destroy();

// Close the database
await db.close();
```

### Automatic Cleanup with `await using`

Valkeyrie supports the explicit resource management proposal (on Node.js v24):

```typescript
{
  await using db = await Valkeyrie.open('./temp.db', { destroyOnClose: true });

  await db.set(['key'], 'value');

  // Database is automatically closed and destroyed when the block exits
}
```

## Error Handling

Valkeyrie operations can throw errors:

```typescript
try {
  await db.set(['key'], someValue);
} catch (error) {
  if (error instanceof TypeError) {
    console.error('Invalid key or value type');
  } else if (error instanceof Error && error.message.includes('Database is closed')) {
    console.error('Database was closed');
  } else {
    console.error('Unexpected error:', error);
  }
}
```

Common errors:
- **TypeError** - Invalid key or value type
- **"Database is closed"** - Attempted operation on a closed database
- **ValidationError** - Schema validation failed (if using schema validation)

## Best Practices

### 1. Design Your Keys Carefully

Think about how you'll query your data:

```typescript
// Good: Easy to query all posts by a user
await db.set(['users', 'alice', 'posts', 'post-1'], post);
await db.set(['users', 'alice', 'posts', 'post-2'], post);

// Query: all posts by alice
db.list({ prefix: ['users', 'alice', 'posts'] });

// Bad: Hard to query
await db.set(['post-1', 'author'], 'alice');
await db.set(['post-2', 'author'], 'alice');
// Can't easily get all posts by alice
```

### 2. Use Consistent Key Structures

```typescript
// Good: Consistent structure
await db.set(['users', userId, 'profile'], profile);
await db.set(['users', userId, 'settings'], settings);

// Bad: Inconsistent structure
await db.set(['user', userId], profile);
await db.set([userId, 'settings'], settings);
```

### 3. Close Your Databases

Always close databases when done:

```typescript
const db = await Valkeyrie.open('./app.db');

try {
  // Do work
  await db.set(['key'], 'value');
} finally {
  await db.close();
}
```

Or use explicit resource management (on Node.js v24):

```typescript
await using db = await Valkeyrie.open('./app.db');
// Automatically closed
```

### 4. Use TTL for Temporary Data

```typescript
// Session tokens
await db.set(['sessions', sessionId], token, {
  expireIn: 24 * 60 * 60 * 1000 // 24 hours
});

// Cache
await db.set(['cache', cacheKey], data, {
  expireIn: 5 * 60 * 1000 // 5 minutes
});
```

## Next Steps

Now that you understand the basics, explore more advanced features:

- **[Schema Validation](./schema-validation.md)** - Add type safety with Zod, Valibot, or ArkType
- **[Factory Methods](./factory-methods.md)** - Create databases from existing data
- **[Advanced Patterns](./advanced-patterns.md)** - Atomic operations, watch API, and more
- **[Serializers](./serializers.md)** - Customize how data is stored

## Common Patterns

### User Management

```typescript
// Create a user
await db.set(['users', 'alice'], {
  name: 'Alice',
  email: 'alice@example.com',
  createdAt: new Date()
});

// Get a user
const user = await db.get(['users', 'alice']);

// List all users
for await (const entry of db.list({ prefix: ['users'] })) {
  console.log(entry.value);
}

// Delete a user
await db.delete(['users', 'alice']);
```

### Caching

```typescript
async function getCachedData(key: string) {
  // Try to get from cache
  const cached = await db.get(['cache', key]);
  if (cached.value !== null) {
    return cached.value;
  }

  // Fetch fresh data
  const data = await fetchExpensiveData();

  // Store in cache with 5 minute TTL
  await db.set(['cache', key], data, {
    expireIn: 5 * 60 * 1000
  });

  return data;
}
```

### Counters

```typescript
import { KvU64 } from 'valkeyrie/KvU64';

// Initialize a counter
await db.set(['counters', 'visitors'], new KvU64(0n));

// Increment atomically (see Advanced Patterns guide)
await db.atomic()
  .sum(['counters', 'visitors'], 1n)
  .commit();

// Get the counter
const counter = await db.get(['counters', 'visitors']);
console.log(counter.value.value); // bigint value
```

### Sessions

```typescript
// Create a session
const sessionId = crypto.randomUUID();
await db.set(['sessions', sessionId], {
  userId: 'alice',
  createdAt: Date.now()
}, {
  expireIn: 24 * 60 * 60 * 1000 // 24 hours
});

// Check session
const session = await db.get(['sessions', sessionId]);
if (session.value === null) {
  console.log('Session expired or invalid');
} else {
  console.log('Session valid:', session.value);
}

// Delete session (logout)
await db.delete(['sessions', sessionId]);
```

## Troubleshooting

### "Database is closed" Error

This happens when you try to use a database after closing it:

```typescript
const db = await Valkeyrie.open();
await db.close();

await db.set(['key'], 'value'); // Error: Database is closed
```

**Solution**: Don't use the database after closing it, or reopen it.

### Keys Must Be Arrays

```typescript
// Wrong
await db.set('key', 'value'); // Error

// Correct
await db.set(['key'], 'value');
```

### Large Values

While there's no hard size limit, extremely large values (> 10MB) may impact performance. Consider:
- Splitting large objects into smaller pieces
- Storing large files on disk and keeping only references in Valkeyrie
- Using compression for large text data

## Summary
- ✅ How to open and close databases
- ✅ Understanding hierarchical keys
- ✅ Setting, getting, and deleting values
- ✅ Listing and querying data
- ✅ Using data expiration
- ✅ Basic error handling
- ✅ Common patterns and best practices
