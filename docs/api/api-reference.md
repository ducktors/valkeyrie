# API Reference

Complete reference for all Valkeyrie methods and classes.

## Table of Contents

- [Valkeyrie Class](#valkeyrie-class)
- [AtomicOperation Class](#atomicoperation-class)
- [KvU64 Class](#kvu64-class)
- [Errors](#errors)

## Valkeyrie Class

Main database class for all operations.

### Static Methods

#### `Valkeyrie.withSchema()`

Register a schema for validation and type inference.

```typescript
static withSchema<TPattern extends Key, TSchema extends StandardSchemaV1>(
  pattern: TPattern,
  schema: TSchema
): ValkeyrieBuilder
```

**Parameters:**
- `pattern` - Key pattern with wildcards (e.g., `['users', '*']`)
- `schema` - Standard Schema-compatible validator (Zod, Valibot, ArkType)

**Returns:** `ValkeyrieBuilder` for chaining

**Example:**
```typescript
import { z } from 'zod';

const userSchema = z.object({
  name: z.string(),
  email: z.string().email()
});

const db = await Valkeyrie
  .withSchema(['users', '*'], userSchema)
  .open('./data.db');
```

---

#### `Valkeyrie.open()`

Open or create a database. The first argument is either a file path for the
built-in SQLite backend, or a driver factory function for a custom backend.

```typescript
// Built-in SQLite backend (in-memory when path is omitted)
static async open(
  path?: string,
  options?: {
    serializer?: () => Serializer;
    destroyOnClose?: boolean;
  }
): Promise<Valkeyrie>

// Custom backend
static async open(
  driverFn: DriverFactory, // (serializer?: () => Serializer) => Promise<Driver>
  options?: {
    serializer?: () => Serializer;
    destroyOnClose?: boolean;
  }
): Promise<Valkeyrie>
```

**Parameters:**
- `path` - Optional file path. Omit for in-memory database
- `driverFn` - A `DriverFactory`: a function that receives the resolved serializer factory and returns a `Promise<Driver>`. Implement the `Driver` interface (import the `Driver` type and `defineDriver` helper from `'valkeyrie/driver'`) for your own backend.
- `options` - Configuration options
  - `serializer` - Custom serializer (default: V8 serializer). Passed through to `driverFn` when a custom backend is used.
  - `destroyOnClose` - Delete the underlying storage on close (default: `false`)

**Returns:** `Promise<Valkeyrie>`

**Example:**
```typescript
// In-memory
const db1 = await Valkeyrie.open();

// File-based
const db2 = await Valkeyrie.open('./data.db');

// With options
const db3 = await Valkeyrie.open('./temp.db', {
  serializer: jsonSerializer,
  destroyOnClose: true
});

// Custom backend — `createMyDriver` returns an object implementing the Driver
// interface (the `Driver` type and `defineDriver` helper live in 'valkeyrie/driver')
const db4 = await Valkeyrie.open(
  async (serializer) => createMyDriver(serializer),
);
```

---

#### `Valkeyrie.from()`

Create and populate a database from a synchronous iterable.

```typescript
static async from<T>(
  iterable: Iterable<T>,
  options: FromOptions<T>
): Promise<Valkeyrie>
```

**Parameters:**
- `iterable` - Array, Set, Map, or any iterable
- `options` - Configuration options. Supplying `driverFn` uses a custom driver and takes precedence over `path`.

**FromOptions:**
```typescript
interface FromOptions<T> {
  prefix: Key;                                    // Required
  keyProperty: keyof T | ((item: T) => KeyPart);  // Required
  path?: string;                                  // defaults to in-memory if neither path nor driverFn are given
  driverFn?: (serializer?: () => Serializer) => Promise<Driver>; // takes precedence over path
  serializer?: () => Serializer;
  destroyOnClose?: boolean;
  expireIn?: number;
  onProgress?: (processed: number, total?: number) => void;
  onError?: 'stop' | 'continue';
  onErrorCallback?: (error: Error, item: T) => void;
}
```

**Returns:** `Promise<Valkeyrie>`

**Example:**
```typescript
const users = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' }
];

const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id',
  path: './users.db'
});
```

---

#### `Valkeyrie.fromAsync()`

Create and populate a database from an asynchronous iterable.

```typescript
static async fromAsync<T>(
  iterable: AsyncIterable<T>,
  options: FromOptions<T>
): Promise<Valkeyrie>
```

**Parameters:** Same as `from()`. Supplying `driverFn` in options uses a custom driver and takes precedence over `path`.

**Returns:** `Promise<Valkeyrie>`

**Example:**
```typescript
async function* fetchUsers() {
  for (let page = 1; page <= 10; page++) {
    const response = await fetch(`/api/users?page=${page}`);
    const users = await response.json();
    for (const user of users) yield user;
  }
}

const db = await Valkeyrie.fromAsync(fetchUsers(), {
  prefix: ['users'],
  keyProperty: 'id'
});
```

---

### Instance Methods

#### `get()`

Retrieve a single value by key.

```typescript
async get<T>(key: Key): Promise<EntryMaybe<T>>
```

**Parameters:**
- `key` - Array of key parts

**Returns:**
```typescript
type EntryMaybe<T> =
  | { key: Key; value: T; versionstamp: string }
  | { key: Key; value: null; versionstamp: null }
```

**Example:**
```typescript
const entry = await db.get(['users', 'alice']);

if (entry.value !== null) {
  console.log(entry.value);      // User data
  console.log(entry.versionstamp); // Version identifier
}
```

---

#### `getMany()`

Retrieve multiple values at once.

```typescript
async getMany<T>(keys: Key[]): Promise<EntryMaybe<T>[]>
```

**Parameters:**
- `keys` - Array of keys

**Returns:** Array of `EntryMaybe<T>`

**Example:**
```typescript
const entries = await db.getMany([
  ['users', 'alice'],
  ['users', 'bob'],
  ['users', 'charlie']
]);

for (const entry of entries) {
  if (entry.value) {
    console.log(entry.value);
  }
}
```

---

#### `set()`

Store a value with the given key.

```typescript
async set<T>(
  key: Key,
  value: T,
  options?: {
    expireIn?: number;
  }
): Promise<{ ok: true; versionstamp: string }>
```

**Parameters:**
- `key` - Array of key parts
- `value` - Value to store
- `options` - Optional configuration
  - `expireIn` - Time-to-live in milliseconds

**Returns:** Object with `ok: true` and `versionstamp`

**Example:**
```typescript
await db.set(['users', 'alice'], {
  name: 'Alice',
  email: 'alice@example.com'
});

// With expiration
await db.set(['session', 'token'], 'abc123', {
  expireIn: 3600000 // 1 hour
});
```

---

#### `delete()`

Delete a value by key.

```typescript
async delete(key: Key): Promise<void>
```

**Parameters:**
- `key` - Array of key parts

**Returns:** `Promise<void>`

**Example:**
```typescript
await db.delete(['users', 'alice']);
```

---

#### `list()`

List entries matching a selector.

```typescript
list<T>(
  selector: ListSelector,
  options?: ListOptions
): AsyncIterableIterator<Entry<T>> & { cursor: string }
```

**ListSelector:**
```typescript
type ListSelector =
  | { prefix: Key }
  | { prefix: Key; start: Key }
  | { prefix: Key; end: Key }
  | { start: Key; end: Key }
```

**ListOptions:**
```typescript
interface ListOptions {
  limit?: number;
  cursor?: string;
  reverse?: boolean;
}
```

**Returns:** Async iterator with `cursor` property

**Example:**
```typescript
// List with prefix
for await (const entry of db.list({ prefix: ['users'] })) {
  console.log(entry.key, entry.value);
}

// With pagination
const page1 = db.list({ prefix: ['users'] }, { limit: 10 });
for await (const entry of page1) {
  console.log(entry.value);
}

const cursor = page1.cursor;
const page2 = db.list({ prefix: ['users'] }, { limit: 10, cursor });

// Range query
for await (const entry of db.list({
  prefix: ['users'],
  start: ['alice'],
  end: ['charlie']
})) {
  console.log(entry.value);
}

// Reverse order
for await (const entry of db.list(
  { prefix: ['users'] },
  { reverse: true }
)) {
  console.log(entry.value);
}
```

---

#### `watch()`

Watch keys for changes in real-time.

```typescript
watch<T>(keys: Key[]): ReadableStream<EntryMaybe<T>[]>
```

**Parameters:**
- `keys` - Array of keys to watch

**Returns:** `ReadableStream` that emits entry arrays

**Example:**
```typescript
const stream = db.watch([
  ['users', 'alice'],
  ['users', 'bob']
]);

for await (const [aliceEntry, bobEntry] of stream) {
  console.log('Alice:', aliceEntry.value);
  console.log('Bob:', bobEntry.value);
}

// With manual control
const reader = stream.getReader();
try {
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    console.log('Changes:', value);
  }
} finally {
  await reader.cancel();
}
```

---

#### `atomic()`

Create an atomic operation.

```typescript
atomic(): AtomicOperation
```

**Returns:** `AtomicOperation` instance

**Example:**
```typescript
const result = await db.atomic()
  .set(['users', 'alice'], { name: 'Alice' })
  .set(['users', 'bob'], { name: 'Bob' })
  .delete(['users', 'charlie'])
  .commit();

if (result.ok) {
  console.log('All operations succeeded');
}
```

---

#### `clear()`

Remove all data from the database.

```typescript
async clear(): Promise<void>
```

**Example:**
```typescript
await db.clear(); // Database is now empty
```

---

#### `destroy()`

Destroy the database (deletes the file).

```typescript
async destroy(): Promise<void>
```

**Example:**
```typescript
await db.destroy(); // Database file is deleted
```

---

#### `cleanup()`

Manually remove expired entries.

```typescript
async cleanup(): Promise<void>
```

**Example:**
```typescript
await db.cleanup(); // Remove all expired entries
```

---

#### `close()`

Close the database connection.

```typescript
async close(): Promise<void>
```

**Example:**
```typescript
await db.close();

// With explicit resource management
await using db = await Valkeyrie.open('./data.db');
// Automatically closed when scope exits
```

---

## AtomicOperation Class

Represents an atomic transaction.

### Methods

#### `check()`

Add a version check.

```typescript
check(...checks: AtomicCheck[]): this
```

**AtomicCheck:**
```typescript
interface AtomicCheck {
  key: Key;
  versionstamp: string | null;
}
```

**Example:**
```typescript
const entry = await db.get(['counter']);

await db.atomic()
  .check({ key: ['counter'], versionstamp: entry.versionstamp })
  .set(['counter'], entry.value + 1)
  .commit();
```

---

#### `set()`

Add a set operation.

```typescript
set<T>(key: Key, value: T, options?: { expireIn?: number }): this
```

**Example:**
```typescript
await db.atomic()
  .set(['key1'], 'value1')
  .set(['key2'], 'value2', { expireIn: 60000 })
  .commit();
```

---

#### `delete()`

Add a delete operation.

```typescript
delete(key: Key): this
```

**Example:**
```typescript
await db.atomic()
  .delete(['key1'])
  .delete(['key2'])
  .commit();
```

---

#### `sum()`

Add a numeric sum operation.

```typescript
sum(key: Key, value: bigint | KvU64): this
```

**Example:**
```typescript
await db.atomic()
  .sum(['counter'], 1n)
  .sum(['another'], new KvU64(5n))
  .commit();
```

---

#### `max()`

Set to maximum value.

```typescript
max(key: Key, value: bigint | KvU64): this
```

**Example:**
```typescript
await db.atomic()
  .max(['high-score'], 1000n)
  .commit();
```

---

#### `min()`

Set to minimum value.

```typescript
min(key: Key, value: bigint | KvU64): this
```

**Example:**
```typescript
await db.atomic()
  .min(['low-price'], 50n)
  .commit();
```

---

#### `commit()`

Execute the atomic operation.

```typescript
async commit(): Promise<
  | { ok: true; versionstamp: string }
  | { ok: false }
>
```

**Returns:**
- `{ ok: true, versionstamp }` if successful
- `{ ok: false }` if checks failed

**Example:**
```typescript
const result = await db.atomic()
  .set(['key'], 'value')
  .commit();

if (result.ok) {
  console.log('Success:', result.versionstamp);
} else {
  console.log('Failed: version conflict');
}
```

---

## KvU64 Class

64-bit unsigned integer for atomic numeric operations.

### Constructor

```typescript
constructor(value: bigint)
```

**Example:**
```typescript
import { KvU64 } from 'valkeyrie/KvU64';

const counter = new KvU64(1000n);
await db.set(['counter'], counter);
```

---

### Properties

#### `value`

Get the bigint value.

```typescript
readonly value: bigint
```

**Example:**
```typescript
const entry = await db.get(['counter']);
console.log(entry.value.value); // bigint
```

---

### Methods

#### `valueOf()`

Convert to bigint (for numeric operations).

```typescript
valueOf(): bigint
```

---

#### `toString()`

Convert to string.

```typescript
toString(): string
```

---

#### `toJSON()`

Convert to JSON representation.

```typescript
toJSON(): string
```

---

## Errors

### ValidationError

Thrown when schema validation fails.

```typescript
class ValidationError extends Error {
  key: Key;
  issues: Array<{
    message: string;
    path: (string | number)[];
  }>;
}
```

**Example:**
```typescript
import { ValidationError } from 'valkeyrie';

try {
  await db.set(['users', 'alice'], invalidData);
} catch (error) {
  if (error instanceof ValidationError) {
    console.log('Failed for key:', error.key);
    console.log('Issues:', error.issues);
  }
}
```

---

## Type Definitions

See [Types Reference](./types.md) for complete TypeScript type definitions.

---

## Summary

This API reference covers:

- ✅ All static and instance methods
- ✅ Complete parameter and return types
- ✅ Practical examples for each method
- ✅ Atomic operation API
- ✅ KvU64 numeric type
- ✅ Error types

For more details:
- [Getting Started](../guides/getting-started.md) - Learn the basics
- [Types Reference](./types.md) - TypeScript type definitions
- [Advanced Patterns](../guides/advanced-patterns.md) - Real-world usage
