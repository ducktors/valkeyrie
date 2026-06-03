# TypeScript Types Reference

Complete reference for Valkeyrie's TypeScript types and interfaces.

## Core Types

### Key and KeyPart

```typescript
type KeyPart = Uint8Array | string | number | bigint | boolean | symbol;
type Key = readonly KeyPart[];
```

Keys are arrays of parts. Each part can be one of the allowed types.

**Examples:**
```typescript
const key1: Key = ['users', 'alice'];
const key2: Key = ['products', 42, true];
const key3: Key = ['hash', new Uint8Array([1, 2, 3])];
```

---

### Entry and EntryMaybe

```typescript
interface Entry<T = unknown> {
  key: Key;
  value: T;
  versionstamp: string;
}

type EntryMaybe<T = unknown> =
  | Entry<T>
  | {
      key: Key;
      value: null;
      versionstamp: null;
    }
```

`Entry` represents an existing database entry.
`EntryMaybe` can represent either an existing entry or a missing key.

**Examples:**
```typescript
// Existing entry
const entry: Entry<User> = {
  key: ['users', 'alice'],
  value: { name: 'Alice' },
  versionstamp: '00000000000000000001'
};

// Missing entry
const missing: EntryMaybe<User> = {
  key: ['users', 'bob'],
  value: null,
  versionstamp: null
};
```

---

### Value

```typescript
type Value = unknown;
```

Values can be any serializable JavaScript type. The exact supported types depend on your chosen serializer.

---

## Operation Types

### ListSelector

```typescript
type ListSelector =
  | { prefix: Key }
  | { prefix: Key; start: Key }
  | { prefix: Key; end: Key }
  | { start: Key; end: Key }
```

Defines which entries to list.

**Examples:**
```typescript
// List all with prefix
const selector1: ListSelector = { prefix: ['users'] };

// List range within prefix
const selector2: ListSelector = {
  prefix: ['users'],
  start: ['alice'],
  end: ['charlie']
};

// List range without prefix
const selector3: ListSelector = {
  start: ['a'],
  end: ['z']
};
```

---

### ListOptions

```typescript
interface ListOptions {
  limit?: number;
  cursor?: string;
  reverse?: boolean;
}
```

Options for list operations.

**Example:**
```typescript
const options: ListOptions = {
  limit: 10,
  reverse: true
};

const entries = db.list({ prefix: ['users'] }, options);
```

---

### SetOptions

```typescript
interface SetOptions {
  expireIn?: number;  // milliseconds
}
```

Options for set operations.

**Example:**
```typescript
await db.set(['session', 'token'], 'abc123', {
  expireIn: 3600000  // 1 hour
});
```

---

## Atomic Operation Types

### Check

```typescript
interface Check {
  key: Key;
  versionstamp: string | null;
}
```

Version check for optimistic concurrency control.

**Example:**
```typescript
const entry = await db.get(['key']);

const check: Check = {
  key: ['key'],
  versionstamp: entry.versionstamp
};
```

---

### Mutation

```typescript
type Mutation<T = unknown> = { key: Key } & (
  | { type: 'set'; value: T; expireIn?: number }
  | { type: 'delete' }
  | { type: 'sum'; value: KvU64 }
  | { type: 'max'; value: KvU64 }
  | { type: 'min'; value: KvU64 }
);
```

Represents a mutation in an atomic operation.

**Examples:**
```typescript
const set: Mutation = {
  key: ['key'],
  type: 'set',
  value: 'value',
  expireIn: 60000
};

const del: Mutation = {
  key: ['key'],
  type: 'delete'
};

const sum: Mutation = {
  key: ['counter'],
  type: 'sum',
  value: new KvU64(1n)
};
```

---

### AtomicCheck

```typescript
interface AtomicCheck {
  key: Key;
  versionstamp: string | null;
}
```

Same as `Check` but used in the atomic operation builder.

---

## Factory Method Types

### FromOptions

```typescript
interface FromOptions<T> {
  prefix: Key;
  keyProperty: keyof T | ((item: T) => KeyPart);
  path?: string;                                                   // defaults to in-memory if neither path nor driverFn are given
  driverFn?: (serializer?: () => Serializer) => Promise<Driver>;  // takes precedence over path
  serializer?: () => Serializer;
  destroyOnClose?: boolean;
  expireIn?: number;
  onProgress?: (processed: number, total?: number) => void;
  onError?: 'stop' | 'continue';
  onErrorCallback?: (error: Error, item: T) => void;
}
```

Options for `from()` and `fromAsync()` factory methods.

**Example:**
```typescript
const options: FromOptions<User> = {
  prefix: ['users'],
  keyProperty: 'id',
  path: './users.db',
  expireIn: 86400000,
  onProgress: (processed, total) => {
    console.log(`${processed}/${total}`);
  },
  onError: 'continue',
  onErrorCallback: (error, item) => {
    console.error('Failed:', item, error);
  }
};
```

---

## Serializer Types

### Serializer

```typescript
interface Serializer {
  serialize: (value: unknown) => Uint8Array;
  deserialize: (value: Uint8Array) => unknown;
}
```

Interface for custom serializers.

**Example:**
```typescript
const customSerializer: Serializer = {
  serialize: (value) => {
    return Buffer.from(JSON.stringify(value), 'utf8');
  },
  deserialize: (data) => {
    return JSON.parse(Buffer.from(data).toString('utf8'));
  }
};
```

---

## Schema Validation Types

### StandardSchemaV1

```typescript
// From @standard-schema/spec package
interface StandardSchemaV1 {
  readonly '~standard': {
    readonly version: 1;
    readonly vendor: string;
    readonly validate: (value: unknown) => StandardResult<unknown>;
  };
}
```

Standard Schema specification interface. Implemented by Zod, Valibot, ArkType, etc.

---

## Type Inference

When using schemas, Valkeyrie automatically infers types:

```typescript
import { z } from 'zod';

const userSchema = z.object({
  name: z.string(),
  email: z.string().email(),
  age: z.number()
});

const db = await Valkeyrie
  .withSchema(['users', '*'], userSchema)
  .open();

// Type is automatically inferred!
const user = await db.get(['users', 'alice']);
// user.value: { name: string; email: string; age: number } | null

// Also works for list
for await (const entry of db.list({ prefix: ['users'] })) {
  // entry.value: { name: string; email: string; age: number }
  console.log(entry.value.email); // ✅ Type-safe
}

// And for watch
const stream = db.watch([['users', 'alice']]);
for await (const [entry] of stream) {
  // entry: EntryMaybe<{ name: string; email: string; age: number }>
}
```

---

## Error Types

### ValidationError

```typescript
class ValidationError extends Error {
  name: 'ValidationError';
  key: Key;
  issues: Array<{
    message: string;
    path: (string | number)[];
  }>;
}
```

Thrown when schema validation fails.

**Example:**
```typescript
try {
  await db.set(['users', 'alice'], invalidData);
} catch (error) {
  if (error instanceof ValidationError) {
    error.key;     // ['users', 'alice']
    error.issues;  // Array of validation errors
  }
}
```

---

## Utility Types

### InferTypeForKey

```typescript
type InferTypeForKey<TRegistry, TKey> = /* internal type */
```

Internal type used for inferring value types based on schema registry and key.

---

### InferTypeForPrefix

```typescript
type InferTypeForPrefix<TRegistry, TPrefix> = /* internal type */
```

Internal type used for inferring value types based on schema registry and prefix.

---

## Type Guards

### Checking Entry Existence

```typescript
const entry = await db.get(['key']);

if (entry.value !== null) {
  // TypeScript knows entry.value is not null
  // and entry.versionstamp is string
  console.log(entry.value);
  console.log(entry.versionstamp);
}
```

---

### Checking Atomic Result

```typescript
const result = await db.atomic()
  .set(['key'], 'value')
  .commit();

if (result.ok) {
  // TypeScript knows result has versionstamp
  console.log(result.versionstamp);
} else {
  // result.ok is false - checks failed
}
```

---

## Generic Type Parameters

### Valkeyrie&lt;TRegistry&gt;

```typescript
class Valkeyrie<TRegistry extends SchemaRegistryType = readonly []>
```

The `TRegistry` type parameter tracks registered schemas for type inference.

**Example:**
```typescript
// Without schemas
const db1: Valkeyrie = await Valkeyrie.open();

// With schemas (type is inferred automatically)
const db2 = await Valkeyrie
  .withSchema(['users', '*'], userSchema)
  .open();
// db2's type includes schema information
```

---

## Best Practices

### 1. Use Schema Validation for Auto-Inference

```typescript
// ✅ Best: Automatic type inference from schemas
const db = await Valkeyrie
  .withSchema(['users', '*'], userSchema)
  .open();

// Type is automatically inferred - no annotations needed!
const user = await db.get(['users', 'alice']);
// user.value: { name: string; email: string; age: number } | null
```

### 2. Type Your Data (Without Schemas)

```typescript
// If not using schemas, provide explicit types
interface User {
  name: string;
  email: string;
  age: number;
}

const entry = await db.get<User>(['users', 'alice']);
// entry.value is User | null
```

### 3. Const Assertions (Advanced)

```typescript
// For advanced use cases where you need literal key types
// Note: Not needed for schema type inference
const key = ['users', 'alice'] as const;
// key type: readonly ['users', 'alice']

const key2 = ['users', 'alice'];
// key2 type: string[]
```

### 4. Handle Null Values

```typescript
const entry = await db.get(['key']);

// ✅ Good: Check for null
if (entry.value !== null) {
  console.log(entry.value);
}

// ✅ Good: Use optional chaining
console.log(entry.value?.someProperty);

// ❌ Bad: Assuming value exists
console.log(entry.value.someProperty); // Type error
```

---

## Summary

This type reference covers:

- ✅ Core types (Key, Entry, Value)
- ✅ Operation types (ListSelector, SetOptions, etc.)
- ✅ Atomic operation types
- ✅ Factory method types
- ✅ Serializer interface
- ✅ Schema validation types
- ✅ Type inference with schemas
- ✅ Error types
- ✅ Best practices for type safety

For more information:
- [API Reference](./api-reference.md) - Method signatures and examples
- [Schema Validation Guide](../guides/schema-validation.md) - Type inference in action
