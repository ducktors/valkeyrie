# Valkeyrie - Key-Value Store Documentation
## Table of Contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
  - [Opening a Database](#opening-a-database)
  - [Closing a Database](#closing-a-database)
  - [Setting Values](#setting-values)
  - [Getting Values](#getting-values)
  - [Deleting Values](#deleting-values)
  - [Working with Multiple Keys](#working-with-multiple-keys)
- [Key Structure](#key-structure)
- [Supported Value Types](#supported-value-types)
- [Working with 64-bit Unsigned Integers (KvU64)](#working-with-64-bit-unsigned-integers-kvu64)
- [Listing and Querying](#listing-and-querying)
  - [Listing with Prefix](#listing-with-prefix)
  - [Listing with Range](#listing-with-range)
  - [Pagination with Cursors](#pagination-with-cursors)
  - [Reverse Order](#reverse-order)
  - [Using Array.fromAsync](#using-arrayfromasync)
- [Atomic Operations](#atomic-operations)
  - [Basic Atomic Operations](#basic-atomic-operations)
  - [Check and Set](#check-and-set)
  - [Numeric Operations](#numeric-operations)
  - [Complex Atomic Operations](#complex-atomic-operations)
- [Expiration](#expiration)
- [Performance Considerations](#performance-considerations)
- [Error Handling](#error-handling)
- [Advanced Usage](#advanced-usage)
  - [Custom Drivers](#custom-drivers)
  - [Cleanup](#cleanup)
  - [Serializers](#serializers)
- [API Reference](#api-reference)
  - [Valkeyrie Class](#valkeyrie-class)
  - [Atomic Class](#atomic-class)
  - [KvU64 Class](#kvu64-class)
  - [Types](#types)
- [Benchmarks](#benchmarks)
- [License](#license)

## Introduction

Valkeyrie is a high-performance key-value store for Node.js applications. It provides a simple yet powerful API for storing and retrieving data with support for complex data types, atomic operations, and efficient querying. Valkeyrie is built on SQLite, providing durability and reliability while maintaining excellent performance.

Key features:
- Simple and intuitive API
- Support for complex data types (including nested objects, arrays, dates, etc.)
- Atomic operations with optimistic concurrency control
- Efficient prefix-based and range-based queries
- Support for data expiration
- High performance with minimal overhead
- Specialized 64-bit unsigned integer type for numeric operations
- Pluggable serializers for customizing data storage format

## Installation

Install Valkeyrie using npm, yarn, or pnpm:

```bash
# Using pnpm
pnpm add valkeyrie
```

## Basic Usage

### Opening a Database

To start using Valkeyrie, you first need to open a database:

```typescript
import { Valkeyrie } from 'valkeyrie';

// Open an in-memory database
const db = await Valkeyrie.open();

// Or open a persistent database by specifying a file path
const persistentDb = await Valkeyrie.open('/path/to/database.db');
```

### Closing a Database

Always close the database when you're done with it to release resources:

```typescript
await db.close();
```

### Setting Values

Store values using the `set` method:

```typescript
// Simple key-value pair
await db.set(['users', 'user1'], { name: 'John Doe', age: 30 });

// Nested keys
await db.set(['settings', 'theme', 'dark'], true);

// With expiration (value expires in 60 seconds)
await db.set(['session', 'token123'], 'abc123', { expireIn: 60 * 1000 });
```

The `set` method returns an object with the operation status and the versionstamp:

```typescript
const result = await db.set(['key'], 'value');
console.log(result);
// Output: { ok: true, versionstamp: '00000000000000000001' }
```

### Getting Values

Retrieve values using the `get` method:

```typescript
const entry = await db.get(['users', 'user1']);
console.log(entry);
// Output: { 
//   key: ['users', 'user1'], 
//   value: { name: 'John Doe', age: 30 }, 
//   versionstamp: '00000000000000000001' 
// }

// If the key doesn't exist, value and versionstamp will be null
const nonExistent = await db.get(['unknown']);
console.log(nonExistent);
// Output: { key: ['unknown'], value: null, versionstamp: null }
```

### Deleting Values

Remove values using the `delete` method:

```typescript
await db.delete(['users', 'user1']);
```

### Working with Multiple Keys

You can retrieve multiple keys at once using `getMany`:

```typescript
const entries = await db.getMany([
  ['users', 'user1'],
  ['users', 'user2'],
  ['settings', 'theme']
]);

// entries is an array of Entry objects
```

## Key Structure

In Valkeyrie, keys are arrays of key parts. Each key part can be:
- String
- Number
- BigInt
- Boolean
- Uint8Array

Keys are designed to be hierarchical, making it easy to organize and query your data:

```typescript
// User data
await db.set(['users', 'user1', 'profile'], { ... });
await db.set(['users', 'user1', 'settings'], { ... });

// Product data
await db.set(['products', 'prod1', 'details'], { ... });
await db.set(['products', 'prod1', 'inventory'], { ... });
```

### Examples for Each Key Part Type

Here are examples of using each supported key part type:

#### String Keys
Strings are the most common key part type:

```typescript
// Using string key parts
await db.set(['users', 'john_doe', 'profile'], { name: 'John Doe' });
await db.get(['users', 'john_doe', 'profile']);
```

#### Number Keys
Numbers can be used for indexed or ordered data:

```typescript
// Using number key parts
await db.set(['products', 123, 'name'], 'Ergonomic Chair');
await db.set(['leaderboard', 1], 'First Place');
await db.set(['years', 2023, 'revenue'], 1500000);

// Useful for range queries
const products = db.list({
  prefix: ['products'],
  start: [100],
  end: [200]
});
```

#### BigInt Keys
BigInt is useful for very large numbers or identifiers:

```typescript
// Using BigInt key parts
const userId = 9007199254740992n; // Beyond Number.MAX_SAFE_INTEGER
await db.set(['users', userId, 'name'], 'Jane Doe');

// Useful for database IDs or timestamps with microsecond precision
const timestamp = 1672531200000000n; // Microsecond precision timestamp
await db.set(['logs', timestamp], { event: 'System startup' });
```

#### Boolean Keys
Boolean key parts can be used for binary categorization:

```typescript
// Using boolean key parts
await db.set(['users', 'john_doe', 'settings', 'notifications', true], { 
  email: true, 
  push: false 
});
await db.set(['products', 'in-stock', false], ['Product A', 'Product B']);

// Get all out-of-stock products
const outOfStock = db.list({ prefix: ['products', 'in-stock', false] });
```

#### Uint8Array Keys
Uint8Array is useful for binary data like hashes or IDs:

```typescript
// Using Uint8Array key parts
const binaryId = new Uint8Array([1, 2, 3, 4, 5]);
await db.set(['files', binaryId], { filename: 'document.pdf' });

// Useful for cryptographic hashes
const sha256Hash = new Uint8Array([
  0x9f, 0x86, 0xd0, 0x81, 0x88, 0x4c, 0x7d, 0x65,
  0x9a, 0x2f, 0xea, 0xa0, 0xc5, 0x5a, 0xd0, 0x15,
  0xa3, 0xbf, 0x4f, 0x1b, 0x2b, 0x0b, 0x82, 0x2c,
  0xd1, 0x5d, 0x6c, 0x15, 0xb0, 0xf0, 0x0a, 0x08
]); // SHA-256 hash of "test"
await db.set(['hashes', sha256Hash], { verified: true });
```

#### Mixed Key Types
You can mix different key part types in the same key:

```typescript
// Combining different key part types
await db.set(['users', 42, true, 'profile'], { name: 'Mixed Key User' });

// Complex hierarchical structure with mixed types
const documentId = new Uint8Array([10, 20, 30]);
await db.set(
  ['documents', documentId, 'versions', 2, 'published', true],
  { content: 'Updated document content', timestamp: Date.now() }
);
```

When designing your key structure, choose key part types that best represent your data model and access patterns. The hierarchical nature of keys allows for efficient organization and querying of your data.

## Supported Value Types

Valkeyrie supports different value types depending on which serializer you use. By default, Valkeyrie uses the V8 serializer.

### V8 Serializer (Default)

The V8 serializer supports a wide range of value types:

- Primitive types: string, number, boolean, bigint, null, undefined
- Binary data: Uint8Array, ArrayBuffer
- Complex types: objects, arrays, Map, Set
- Date objects
- RegExp objects
- KvU64 (specialized 64-bit unsigned integer)
- Nested combinations of the above types

Unsupported types (will throw an error):
- Functions
- Symbols
- WeakMap, WeakSet
- Error objects
- DOM nodes

### JSON Serializer

The JSON serializer provides better interoperability with other systems but with some differences in how data is stored:

- Primitive types: string, number, boolean, null
- Complex types: objects, arrays
- Special handling for:
  - undefined (stored as a special object, restored as undefined)
  - BigInt (stored as string representation, restored as BigInt)
  - Uint8Array, ArrayBuffer (base64 encoded, restored to original type)
  - Map (stored as array of key-value pairs, restored as Map)
  - Set (stored as array of values, restored as Set)
  - Date (stored as ISO string, restored as Date)
  - RegExp (stored with source and flags, restored as RegExp)
  - KvU64 (stored as string representation, restored as KvU64)

Unsupported types (will throw an error):
- Functions
- Symbols
- WeakMap, WeakSet
- SharedArrayBuffer
- Error objects
- DOM nodes
- Objects with circular references

To use the JSON serializer:

```typescript
import { Valkeyrie, jsonSerializer } from 'valkeyrie';

const db = await Valkeyrie.open('./data.db', {
  serializer: jsonSerializer
});
```

The JSON serializer is particularly useful when:
- You need to inspect the database contents in a human-readable format
- You need interoperability with other systems or languages
- You want to ensure your data can be easily migrated or exported

For detailed information about serializers, see the [Serializers documentation](./serializers.md).

### Examples for Each Value Type

Here are examples of storing and retrieving different value types, with notes on serializer compatibility:

#### Primitive Types

```typescript
// String
await db.set(['primitives', 'string'], 'Hello, Valkeyrie!');
// ✅ V8 Serializer: Preserved as string
// ✅ JSON Serializer: Preserved as string

// Number
await db.set(['primitives', 'number'], 42);
// ✅ V8 Serializer: Preserved as number
// ✅ JSON Serializer: Preserved as number
await db.set(['primitives', 'float'], 3.14159);
// ✅ V8 Serializer: Preserved as number
// ✅ JSON Serializer: Preserved as number

// Boolean
await db.set(['primitives', 'boolean'], true);
// ✅ V8 Serializer: Preserved as boolean
// ✅ JSON Serializer: Preserved as boolean

// BigInt
await db.set(['primitives', 'bigint'], 9007199254740992n);
// ✅ V8 Serializer: Preserved as bigint
// ✅ JSON Serializer: Stored as string representation, restored as BigInt

// Null
await db.set(['primitives', 'null'], null);
// ✅ V8 Serializer: Preserved as null
// ✅ JSON Serializer: Preserved as null

// Undefined
await db.set(['primitives', 'undefined'], undefined);
// ✅ V8 Serializer: Preserved as undefined
// ✅ JSON Serializer: Stored as special object, restored as undefined
```

#### Binary Data

```typescript
// Uint8Array
const binaryData = new Uint8Array([72, 101, 108, 108, 111]); // "Hello" in ASCII
await db.set(['binary', 'uint8array'], binaryData);
// ✅ V8 Serializer: Preserved as Uint8Array
// ✅ JSON Serializer: Base64 encoded, restored as Uint8Array

// ArrayBuffer
const buffer = new ArrayBuffer(4);
const view = new DataView(buffer);
view.setUint32(0, 0x12345678);
await db.set(['binary', 'arraybuffer'], buffer);
// ✅ V8 Serializer: Preserved as ArrayBuffer
// ✅ JSON Serializer: Base64 encoded, restored as ArrayBuffer
```

#### Complex Types

```typescript
// Object
await db.set(['complex', 'object'], { 
  name: 'John Doe', 
  age: 30, 
  isActive: true 
});
// ✅ V8 Serializer: Preserved as object with all properties
// ✅ JSON Serializer: Preserved as object with all properties

// Array
await db.set(['complex', 'array'], [1, 2, 3, 'four', true]);
// ✅ V8 Serializer: Preserved as array with all elements
// ✅ JSON Serializer: Preserved as array with all elements

// Map
const userMap = new Map();
userMap.set('id', 1);
userMap.set('name', 'Jane Doe');
userMap.set('roles', ['admin', 'user']);
await db.set(['complex', 'map'], userMap);
// ✅ V8 Serializer: Preserved as Map
// ✅ JSON Serializer: Stored as array of key-value pairs, restored as Map

// Set
const uniqueTags = new Set(['javascript', 'typescript', 'database']);
await db.set(['complex', 'set'], uniqueTags);
// ✅ V8 Serializer: Preserved as Set
// ✅ JSON Serializer: Stored as array of values, restored as Set
```

#### Date and RegExp

```typescript
// Date object
await db.set(['dates', 'created'], new Date());
// ✅ V8 Serializer: Preserved as Date object
// ✅ JSON Serializer: Stored as ISO string, restored as Date object
await db.set(['dates', 'specific'], new Date('2023-01-01T00:00:00Z'));
// ✅ V8 Serializer: Preserved as Date object
// ✅ JSON Serializer: Stored as ISO string, restored as Date object

// RegExp object
await db.set(['regex', 'email'], /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/);
// ✅ V8 Serializer: Preserved as RegExp object
// ✅ JSON Serializer: Stored with source and flags, restored as RegExp object
await db.set(['regex', 'phone'], /^\+?[1-9]\d{1,14}$/);
// ✅ V8 Serializer: Preserved as RegExp object
// ✅ JSON Serializer: Stored with source and flags, restored as RegExp object
```

#### KvU64 (64-bit Unsigned Integer)

```typescript
import { KvU64 } from 'valkeyrie/KvU64';

// Create and store a KvU64 value
const counter = new KvU64(1000n);
await db.set(['counters', 'visitors'], counter);
// ✅ V8 Serializer: Preserved as KvU64 instance
// ✅ JSON Serializer: Stored as string representation, restored as KvU64 instance

// Retrieve and use the KvU64 value
const result = await db.get(['counters', 'visitors']);
console.log(result.value); // KvU64 instance with both serializers
console.log(result.value.value); // 1000n with both serializers
```

> **Important Note**: KvU64 instances can only be used as top-level values. They cannot be nested within objects or arrays. This is a deliberate design decision to ensure efficient atomic operations on numeric values. This limitation applies to both serializers.

```typescript
// ✅ CORRECT: KvU64 as a top-level value
await db.set(['counters', 'visitors'], new KvU64(1000n));

// ❌ INCORRECT: KvU64 in a nested property
// This will throw an error
await db.set(['stats'], { 
  visitors: new KvU64(1000n), // Error: KvU64 must be a top-level value
  pageViews: new KvU64(5000n)  // Error: KvU64 must be a top-level value
});

// ✅ CORRECT: Store numeric values in nested properties as regular BigInts
await db.set(['stats'], { 
  visitors: 1000n,
  pageViews: 5000n
});

// ✅ CORRECT: Use separate keys for KvU64 values that need atomic operations
await db.set(['stats', 'visitors'], new KvU64(1000n));
await db.set(['stats', 'pageViews'], new KvU64(5000n));
```

#### Nested Combinations

```typescript
// Complex nested structure with multiple types
await db.set(['users', 'user123'], {
  profile: {
    name: 'Alice Johnson',
    age: 28,
    joined: new Date('2022-03-15'),
    preferences: {
      theme: 'dark',
      notifications: true,
      tags: new Set(['tech', 'programming', 'databases'])
    }
  },
  posts: [
    { 
      id: 1, 
      title: 'Getting Started with Valkeyrie',
      published: true,
      tags: ['database', 'tutorial'],
      stats: {
        views: 1256n, // Using BigInt instead of KvU64
        likes: 42n    // Using BigInt instead of KvU64
      }
    },
    { 
      id: 2, 
      title: 'Advanced Valkeyrie Patterns',
      published: false,
      content: new Uint8Array([/* binary content */])
    }
  ],
  activityMap: new Map([
    ['lastLogin', new Date()],
    ['loginCount', 37n], // Using BigInt instead of KvU64
    ['devices', ['desktop', 'mobile']]
  ])
});
// ✅ V8 Serializer: All types preserved as their original types
// ✅ JSON Serializer: All types preserved, with special handling during storage:
//    - Date objects stored as ISO strings, restored as Date objects
//    - Set stored as array, restored as Set
//    - BigInt values stored as strings, restored as BigInt
//    - Uint8Array base64 encoded, restored as Uint8Array
//    - Map stored as array of [key, value] pairs, restored as Map

// For values that need atomic operations, store them separately as KvU64
await db.set(['users', 'user123', 'posts', '1', 'views'], new KvU64(1256n));
await db.set(['users', 'user123', 'posts', '1', 'likes'], new KvU64(42n));
await db.set(['users', 'user123', 'loginCount'], new KvU64(37n));
// KvU64 values work with both serializers for atomic operations
```

#### Retrieving Complex Values

When retrieving complex values, they maintain their original structure and types with both serializers, though the internal storage format differs:

```typescript
// Using either V8 or JSON Serializer
const user = await db.get(['users', 'user123']);
console.log(user.value.profile.name); // 'Alice Johnson'
console.log(user.value.profile.preferences.tags.has('tech')); // true (Set method works)
console.log(user.value.posts[0].stats.views); // 1256n (regular BigInt)

// For KvU64 values, retrieve them separately (works with both serializers)
const views = await db.get(['users', 'user123', 'posts', '1', 'views']);
console.log(views.value.value); // 1256n (from KvU64)

const loginCount = await db.get(['users', 'user123', 'loginCount']);
console.log(loginCount.value.value); // 37n (from KvU64)
```

The main difference between the serializers is in how the data is stored internally:

- **V8 Serializer**: Uses Node.js's built-in serialization for efficient binary storage
- **JSON Serializer**: Uses a custom JSON-based format with type information for better interoperability

Both serializers preserve the original types when retrieving values, making them largely interchangeable from an API perspective, though with different performance and compatibility characteristics.

## Working with 64-bit Unsigned Integers (KvU64)

Valkeyrie provides a specialized `KvU64` class for working with 64-bit unsigned integers, which is particularly useful for counters and numeric operations:

```typescript
import { KvU64 } from 'valkeyrie/KvU64';

// Create a KvU64 instance
const counter = new KvU64(1000n);

// Store it in the database
await db.set(['counters', 'visitors'], counter);

// Retrieve it
const entry = await db.get(['counters', 'visitors']);
console.log(entry.value); // KvU64 instance
console.log(entry.value.value); // 1000n (the bigint value)

// Use it with atomic operations
await db.atomic()
  .sum(['counters', 'visitors'], 5n)
  .commit();
```

## Listing and Querying

Valkeyrie provides powerful listing capabilities to query your data.

### Listing with Prefix

List all entries with a common prefix:

```typescript
// List all users
const userIterator = db.list({ prefix: ['users'] });

for await (const entry of userIterator) {
  console.log(entry.key, entry.value);
}

// You can also convert to an array
const users = await Array.fromAsync(db.list({ prefix: ['users'] }));
```

### Listing with Range

List entries within a specific range:

```typescript
// List users from 'user1' to 'user5'
const userRange = db.list({
  prefix: ['users'],
  start: ['user1'],
  end: ['user5']
});

for await (const entry of userRange) {
  console.log(entry.key, entry.value);
}

// Or specify a complete range without a prefix
const range = db.list({
  start: ['a'],
  end: ['z']
});
```

### Pagination with Cursors

Implement pagination using limits and cursors:

```typescript
// Get first page (10 items)
const page1 = db.list({ prefix: ['users'] }, { limit: 10 });
const users1 = await Array.fromAsync(page1);

// Get the cursor for the next page
const cursor = page1.cursor;

// Get next page using the cursor
const page2 = db.list({ prefix: ['users'] }, { limit: 10, cursor });
const users2 = await Array.fromAsync(page2);
```

### Reverse Order

List entries in reverse order:

```typescript
const reversedUsers = db.list(
  { prefix: ['users'] },
  { reverse: true }
);

for await (const entry of reversedUsers) {
  console.log(entry.key, entry.value);
}
```

### Using Array.fromAsync

Convert an async iterator to an array:

```typescript
const users = await Array.fromAsync(db.list({ prefix: ['users'] }));
```

## Atomic Operations

Valkeyrie provides powerful atomic operations with optimistic concurrency control.

### Basic Atomic Operations

Perform multiple operations atomically:

```typescript
const result = await db.atomic()
  .set(['users', 'user1'], { name: 'John' })
  .set(['users', 'user2'], { name: 'Jane' })
  .delete(['users', 'user3'])
  .commit();

if (result.ok) {
  console.log('Atomic operation succeeded');
  console.log('Versionstamp:', result.versionstamp);
} else {
  console.log('Atomic operation failed due to conflicts');
}
```

### Check and Set

Implement optimistic concurrency control with check and set:

```typescript
// Get the current entry
const entry = await db.get(['counter']);

// Perform an atomic check and set
const result = await db.atomic()
  .check({ key: ['counter'], versionstamp: entry.versionstamp })
  .set(['counter'], entry.value + 1)
  .commit();

if (result.ok) {
  console.log('Counter incremented successfully');
} else {
  console.log('Counter was modified by another process');
}
```

### Numeric Operations

Perform atomic numeric operations with KvU64:

```typescript
import { KvU64 } from 'valkeyrie/KvU64';

// Initialize a counter
await db.set(['counter'], new KvU64(0n));

// Increment the counter atomically
await db.atomic()
  .sum(['counter'], new KvU64(1n))
  .commit();

// Get the maximum of two values
await db.atomic()
  .max(['counter'], new KvU64(100n))
  .commit();

// Get the minimum of two values
await db.atomic()
  .min(['counter'], new KvU64(50n))
  .commit();
```

### Complex Atomic Operations

Combine multiple operations in a single atomic transaction:

```typescript
await db.atomic()
  // Check conditions
  .check({ key: ['users', 'user1'], versionstamp: userVersionstamp })
  .check({ key: ['inventory', 'item1'], versionstamp: inventoryVersionstamp })
  
  // Update user data
  .set(['users', 'user1', 'orders'], [...existingOrders, newOrder])
  
  // Update inventory
  .sum(['inventory', 'item1', 'count'], new KvU64(-1n)) // Decrement by 1
  
  // Add to order history
  .set(['orders', orderId], orderDetails)
  
  // Execute the transaction
  .commit();
```

## Expiration

Set values with an expiration time:

```typescript
// Value expires in 60 seconds
await db.set(['session', 'token123'], 'abc123', { expireIn: 60 * 1000 });

// Atomic operation with expiration
await db.atomic()
  .set(['session', 'token456'], 'def456', { expireIn: 60 * 1000 })
  .commit();
```

Expired values are automatically removed when accessed and during periodic cleanup.

## Performance Considerations

- **Key Design**: Design your keys carefully to optimize for your access patterns
- **Batch Operations**: Use atomic operations for batching multiple operations
- **Pagination**: Use limits and cursors for large result sets
- **Cleanup**: Call `cleanup()` periodically to remove expired entries
- **Memory Usage**: Be mindful of value sizes, especially when storing large objects or binary data

## Error Handling

Valkeyrie operations may throw errors for various reasons:

```typescript
try {
  await db.set(['key'], value);
} catch (error) {
  if (error instanceof TypeError) {
    console.error('Invalid key or value type:', error.message);
  } else if (error instanceof RangeError) {
    console.error('Value exceeds size limits:', error.message);
  } else {
    console.error('Unexpected error:', error);
  }
}
```

## Advanced Usage

### Custom Drivers

Valkeyrie supports custom drivers through the `defineDriver` function:

```typescript
import { defineDriver } from 'valkeyrie';

const customDriver = defineDriver(async (path) => {
  // Implement your custom driver
  // Must conform to the Driver interface
  return {
    close: async () => { /* ... */ },
    get: async (keyHash, now) => { /* ... */ },
    set: async (keyHash, value, versionstamp, expiresAt) => { /* ... */ },
    delete: async (keyHash) => { /* ... */ },
    list: async (startHash, endHash, prefixHash, now, limit, reverse) => { /* ... */ },
    cleanup: async (now) => { /* ... */ },
    withTransaction: async (callback) => { /* ... */ }
  };
});

// Use the custom driver
const db = await Valkeyrie.open('/path/to/db', customDriver);
```

### Cleanup

Manually trigger cleanup of expired entries:

```typescript
await db.cleanup();
```

### Serializers

Valkeyrie supports pluggable serializers that allow you to customize how values are stored in the database:

```typescript
import { Valkeyrie, jsonSerializer } from 'valkeyrie';

// Using the JSON serializer
const db = await Valkeyrie.open('./data.db', {
  serializer: jsonSerializer
});
```

Valkeyrie comes with two built-in serializers:

- **V8 Serializer (Default)** - Uses Node.js's built-in `node:v8` module for efficient binary serialization
- **JSON Serializer** - Human-readable format compatible with other programming languages

You can also create custom serializers for specialized needs like compression or encryption.

For detailed information about serializers, see the [Serializers documentation](./serializers.md).

## API Reference

### Valkeyrie Class

#### Static Methods

- `static async open(path?: string): Promise<Valkeyrie>`
  - Opens a database at the specified path or in memory if no path is provided

#### Instance Methods

- `async close(): Promise<void>`
  - Closes the database connection
- `async get<T = unknown>(key: Key): Promise<EntryMaybe<T>>`
  - Retrieves a value by key
- `async getMany(keys: Key[]): Promise<EntryMaybe[]>`
  - Retrieves multiple values by keys
- `async set<T extends Value>(key: Key, value: T, options?: SetOptions): Promise<{ ok: true; versionstamp: string }>`
  - Sets a value with the given key
- `async delete(key: Key): Promise<void>`
  - Deletes a value by key
- `list<T = unknown>(selector: ListSelector, options?: ListOptions): AsyncIterableIterator<Entry<T>> & { readonly cursor: string }`
  - Lists entries matching the selector
- `async cleanup(): Promise<void>`
  - Removes expired entries
- `atomic(): Atomic`
  - Creates a new atomic operation
- `async executeAtomicOperation(checks: Check[], mutations: Mutation[]): Promise<{ ok: true; versionstamp: string } | { ok: false }>`
  - Executes an atomic operation

### Atomic Class

- `check(...checks: AtomicCheck[]): Atomic`
  - Adds checks to the atomic operation
- `mutate(...mutations: Mutation[]): Atomic`
  - Adds mutations to the atomic operation
- `set(key: Key, value: Value, options?: SetOptions): Atomic`
  - Adds a set operation
- `delete(key: Key): Atomic`
  - Adds a delete operation
- `sum(key: Key, value: bigint | KvU64): Atomic`
  - Adds a sum operation
- `max(key: Key, value: bigint | KvU64): Atomic`
  - Adds a max operation
- `min(key: Key, value: bigint | KvU64): Atomic`
  - Adds a min operation
- `async commit(): Promise<{ ok: true; versionstamp: string } | { ok: false }>`
  - Commits the atomic operation

### KvU64 Class

- `constructor(value: bigint)`
  - Creates a new KvU64 instance
- `readonly value: bigint`
  - The underlying bigint value
- `valueOf(): bigint`
  - Returns the bigint value
- `toString(): string`
  - Returns the string representation
- `toJSON(): string`
  - Returns the JSON representation

### Types

- `Key`: Array of key parts (`KeyPart[]`)
- `KeyPart`: `string | number | bigint | boolean | Uint8Array`
- `Value`: Any supported value type
- `Entry<T>`: `{ key: Key; value: T; versionstamp: string }`
- `EntryMaybe<T>`: `Entry<T> | { key: Key; value: null; versionstamp: null }`
- `ListSelector`: 
  - `{ prefix: Key }`
  - `{ prefix: Key; start: Key }`
  - `{ prefix: Key; end: Key }`
  - `{ start: Key; end: Key }`
- `ListOptions`: `{ limit?: number; cursor?: string; reverse?: boolean }`
- `SetOptions`: `{ expireIn?: number }`
- `Check`: `{ key: Key; versionstamp: string | null }`
- `Mutation`: 
  - `{ key: Key; type: 'set'; value: Value; expireIn?: number }`
  - `{ key: Key; type: 'delete' }`
  - `{ key: Key; type: 'sum'; value: KvU64 }`
  - `{ key: Key; type: 'max'; value: KvU64 }`
  - `{ key: Key; type: 'min'; value: KvU64 }`
