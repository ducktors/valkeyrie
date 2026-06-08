# Factory Methods

Valkeyrie provides powerful factory methods to create and populate databases from existing data sources. This is perfect for data migrations, imports, seeding databases, or creating databases from API responses.

## Table of Contents

- [Overview](#overview)
- [Valkeyrie.from() - Synchronous Data](#valkeyriefrom---synchronous-data)
- [Valkeyrie.fromAsync() - Asynchronous Data](#valkeyriefromasync---asynchronous-data)
- [Options Reference](#options-reference)
- [Key Extraction](#key-extraction)
- [Progress Tracking](#progress-tracking)
- [Error Handling](#error-handling)
- [Performance](#performance)
- [With Schema Validation](#with-schema-validation)
- [Real-World Examples](#real-world-examples)
- [Best Practices](#best-practices)

## Overview

Instead of opening an empty database and inserting items one by one, factory methods let you create and populate a database in one operation:

```typescript
// ❌ The hard way
const db = await Valkeyrie.open('./users.db');
for (const user of users) {
  await db.set(['users', user.id], user);
}

// ✅ The easy way
const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id',
  path: './users.db'
});
```

**Benefits:**
- Much faster (batched atomic operations)
- Less code
- Progress tracking built-in
- Flexible error handling
- Works with any iterable source

## Valkeyrie.from() - Synchronous Data

Use `from()` for synchronous iterables: arrays, Sets, Maps, or custom iterables.

### Basic Usage

```typescript
import { Valkeyrie } from 'valkeyrie';

const users = [
  { id: 1, name: 'Alice', email: 'alice@example.com' },
  { id: 2, name: 'Bob', email: 'bob@example.com' },
  { id: 3, name: 'Charlie', email: 'charlie@example.com' }
];

const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id'
});

// Database is now populated!
const alice = await db.get(['users', 1]);
console.log(alice.value); // { id: 1, name: 'Alice', email: 'alice@example.com' }
```

### From Arrays

The most common use case:

```typescript
const products = [
  { sku: 'LAPTOP-1', name: 'Laptop', price: 999 },
  { sku: 'MOUSE-1', name: 'Mouse', price: 29 },
  { sku: 'KEYBOARD-1', name: 'Keyboard', price: 79 }
];

const db = await Valkeyrie.from(products, {
  prefix: ['products'],
  keyProperty: 'sku',
  path: './products.db'
});
```

### From Sets

```typescript
const uniqueEmails = new Set([
  'alice@example.com',
  'bob@example.com',
  'charlie@example.com'
]);

const db = await Valkeyrie.from(uniqueEmails, {
  prefix: ['emails'],
  keyProperty: (email) => email, // Use the email itself as key
  path: './emails.db'
});

// Access by email
const exists = await db.get(['emails', 'alice@example.com']);
```

### From Maps

```typescript
const configMap = new Map([
  ['database_url', 'postgresql://...'],
  ['api_key', 'secret-key'],
  ['max_connections', 100]
]);

const db = await Valkeyrie.from(configMap.entries(), {
  prefix: ['config'],
  keyProperty: ([key]) => key, // Extract key from [key, value] tuple
  path: './config.db'
});
```

### From Custom Iterables

```typescript
class UserRepository {
  *getAll() {
    // Custom iteration logic
    for (let i = 1; i <= 100; i++) {
      yield {
        id: i,
        name: `User ${i}`,
        email: `user${i}@example.com`
      };
    }
  }
}

const repo = new UserRepository();
const db = await Valkeyrie.from(repo.getAll(), {
  prefix: ['users'],
  keyProperty: 'id'
});
```

## Valkeyrie.fromAsync() - Asynchronous Data

Use `fromAsync()` for async iterables: async generators, streams, or async iterators.

### From Async Generators

Perfect for paginated APIs:

```typescript
async function* fetchAllUsers() {
  let page = 1;
  let hasMore = true;

  while (hasMore) {
    const response = await fetch(`/api/users?page=${page}`);
    const data = await response.json();

    for (const user of data.users) {
      yield user;
    }

    hasMore = data.hasMore;
    page++;
  }
}

const db = await Valkeyrie.fromAsync(fetchAllUsers(), {
  prefix: ['users'],
  keyProperty: 'id',
  path: './users.db',
  onProgress: (processed) => {
    console.log(`Imported ${processed} users...`);
  }
});
```

### From Streams

```typescript
import { createReadStream } from 'node:fs';
import { parse } from 'csv-parse';
import { Readable } from 'node:stream';

// Parse CSV file
const stream = createReadStream('./users.csv')
  .pipe(parse({
    columns: true,
    skip_empty_lines: true
  }));

const db = await Valkeyrie.fromAsync(Readable.from(stream), {
  prefix: ['users'],
  keyProperty: 'id',
  path: './users.db'
});
```

### From Database Cursors

```typescript
async function* fetchFromDatabase() {
  const cursor = db.collection('users').find().cursor();

  for await (const doc of cursor) {
    yield doc;
  }
}

const valkeyrie = await Valkeyrie.fromAsync(fetchFromDatabase(), {
  prefix: ['users'],
  keyProperty: '_id',
  path: './migrated-users.db'
});
```

### Processing Large Datasets

```typescript
async function* processLargeDataset() {
  const batchSize = 100;
  let offset = 0;

  while (true) {
    const batch = await fetchDataBatch(offset, batchSize);
    if (batch.length === 0) break;

    for (const item of batch) {
      // Transform data before inserting
      const processed = {
        id: item.id,
        data: await processItem(item),
        processedAt: Date.now()
      };

      yield processed;
    }

    offset += batchSize;
  }
}

const db = await Valkeyrie.fromAsync(processLargeDataset(), {
  prefix: ['processed'],
  keyProperty: 'id',
  expireIn: 86400000, // 24 hours TTL
  onProgress: (processed) => {
    if (processed % 1000 === 0) {
      console.log(`Processed ${processed} items`);
    }
  }
});
```

## Options Reference

Both `from()` and `fromAsync()` accept the same options:

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `prefix` | `Key` | **Yes** | Key prefix for all entries (e.g., `['users']`) |
| `keyProperty` | `keyof T \| (item: T) => KeyPart` | **Yes** | Property name or function to extract key part |
| `path` | `string` | No | Database file path. If omitted, creates in-memory database |
| `driverFn` | `(serializer?: () => Serializer) => Promise<Driver>` | No | Custom driver function. Takes precedence over `path` |
| `serializer` | `() => Serializer` | No | Custom serializer (default: v8 serializer) |
| `destroyOnClose` | `boolean` | No | Destroy database file on close (default: `false`) |
| `expireIn` | `number` | No | TTL for all entries in milliseconds |
| `onProgress` | `(processed: number, total?: number) => void` | No | Progress callback. `total` is only provided for sync iterables with known size |
| `onError` | `'stop' \| 'continue'` | No | Error handling strategy (default: `'stop'`) |
| `onErrorCallback` | `(error: Error, item: T) => void` | No | Called for each error when `onError: 'continue'` |

### Custom Drivers

For advanced use cases, supply a custom driver via `driverFn` instead of `path`. The driver function receives the resolved serializer factory and returns a `Driver` instance. The `Driver` type and `defineDriver` helper for authoring your own backend are available from `'valkeyrie/driver'`. Omitting `driverFn` and using `path` (or neither) keeps the built-in SQLite driver.

```typescript
import { Valkeyrie } from 'valkeyrie';
// `Driver` type and `defineDriver` helper from 'valkeyrie/driver'

// `createMyDriver` returns an object implementing the Driver interface
const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id',
  driverFn: async (serializer) => createMyDriver(serializer),
});
```

## Key Extraction

The `keyProperty` option determines how keys are extracted from items.

### Using Property Names

```typescript
const users = [
  { id: 1, name: 'Alice' },
  { id: 2, name: 'Bob' }
];

// Use 'id' property as key
const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id' // Property name
});

// Results in keys: ['users', 1], ['users', 2]
```

### Using Functions

For more control, use a function:

```typescript
const users = [
  { firstName: 'Alice', lastName: 'Smith' },
  { firstName: 'Bob', lastName: 'Jones' }
];

// Use combined name as key
const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: (user) => `${user.firstName}-${user.lastName}`
});

// Results in keys: ['users', 'Alice-Smith'], ['users', 'Bob-Jones']
```

### Transforming Keys

```typescript
const users = [
  { email: 'Alice@EXAMPLE.COM' },
  { email: 'Bob@EXAMPLE.COM' }
];

// Normalize emails
const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: (user) => user.email.toLowerCase()
});

// Results in keys: ['users', 'alice@example.com'], ['users', 'bob@example.com']
```

### Valid Key Parts

The extracted key part must be one of:
- `string`
- `number`
- `bigint`
- `boolean`
- `Uint8Array`

```typescript
// ❌ Invalid - returns an object
keyProperty: (item) => ({ id: item.id })

// ✅ Valid - returns a string
keyProperty: (item) => String(item.id)

// ✅ Valid - returns a number
keyProperty: 'id'

// ✅ Valid - returns a bigint
keyProperty: (item) => BigInt(item.id)
```

## Progress Tracking

### Synchronous Iterables

For arrays and other sync iterables with known size, `total` is provided:

```typescript
const users = new Array(10000).fill(null).map((_, i) => ({
  id: i,
  name: `User ${i}`
}));

const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id',
  onProgress: (processed, total) => {
    const percent = ((processed / total!) * 100).toFixed(1);
    console.log(`Progress: ${processed}/${total} (${percent}%)`);
  }
});
```

### Asynchronous Iterables

For async iterables, `total` is not known:

```typescript
const db = await Valkeyrie.fromAsync(fetchAllUsers(), {
  prefix: ['users'],
  keyProperty: 'id',
  onProgress: (processed) => {
    // No total available
    console.log(`Processed ${processed} items...`);

    // Update every 100 items
    if (processed % 100 === 0) {
      console.log(`Milestone: ${processed} items processed`);
    }
  }
});
```

### Progress Bar Integration

```typescript
import ProgressBar from 'progress';

const users = [...]; // Array with known size

const bar = new ProgressBar('Importing [:bar] :current/:total :percent', {
  total: users.length
});

const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id',
  onProgress: (processed, total) => {
    bar.tick();
  }
});
```

## Error Handling

### Stop on Error (Default)

By default, the import stops immediately when an error occurs:

```typescript
const users = [
  { id: 1, name: 'Alice' },
  { id: 2 }, // Missing name - might cause validation error
  { id: 3, name: 'Charlie' }
];

try {
  const db = await Valkeyrie.from(users, {
    prefix: ['users'],
    keyProperty: 'id'
  });
} catch (error) {
  console.error('Import failed:', error);
  // Database is automatically closed on error
}
```

### Continue on Error

Skip invalid items and continue:

```typescript
const errors: Array<{ item: any; error: Error }> = [];

const db = await Valkeyrie.from(users, {
  prefix: ['users'],
  keyProperty: 'id',
  onError: 'continue',
  onErrorCallback: (error, item) => {
    console.error(`Failed to import item:`, item, error);
    errors.push({ item, error });
  }
});

console.log(`Imported successfully, ${errors.length} items failed`);
```

### Validation Errors with Schemas

```typescript
import { z } from 'zod';
import { ValidationError } from 'valkeyrie';

const userSchema = z.object({
  id: z.number(),
  email: z.string().email()
});

const users = [
  { id: 1, email: 'valid@example.com' },
  { id: 2, email: 'invalid-email' }, // Invalid!
  { id: 3, email: 'another@example.com' }
];

const invalidItems: any[] = [];

const db = await Valkeyrie
  .withSchema(['users', '*'], userSchema)
  .from(users, {
    prefix: ['users'],
    keyProperty: 'id',
    onError: 'continue',
    onErrorCallback: (error, item) => {
      if (error instanceof ValidationError) {
        console.log(`Validation failed for item ${item.id}:`, error.issues);
        invalidItems.push(item);
      }
    }
  });

console.log(`Imported ${users.length - invalidItems.length} valid items`);
```

## Performance

Factory methods are optimized for performance:

### Automatic Batching

Items are automatically batched into groups of 1000 and inserted using atomic operations:

```typescript
// Under the hood, this:
const db = await Valkeyrie.from(largeArray, {
  prefix: ['items'],
  keyProperty: 'id'
});

// Does this:
// Batch 1: items 0-999 (atomic operation)
// Batch 2: items 1000-1999 (atomic operation)
// Batch 3: items 2000-2999 (atomic operation)
// ... and so on
```

This provides excellent performance while maintaining atomicity within each batch.

### Memory Efficiency

For async iterables, items are processed as they arrive:

```typescript
// Memory-efficient: processes items one at a time
async function* hugeDataset() {
  for (let i = 0; i < 1000000; i++) {
    yield { id: i, data: generateData() };
  }
}

const db = await Valkeyrie.fromAsync(hugeDataset(), {
  prefix: ['items'],
  keyProperty: 'id'
});
```

### Performance Tips

1. **Use async iterables for large datasets**
   ```typescript
   // ❌ Loads everything into memory
   const allData = await fetchAllData();
   const db = await Valkeyrie.from(allData, options);

   // ✅ Streams data
   const db = await Valkeyrie.fromAsync(streamData(), options);
   ```

2. **Batch external API calls**
   ```typescript
   async function* fetchInBatches() {
     const batchSize = 100;
     for (let page = 0; page < totalPages; page++) {
       const batch = await fetch(`/api/items?page=${page}&size=${batchSize}`);
       for (const item of batch) {
         yield item;
       }
     }
   }
   ```

3. **Choose the right serializer**
   ```typescript
   // Faster for simple data
   const db = await Valkeyrie.from(data, {
     prefix: ['items'],
     keyProperty: 'id',
     serializer: () => jsonSerializer()
   });
   ```

## With Schema Validation

Factory methods work seamlessly with schema validation:

```typescript
import { z } from 'zod';

const userSchema = z.object({
  id: z.number(),
  name: z.string().min(1),
  email: z.string().email(),
  age: z.number().int().min(0).optional()
});

const users = [
  { id: 1, name: 'Alice', email: 'alice@example.com', age: 30 },
  { id: 2, name: 'Bob', email: 'bob@example.com' },
  { id: 3, name: 'Charlie', email: 'charlie@example.com', age: 25 }
];

// All items are validated during import
const db = await Valkeyrie
  .withSchema(['users', '*'], userSchema)
  .from(users, {
    prefix: ['users'],
    keyProperty: 'id',
    path: './users.db'
  });

// Type inference works!
const user = await db.get(['users', 1]);
// user.value is typed as the schema output type
```

## Real-World Examples

### Migrating from MongoDB

```typescript
import { MongoClient } from 'mongodb';

async function migrateFromMongoDB() {
  const mongo = await MongoClient.connect('mongodb://localhost:27017');
  const collection = mongo.db('myapp').collection('users');

  async function* streamUsers() {
    const cursor = collection.find();
    for await (const doc of cursor) {
      yield doc;
    }
  }

  const db = await Valkeyrie.fromAsync(streamUsers(), {
    prefix: ['users'],
    keyProperty: (user) => user._id.toString(),
    path: './migrated-db.db',
    onProgress: (processed) => {
      console.log(`Migrated ${processed} users`);
    }
  });

  await mongo.close();
  return db;
}
```

### Importing CSV Files

```typescript
import { createReadStream } from 'node:fs';
import { parse } from 'csv-parse';
import { Readable } from 'node:stream';

async function importCSV(filePath: string) {
  const stream = createReadStream(filePath)
    .pipe(parse({
      columns: true,
      skip_empty_lines: true,
      cast: true // Automatically convert types
    }));

  return await Valkeyrie.fromAsync(Readable.from(stream), {
    prefix: ['records'],
    keyProperty: 'id',
    path: './imported.db',
    onProgress: (processed) => {
      if (processed % 1000 === 0) {
        console.log(`Imported ${processed} records`);
      }
    }
  });
}
```

### Seeding Test Data

```typescript
import { faker } from '@faker-js/faker';

function* generateTestUsers(count: number) {
  for (let i = 0; i < count; i++) {
    yield {
      id: i + 1,
      name: faker.person.fullName(),
      email: faker.internet.email(),
      createdAt: faker.date.past()
    };
  }
}

// Create a test database with 10,000 users
const testDb = await Valkeyrie.from(generateTestUsers(10000), {
  prefix: ['users'],
  keyProperty: 'id',
  path: './test-data.db',
  destroyOnClose: true // Clean up after tests
});
```

### Caching API Responses

```typescript
async function* fetchAndCacheProducts() {
  const response = await fetch('https://api.example.com/products');
  const products = await response.json();

  for (const product of products) {
    yield product;
  }
}

const cache = await Valkeyrie.fromAsync(fetchAndCacheProducts(), {
  prefix: ['products'],
  keyProperty: 'id',
  expireIn: 3600000, // 1 hour cache
  onProgress: (processed) => {
    console.log(`Cached ${processed} products`);
  }
});
```

### Building Search Indexes

```typescript
const posts = [
  { id: 1, title: 'Hello World', content: 'This is my first post' },
  { id: 2, title: 'Another Post', content: 'More content here' }
];

// Index by ID
const byId = await Valkeyrie.from(posts, {
  prefix: ['posts', 'by-id'],
  keyProperty: 'id'
});

// Index by title (normalized)
const byTitle = await Valkeyrie.from(posts, {
  prefix: ['posts', 'by-title'],
  keyProperty: (post) => post.title.toLowerCase().replace(/\s+/g, '-')
});

// Now you can search by both
const post1 = await byId.get(['posts', 'by-id', 1]);
const post2 = await byTitle.get(['posts', 'by-title', 'hello-world']);
```

## Best Practices

### 1. Choose the Right Method

```typescript
// ✅ Use from() for arrays and known-size data
const db = await Valkeyrie.from(arrayData, options);

// ✅ Use fromAsync() for streams and large datasets
const db = await Valkeyrie.fromAsync(streamData(), options);
```

### 2. Handle Errors Appropriately

```typescript
// For production: log and continue
const db = await Valkeyrie.from(data, {
  prefix: ['items'],
  keyProperty: 'id',
  onError: 'continue',
  onErrorCallback: (error, item) => {
    logger.error('Import failed for item', { item, error });
  }
});

// For development: stop and debug
const db = await Valkeyrie.from(data, {
  prefix: ['items'],
  keyProperty: 'id',
  onError: 'stop' // Default
});
```

### 3. Use TTL for Temporary Data

```typescript
const cache = await Valkeyrie.from(data, {
  prefix: ['cache'],
  keyProperty: 'id',
  expireIn: 3600000 // 1 hour
});
```

### 4. Monitor Progress for Long Operations

```typescript
const db = await Valkeyrie.from(largeDataset, {
  prefix: ['items'],
  keyProperty: 'id',
  onProgress: (processed, total) => {
    if (processed % 5000 === 0) {
      console.log(`Progress: ${processed}/${total}`);
    }
  }
});
```

### 5. Clean Up Test Databases

```typescript
const testDb = await Valkeyrie.from(testData, {
  prefix: ['test'],
  keyProperty: 'id',
  path: './test.db',
  destroyOnClose: true // Automatic cleanup
});

// Use in tests
await using db = await Valkeyrie.from(testData, options);
// Automatically closed and destroyed
```

### 6. Validate Data with Schemas

```typescript
// Define what valid data looks like
const db = await Valkeyrie
  .withSchema(['users', '*'], userSchema)
  .from(externalData, {
    prefix: ['users'],
    keyProperty: 'id',
    onError: 'continue' // Skip invalid items
  });
```

## Summary

- ✅ How to create databases from arrays, Sets, Maps, and custom iterables
- ✅ How to stream large datasets efficiently with `fromAsync()`
- ✅ Key extraction strategies and options
- ✅ Progress tracking and error handling
- ✅ Performance optimization techniques
- ✅ Integration with schema validation
- ✅ Real-world migration and import patterns

Next steps:
- **[Advanced Patterns](./advanced-patterns.md)** - Atomic operations and watch API
- **[Serializers](./serializers.md)** - Choose the right serializer for your use case
