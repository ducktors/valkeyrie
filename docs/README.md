# Valkeyrie Documentation

Welcome to the Valkeyrie documentation! This directory contains comprehensive guides and API references for using Valkeyrie.

## Documentation Structure

### For Beginners

Start here if you're new to Valkeyrie:

1. **[Getting Started Guide](./guides/getting-started.md)**
   - Installation and setup
   - Basic operations (set, get, delete, list)
   - Understanding keys and values
   - Common patterns and best practices

### Guides

Feature-specific guides for different use cases:

- **[Schema Validation](./guides/schema-validation.md)**
  - Type-safe operations with Zod, Valibot, and ArkType
  - Automatic TypeScript type inference
  - Pattern matching and validation timing
  - Error handling

- **[Factory Methods](./guides/factory-methods.md)**
  - Creating databases from arrays and iterables
  - Streaming data with `fromAsync()`
  - Progress tracking and error handling
  - Real-world import/migration examples
  - Custom drivers via `openWithDriver()` and the `driverFn` option

- **[Serializers](./guides/serializers.md)**
  - Choosing the right serializer
  - V8, JSON, BSON, MessagePack, and CBOR-X
  - Creating custom serializers
  - Migration between serializers

- **[Advanced Patterns](./guides/advanced-patterns.md)**
  - **Watch API** - Real-time key monitoring
  - Atomic operations
  - Optimistic concurrency control
  - KvU64 numeric operations
  - Multi-instance concurrency
  - Real-world patterns (sessions, caching, events)

### API Reference

Complete technical reference:

- **[API Reference](./api/api-reference.md)**
  - All methods with parameters and return types
  - Valkeyrie class methods
  - AtomicOperation methods
  - KvU64 class
  - Complete code examples

- **[TypeScript Types](./api/types.md)**
  - Core types (Key, Entry, Value)
  - Operation types
  - Schema validation types
  - Type inference
  - Best practices for type safety

## Quick Links

### Common Tasks

- **Set up a new database** → [Getting Started](./guides/getting-started.md#your-first-database)
- **Add validation** → [Schema Validation](./guides/schema-validation.md#quick-start)
- **Import data** → [Factory Methods](./guides/factory-methods.md#valkeyriefrom---synchronous-data)
- **Watch for changes** → [Watch API](./guides/advanced-patterns.md#watch-api)
- **Atomic transactions** → [Atomic Operations](./guides/advanced-patterns.md#atomic-operations)
- **Choose a serializer** → [Serializers Guide](./guides/serializers.md#choosing-a-serializer)

### By Feature

| Feature | Documentation |
|---------|--------------|
| Basic CRUD | [Getting Started](./guides/getting-started.md#basic-operations) |
| Type Safety | [Schema Validation](./guides/schema-validation.md) |
| Data Import | [Factory Methods](./guides/factory-methods.md) |
| Real-time Updates | [Watch API](./guides/advanced-patterns.md#watch-api) |
| Transactions | [Atomic Operations](./guides/advanced-patterns.md#atomic-operations) |
| Counters | [KvU64](./guides/advanced-patterns.md#numeric-operations-with-kvu64) |
| Serialization | [Serializers](./guides/serializers.md) |
| TTL/Expiration | [Getting Started](./guides/getting-started.md#data-expiration) |
| Pagination | [API Reference - list()](./api/api-reference.md#list) |
| Multi-process | [Multi-Instance](./guides/advanced-patterns.md#multi-instance-concurrency) |

## What's New

Recent additions to Valkeyrie (check the [CHANGELOG](https://github.com/ducktors/valkeyrie/blob/main/CHANGELOG.md) for full history):

## Examples by Use Case

### Web Application

```typescript
// Session management
const db = await Valkeyrie.open('./sessions.db');

await db.set(['sessions', sessionId], userData, {
  expireIn: 24 * 60 * 60 * 1000 // 24 hours
});
```

[More →](./guides/advanced-patterns.md#session-management)

### Caching Layer

```typescript
// Smart cache with TTL
const cache = await Valkeyrie.open('./cache.db');

await cache.set(['api', endpoint], response, {
  expireIn: 5 * 60 * 1000 // 5 minutes
});
```

[More →](./guides/advanced-patterns.md#caching-layer)

### Data Migration

```typescript
// Import from MongoDB
const db = await Valkeyrie.fromAsync(mongoDbCursor, {
  prefix: ['users'],
  keyProperty: (doc) => doc._id.toString()
});
```

[More →](./guides/factory-methods.md#migrating-from-mongodb)

### Real-time Dashboard

```typescript
// Watch metrics
const stream = db.watch([
  ['metrics', 'cpu'],
  ['metrics', 'memory']
]);

for await (const [cpu, memory] of stream) {
  updateDashboard({ cpu: cpu.value, memory: memory.value });
}
```

[More →](./guides/advanced-patterns.md#real-time-dashboard-example)

## Migration Guides

### From Deno.kv

Valkeyrie's API is heavily inspired by Deno.kv, making migration straightforward:

```typescript
// Deno.kv
const kv = await Deno.openKv();
await kv.set(['key'], 'value');

// Valkeyrie
const db = await Valkeyrie.open();
await db.set(['key'], 'value');
```

Main differences:
- File path is first parameter in `open()`
- Schema validation available via `withSchema()`
- Pluggable serializers
- Factory methods (`from`/`fromAsync`)

## Contributing to Documentation

Found an issue or want to improve the docs? See [CONTRIBUTING.md](https://github.com/ducktors/valkeyrie/blob/main/CONTRIBUTING.md).

### Documentation Structure

```
docs/
├── README.md                    # This file
├── guides/                      # Feature guides
│   ├── getting-started.md      # Beginner tutorial
│   ├── schema-validation.md    # Type safety guide
│   ├── factory-methods.md      # Data import guide
│   ├── serializers.md          # Serialization guide
│   └── advanced-patterns.md    # Advanced features
└── api/                         # API reference
    ├── api-reference.md        # Method reference
    └── types.md                # Type definitions
```

## Getting Help

- **Questions?** Open a [GitHub Discussion](https://github.com/ducktors/valkeyrie/discussions)
- **Bug reports?** Open a [GitHub Issue](https://github.com/ducktors/valkeyrie/issues)
- **Feature requests?** Open a [GitHub Issue](https://github.com/ducktors/valkeyrie/issues)

## License

Valkeyrie is MIT licensed. See [LICENSE](https://github.com/ducktors/valkeyrie/blob/main/LICENSE) for details.
