# Valkey and Redis Driver

The Valkey driver stores a Valkeyrie database in Valkey or Redis instead of a SQLite file. Several processes, such as the instances of one service, can then share one database.

## Table of Contents

- [Installation](#installation)
- [Opening a Database](#opening-a-database)
- [Client Ownership](#client-ownership)
- [Namespaces](#namespaces)
- [Expiry, Cleanup and Memory](#expiry-cleanup-and-memory)
- [Atomic Commits](#atomic-commits)
- [Watching Keys](#watching-keys)
- [Compatibility](#compatibility)

## Installation

The driver uses an [ioredis](https://github.com/redis/ioredis) client, which you install next to Valkeyrie:

```bash
pnpm add valkeyrie ioredis
```

## Opening a Database

```typescript
import { Redis } from 'ioredis';
import { Valkeyrie } from 'valkeyrie';
import { valkeyDriver } from 'valkeyrie/drivers/valkey';

const client = new Redis('redis://localhost:6379');
const db = await Valkeyrie.open(
  valkeyDriver({ client, namespace: 'my-app' })
);

await db.set(['users', 'alice'], { name: 'Alice' });

await db.close();
await client.quit();
```

Every Valkeyrie feature works as with SQLite: `get`, `set`, `delete`, `list`, atomic commits, expiry, `watch`, `clear` and `destroy`. Serializers work the same way; the default is the V8 serializer.

## Client Ownership

You create the client, and you close it:

- `db.close()` does not close your client.
- The driver opens one more connection, a duplicate of your client, for atomic commits. It opens it on the first commit and closes it in `db.close()`.
- The client can be an ioredis `Redis` or `Cluster`. [iovalkey](https://github.com/valkey-io/iovalkey) has the same API.

For a server, options that fail fast are a good start:

```typescript
const client = new Redis(url, {
  enableAutoPipelining: true, // concurrent reads share one round trip
  enableOfflineQueue: false, // fail at once while disconnected
  maxRetriesPerRequest: 1,
  connectTimeout: 2_000,
});
```

## Namespaces

The namespace names the database: one namespace is one Valkeyrie database. Two drivers with the same namespace see the same data, even on different clients or in different processes. Two namespaces never see each other's keys.

All keys of a namespace start with `{namespace}:`. The braces make a Redis [hash tag](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/#hash-tags), so in cluster mode a whole namespace stays on one shard and atomic commits keep working. A namespace must not be empty and must not contain `{` or `}`.

| Key                            | Type       | Holds                                                           |
| ------------------------------ | ---------- | --------------------------------------------------------------- |
| `{namespace}:e:` + key bytes   | string     | the versionstamp, the expiry time and the serialized value      |
| `{namespace}:idx`              | sorted set | every key, for `list`                                           |
| `{namespace}:exp`              | sorted set | the keys that expire, scored by their expiry time               |
| `{namespace}:seq`              | string     | the counter shared by every client, for versionstamps           |

## Expiry, Cleanup and Memory

A value set with `expireIn` also gets a Redis TTL, so Redis deletes it on time by itself. The TTL is the time left on your process's clock when the value is written, so a server clock that runs ahead does not expire values early. Valkeyrie still decides whether a value has expired by your process's clock. Setting the key again without `expireIn` removes the TTL.

`db.cleanup()` removes expired keys from the two indexes. Valkeyrie runs it when it opens a database. A long-running process should also call it regularly, for example every minute. Several processes can run it at the same time.

If the database holds data you can rebuild, such as caches, set the server's `maxmemory-policy` to `volatile-lru`. When memory runs out, the server then evicts values that have a TTL, and never the indexes or the counter, which have none. Valkeyrie reads an evicted value as missing, `list` skips it, and cleanup removes it from the indexes at its expiry time. Avoid the `allkeys-*` policies: they can evict the indexes and the counter.

## Atomic Commits

`db.atomic()` commits all or nothing, also across processes. The driver uses `WATCH` and `MULTI`/`EXEC`:

- A check that does not hold returns `{ ok: false }`, as with SQLite.
- When another process changes a key that the commit read, the commit runs again on the new data. A check that no longer holds then returns `{ ok: false }`, and other commits succeed. A sum on a busy counter therefore succeeds, as on SQLite.
- After 10 such conflicts in a row, the commit returns `{ ok: false }`.

One driver instance runs one commit at a time. Open one database per process and share it.

## Watching Keys

`db.watch()` reports the changes made through the same database instance, in the same process. Writes from other processes or other instances are not reported.

## Compatibility

- Redis 6.2 or later, and Valkey 7.2 or later. The test suite runs on standalone Redis 6.2, Redis 7.2, Valkey 8 and Valkey 9 servers.
- Sentinel works through an ioredis `Redis` client, and cluster mode through an ioredis `Cluster` client, since a namespace stays on one shard. The test suite does not cover them yet.
- The driver uses only core commands, which Redis and Valkey share, and no modules: `GET`, `MGET`, `SET` (with `PX`), `UNLINK`, `INCR`, `ZADD`, `ZREM`, `ZRANGEBYLEX`, `ZREVRANGEBYLEX`, `ZRANGEBYSCORE`, `WATCH`, `MULTI`, `EXEC` and `UNWATCH`.
