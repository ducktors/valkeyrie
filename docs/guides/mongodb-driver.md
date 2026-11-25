# MongoDB Driver

The MongoDB driver stores a Valkeyrie database in MongoDB instead of a SQLite file. Several processes, such as the instances of one service, can then share one database, and one collection can hold many databases side by side.

## Table of Contents

- [Installation](#installation)
- [Opening a Database](#opening-a-database)
- [Client Ownership](#client-ownership)
- [Namespaces](#namespaces)
- [Documents and Indexes](#documents-and-indexes)
- [Expiry and Cleanup](#expiry-and-cleanup)
- [Atomic Commits](#atomic-commits)
- [Watching Keys](#watching-keys)
- [Compatibility](#compatibility)

## Installation

The driver uses the official [MongoDB Node.js driver](https://github.com/mongodb/node-mongodb-native), which you install next to Valkeyrie:

```bash
pnpm add valkeyrie mongodb
```

## Opening a Database

```typescript
import { MongoClient } from 'mongodb';
import { Valkeyrie } from 'valkeyrie';
import { mongodbDriver } from 'valkeyrie/drivers/mongodb';

const client = await MongoClient.connect('mongodb://localhost:27017/app?replicaSet=rs0');
const db = await Valkeyrie.open(
  mongodbDriver({ client, namespace: 'my-app' })
);

await db.set(['users', 'alice'], { name: 'Alice' });

await db.close();
await client.close();
```

Every Valkeyrie feature works as with SQLite: `get`, `set`, `delete`, `list`, atomic commits, expiry, `watch`, `clear` and `destroy`. Serializers work the same way; the default is the V8 serializer.

The options are:

| Option                   | Default                               | Meaning                                           |
| ------------------------ | ------------------------------------- | ------------------------------------------------- |
| `client`                 | (required)                            | A `MongoClient`                                   |
| `namespace`              | (required)                            | Names the database; see [Namespaces](#namespaces) |
| `namespaceField`         | `'namespace'`                         | The document field that holds the namespace       |
| `dbName`                 | the database of the connection string | The MongoDB database                              |
| `collectionName`         | `'valkeyrie'`                         | The collection of the entries                     |
| `sequenceCollectionName` | `'valkeyrie_sequences'`               | The collection of the versionstamp counters       |

## Client Ownership

You create the client, and you close it. `db.close()` does not close your client. The driver opens a session for each atomic commit and ends it when the commit ends.

## Namespaces

The namespace names the database: one namespace is one Valkeyrie database. Two drivers with the same namespace see the same data, even on different clients or in different processes. Two namespaces never see each other's keys, and `clear`, `destroy` and `cleanup` touch only their own namespace.

The namespace can be a string, a number or a BSON value such as an `ObjectId` or a `UUID`. The driver stores it as given, so a namespace that is an `ObjectId` stays an `ObjectId`, and you can use the IDs of your own documents:

```typescript
const db = await Valkeyrie.open(
  mongodbDriver({ client, namespace: workspace._id, namespaceField: 'workspaceId' })
);
```

The driver rejects an empty string and a plain object: MongoDB would read a plain object such as `{ $ne: null }` as a query, which matches other namespaces.

Use one `namespaceField` per collection. The unique index covers the namespace field and the key, so documents of another namespace field would collide on it.

## Documents and Indexes

Each entry is one document in the entries collection:

| Field          | Holds                                                                  |
| -------------- | ---------------------------------------------------------------------- |
| `namespace`    | the namespace (the field is named by `namespaceField`)                 |
| `keyHash`      | the key bytes as lowercase hex, as Valkeyrie gives them to drivers     |
| `value`        | the serialized value, as binary                                        |
| `versionstamp` | the versionstamp, 20 hex characters                                    |
| `expiresAt`    | the expiry time in milliseconds, only on entries with an expiry        |

Two rows of each namespace are not entries, and `list()` never returns them: the key format row that Valkeyrie writes, and the commit lock row that the driver writes (see [Atomic Commits](#atomic-commits)). Their `keyHash` starts with `ff`, which no key starts with.

The sequences collection holds one document per namespace, with the counter for versionstamps in `sequence`. A versionstamp is the time in microseconds shifted left by 20 bits, OR the low 20 bits of the counter, as with SQLite. `clear` keeps the counter, and `destroy` deletes it.

The driver creates these indexes when it opens a database:

- `{ namespace: 1, keyHash: 1 }`, unique, on the entries
- `{ namespace: 1, expiresAt: 1 }` on the entries, for cleanup
- `{ namespace: 1 }`, unique, on the sequences

Valkeyrie orders keys by their bytes, so the entries collection must compare strings by their bytes too: it must not have a default collation other than `simple`. The driver checks this when it opens a database and throws if the collection has another collation.

## Expiry and Cleanup

Valkeyrie decides whether a value has expired by your process's clock: an expired value reads as missing at once, and `list()` skips it. The document stays until cleanup deletes it.

`db.cleanup()` deletes the expired entries of the namespace and tells the watchers of the deleted keys. Valkeyrie runs it when it opens a database. A long-running process should also call it regularly, for example every minute. Several processes can run it at the same time. A key that another process sets again while cleanup runs is kept.

The driver does not use a TTL index: MongoDB would delete expired documents without telling the watchers.

## Atomic Commits

`db.atomic()` commits all or nothing, also across processes, in a MongoDB transaction:

- A check that does not hold returns `{ ok: false }`, as with SQLite.
- Every commit first writes the commit lock row of its namespace. Two commits of one namespace therefore conflict, and MongoDB lets only one commit. The other runs again on the new data, after 5–20 ms, so a check that no longer holds returns `{ ok: false }`, and a sum on a busy counter succeeds. Without the lock row, two commits that each check a key that the other writes could both succeed.
- After 10 conflicts in a row, the commit returns `{ ok: false }`.
- A commit that throws, such as a `sum` on a value that is not a `KvU64`, writes nothing.

One driver instance runs one commit at a time, and commits of one namespace in different processes take turns through the lock row. Plain `set` and `delete` calls do not take the lock. Spread unrelated data over namespaces if many processes commit at once.

## Watching Keys

`db.watch()` reports the changes made through the same database instance, in the same process. Writes from other processes or other instances are not reported.

Watchers get the value that was written, not a value read back after the write: under load, a read right after a commit can still see the old value. A commit tells the watchers once, after it commits. MongoDB change streams could report other processes' writes, but they start at the time they open, which makes the first value of a watch and the changes after it hard to line up.

## Compatibility

- The server must be a replica set or a sharded cluster, because commits use transactions. A single-node replica set works for development. A standalone server rejects the first commit, which `Valkeyrie.open()` already makes.
- MongoDB 7.0 or later. The test suite runs on MongoDB 7.0 and 8.0.
- The `mongodb` Node.js driver 6 or 7. The driver imports nothing from it at run time and uses only APIs that work the same from version 4 on, but the test suite runs with version 7.

To move a store that Valkeyrie 0.8 or earlier wrote, see [Migrate the Store of a Custom Driver](./upgrading.md#migrate-the-store-of-a-custom-driver).
