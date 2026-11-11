# Upgrading to 0.9

Valkeyrie 0.9 stores some keys with new bytes. It does not open a store that Valkeyrie 0.8 or earlier wrote until you migrate the store. This guide tells you if you must migrate, and how.

## Table of Contents

- [What Changed](#what-changed)
- [What 0.9 Does When It Opens a Store](#what-0-9-does-when-it-opens-a-store)
- [Do You Need to Migrate?](#do-you-need-to-migrate)
- [Migrate a SQLite Store](#migrate-a-sqlite-store)
- [Keys with a NUL Byte](#keys-with-a-nul-byte)
- [Migrate the Store of a Custom Driver](#migrate-the-store-of-a-custom-driver)

## What Changed

Valkeyrie stores each key as bytes. A string or `Uint8Array` key part ends with a `0x00` byte.

- **Key format 1** (Valkeyrie 0.8 and earlier) writes a `0x00` byte inside a part as it is. Two different keys can then get the same bytes and share one entry. Some keys with a NUL byte can be written, but `get()` and `list()` cannot read them.
- **Key format 2** (Valkeyrie 0.9) writes each `0x00` byte inside a part as `0x00 0xFF`, as the FoundationDB tuple layer and Deno KV do. Each key gets its own bytes, and keys keep their order.

A key without a `0x00` byte in a string or `Uint8Array` part has the same bytes in both formats.

The same bytes can be a different key in each format. For example, both of these keys are stored as `0100ff00`:

```typescript
[new Uint8Array([0, 255])] // in key format 1
[new Uint8Array([0])]      // in key format 2
```

If 0.9 used a 0.8 store as it is, `get()`, `set()` and `delete()` of the new key would read, overwrite or delete the old entry. The bytes cannot tell the two keys apart, so Valkeyrie cannot fix this when it reads a key.

## What 0.9 Does When It Opens a Store

Valkeyrie 0.9 records the key format in each store, in a row that no key can address and `list()` never returns. `Valkeyrie.open()`, `Valkeyrie.from()` and `Valkeyrie.fromAsync()` check this row before they read or write the store:

| The store | What happens |
| --- | --- |
| Is empty | Valkeyrie records key format 2 and opens the store. |
| Has key format 2 | Valkeyrie opens the store. |
| Has entries and no key format row (Valkeyrie 0.8 or earlier wrote it) | Valkeyrie throws `KeyFormatError` with `found: 1` and changes nothing. |
| Has another key format | Valkeyrie throws `KeyFormatError` and changes nothing. |

The check applies to every driver, the built-in SQLite driver and custom drivers.

```typescript
import { KeyFormatError, Valkeyrie } from 'valkeyrie';

try {
  const db = await Valkeyrie.open('./data.db');
} catch (error) {
  if (error instanceof KeyFormatError && error.found === 1) {
    // Migrate the store, then open it again
  }
}
```

## Do You Need to Migrate?

- **An in-memory store, or a store that you create new on each start** (for example a temporary file): no. Valkeyrie records key format 2 when it opens the empty store.
- **A store that you can build again**, such as a cache: no. Delete the store, and Valkeyrie 0.9 creates a new one.
- **Any other store that Valkeyrie 0.8 or earlier wrote**: yes. Follow the steps below.

## Migrate a SQLite Store

The migration is offline. It copies the old store to a new file, and does not change the old store.

1. **Stop every process that uses the store**, the processes that run Valkeyrie 0.8 too. A 0.8 process must never write to a migrated store: it writes keys in key format 1.
2. **Back up the store**: the database file, and its `-wal` and `-shm` files if they exist.
3. **Copy the store with Valkeyrie 0.9:**

   ```typescript
   import { migrateSqliteKeyFormat } from 'valkeyrie/migrate';

   const result = await migrateSqliteKeyFormat('./data.db', './data-0.9.db');
   console.log(result); // { migrated: 1200, rekeyed: 3, skipped: [] }
   ```

   If the store uses another serializer than the default V8 serializer, give the same serializer that you give to `Valkeyrie.open()`:

   ```typescript
   import { jsonSerializer } from 'valkeyrie/serializers/json';

   await migrateSqliteKeyFormat('./data.db', './data-0.9.db', {
     serializer: jsonSerializer,
   });
   ```

4. **Check the new store** before you use it: the number of entries, some keys and values, and the expiry times that your application depends on. Look at the keys of the `rekeyed` rows (see [Keys with a NUL Byte](#keys-with-a-nul-byte)).
5. **Replace the old store** with the new file, or point your application at the new file. Then start your application on Valkeyrie 0.9.

The migration keeps:

- **Each value as its stored bytes**, so every type that your serializer supports stays the same.
- **The versionstamp of each entry**, so `atomic().check()` with an old versionstamp still works.
- **The expiry time of each entry.**

The target must not exist. If the migration fails, it deletes the target, and the old store stays as it was.

## Keys with a NUL Byte

Key format 1 cannot always tell two keys apart, so the migration cannot always find the key that your application wrote. It gives each entry the key that `list()` of Valkeyrie 0.8 returned for it. `rekeyed` is the number of entries whose bytes changed: their key has a `0x00` byte in a string or `Uint8Array` part.

- **Two keys with one entry.** In 0.8, `[new Uint8Array([1, 0, 2, 0x79])]` and `[new Uint8Array([1]), 'y']` read and wrote the same entry. After the migration, the entry has the key `[new Uint8Array([1]), 'y']`. If your application wrote the first key, write the entry again with that key.
- **Overwritten entries.** If two keys shared one entry in 0.8, a write to one key replaced the value of the other key. The migration cannot get back a value that 0.8 replaced.
- **Entries that 0.8 could not read.** Some string parts with a NUL byte, such as `['a\0b']`, were written but `get()` and `list()` of 0.8 threw. The migration stops and lists these rows, and creates no target. To copy the other rows, set `skipUnmigratable: true`. The result then lists the rows that the migration did not copy in `skipped`.

## Migrate the Store of a Custom Driver

A custom driver stores each entry under the hex key hash that Valkeyrie gives it. `migrateKeyHash()` turns a key hash in key format 1 into the key hash of the same key in key format 2.

1. Stop every process that uses the store, and back up the store.
2. Create a new, empty store for your driver. Open it one time with `Valkeyrie.open(driverFn)` to record key format 2, then close it.
3. Copy each entry from the old store to the new store. Give it the key hash `migrateKeyHash(oldKeyHash)`, and keep its value, versionstamp and expiry time.
4. Check the new store, then switch your application to it.

```typescript
import { migrateKeyHash } from 'valkeyrie/migrate';

migrateKeyHash('0100ff00'); // '0100ffff00'
migrateKeyHash('010000'); // '0100ff00'
```

Do not change the key hashes in the old store. As the example shows, the new hash of one entry can be the old hash of another entry. `migrateKeyHash()` throws for a key hash that Valkeyrie 0.8 cannot read; see [Keys with a NUL Byte](#keys-with-a-nul-byte).
