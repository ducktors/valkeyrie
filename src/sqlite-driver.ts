import { DatabaseSync } from 'node:sqlite'
import { type DriverValue, defineDriver } from './driver.js'
import type { Serializer } from './serializers/serializer.js'
import { v8Serializer } from './serializers/v8.js'

type SqlTable = Pick<DriverValue, 'versionstamp'> & {
  key_hash: string
  value: Uint8Array
  is_u64: number
}

export const sqliteDriver = defineDriver(
  async (path = ':memory:', customSerializer?: () => Promise<Serializer>) => {
    const db = new DatabaseSync(path)
    // Enable WAL mode for better performance
    db.exec('PRAGMA journal_mode = WAL')

    // Create the KV table with versioning and expiry support
    db.exec(`
    CREATE TABLE IF NOT EXISTS kv_store (
      key_hash TEXT PRIMARY KEY,
      value BLOB,
      versionstamp TEXT NOT NULL,
      expires_at INTEGER,
      is_u64 INTEGER DEFAULT 0
    );

    CREATE INDEX IF NOT EXISTS idx_kv_store_expires_at
    ON kv_store(expires_at)
    WHERE expires_at IS NOT NULL;

    CREATE INDEX IF NOT EXISTS idx_kv_store_key_hash
    ON kv_store(key_hash);
  `)

    const statements = {
      get: db.prepare(
        'SELECT key_hash, value, versionstamp, is_u64 FROM kv_store WHERE key_hash = ? AND (expires_at IS NULL OR expires_at > ?)',
      ),
      set: db.prepare(
        'INSERT OR REPLACE INTO kv_store (key_hash, value, versionstamp, is_u64) VALUES (?, ?, ?, ?)',
      ),
      setWithExpiry: db.prepare(
        'INSERT OR REPLACE INTO kv_store (key_hash, value, versionstamp, expires_at, is_u64) VALUES (?, ?, ?, ?, ?)',
      ),
      delete: db.prepare('DELETE FROM kv_store WHERE key_hash = ?'),
      list: db.prepare(
        'SELECT key_hash, value, versionstamp, is_u64 FROM kv_store WHERE key_hash >= ? AND key_hash < ? AND key_hash != ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY key_hash ASC LIMIT ?',
      ),
      listReverse: db.prepare(
        'SELECT key_hash, value, versionstamp, is_u64 FROM kv_store WHERE key_hash >= ? AND key_hash < ? AND key_hash != ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY key_hash DESC LIMIT ?',
      ),
      cleanup: db.prepare('DELETE FROM kv_store WHERE expires_at <= ?'),
    }

    // Use the provided serializer or default to v8Serializer
    const serializer = await (customSerializer
      ? customSerializer()
      : v8Serializer())

    return {
      close: async () => {
        db.close()
      },
      get: async (keyHash: string, now: number) => {
        const result = (await statements.get.get(keyHash, now)) as
          | SqlTable
          | undefined
        if (!result) {
          return undefined
        }
        return {
          keyHash: result.key_hash,
          value: serializer.deserialize(result.value, result.is_u64),
          versionstamp: result.versionstamp,
        }
      },
      set: async (key, value, versionstamp, expiresAt) => {
        const { serialized, isU64 } = serializer.serialize(value)
        if (expiresAt) {
          statements.setWithExpiry.run(
            key,
            serialized,
            versionstamp,
            expiresAt,
            isU64,
          )
        } else {
          statements.set.run(key, serialized, versionstamp, isU64)
        }
      },
      delete: async (keyHash) => {
        statements.delete.run(keyHash)
      },
      list: async (
        startHash,
        endHash,
        prefixHash,
        now,
        limit,
        reverse = false,
      ) => {
        return (
          (reverse
            ? statements.listReverse.all(
                startHash,
                endHash,
                prefixHash,
                now,
                limit,
              )
            : statements.list.all(
                startHash,
                endHash,
                prefixHash,
                now,
                limit,
              )) as SqlTable[]
        ).map((r) => ({
          keyHash: r.key_hash,
          value: serializer.deserialize(r.value, r.is_u64),
          versionstamp: r.versionstamp,
        }))
      },
      cleanup: async (now) => {
        statements.cleanup.run(now)
      },
      withTransaction: async <T>(callback: () => Promise<T>): Promise<T> => {
        db.exec('BEGIN TRANSACTION')
        try {
          const result = await callback()
          db.exec('COMMIT')
          return result
        } catch (error) {
          db.exec('ROLLBACK')
          throw error
        }
      },
    }
  },
)
