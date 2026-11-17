import { unlink } from 'node:fs/promises'
import { DatabaseSync, type StatementSync } from 'node:sqlite'
import { setTimeout } from 'node:timers/promises'
import { type DriverValue, defineDriver } from './driver.ts'
import type { Serializer } from './serializers/serializer.ts'
import { v8Serializer } from './serializers/v8.ts'

type SqlRow = Pick<DriverValue, 'versionstamp'> & {
  key_hash: string
  value: Uint8Array
  is_u64: number
}

// Helper function to generate database-level versionstamps
async function generateVersionstamp(
  db: DatabaseSync,
  versionstampStatement: StatementSync,
  transactionState: { inTransaction: boolean },
): Promise<string> {
  try {
    const result = versionstampStatement.get() as { sequence: number }
    const timestamp = BigInt(Date.now() * 1000) // microseconds
    // Combine timestamp and sequence into a single 20-character hex string:
    // - Shift timestamp left by 20 bits (makes room for sequence in lower bits)
    // - Mask sequence to 20 bits (0xFFFFF = 1048575 max) to prevent overflow
    // - OR them together: [timestamp bits][sequence bits]
    // This ensures uniqueness (sequence) and ordering (timestamp prefix)
    const combined = (timestamp << 20n) | BigInt(result.sequence & 0xfffff)
    return combined.toString(16).padStart(20, '0')
  } catch (error: unknown) {
    // If we're outside a transaction and get a lock error, use a transaction with retry logic
    const sqliteError = error as SqliteError
    if (
      sqliteError?.code === 'SQLITE_BUSY' ||
      sqliteError?.message?.includes('database is locked')
    ) {
      // Retry logic similar to withTransaction
      while (true) {
        try {
          db.exec('BEGIN IMMEDIATE TRANSACTION')
          transactionState.inTransaction = true
          const result = versionstampStatement.get() as {
            sequence: number
          }
          db.exec('COMMIT')
          transactionState.inTransaction = false
          const timestamp = BigInt(Date.now() * 1000) // microseconds
          // Same bit manipulation as above: combine timestamp and sequence
          const combined =
            (timestamp << 20n) | BigInt(result.sequence & 0xfffff)
          return combined.toString(16).padStart(20, '0')
        } catch (txError: unknown) {
          try {
            if (transactionState.inTransaction) {
              db.exec('ROLLBACK')
              transactionState.inTransaction = false
            }
          } catch (rollbackError) {
            // Ignore rollback errors if transaction is already rolled back
          }

          const txSqliteError = txError as SqliteError
          if (
            txSqliteError?.code === 'SQLITE_BUSY' ||
            txSqliteError?.message?.includes('database is locked')
          ) {
            // Random backoff between 5-20ms
            const backoff = 5 + Math.random() * 15
            await setTimeout(backoff)
            continue
          }
          throw txError
        }
      }
    }
    throw error
  }
}

interface SqliteError extends Error {
  code?: string
  message: string
}

const MEMORY_PATH = ':memory:'

export const sqliteDriver = defineDriver(
  async (path = MEMORY_PATH, customSerializer?: () => Serializer) => {
    const db = new DatabaseSync(path)
    const transactionState = { inTransaction: false }

    db.exec(`
    PRAGMA synchronous = NORMAL;
    PRAGMA journal_mode = WAL;
    `)

    // Create the KV table with versioning and expiry support
    db.exec(`
    CREATE TABLE IF NOT EXISTS kv_store (
      key_hash TEXT PRIMARY KEY,
      value BLOB,
      versionstamp TEXT NOT NULL,
      expires_at INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_kv_store_expires_at
    ON kv_store(expires_at)
    WHERE expires_at IS NOT NULL;

    CREATE INDEX IF NOT EXISTS idx_kv_store_key_hash
    ON kv_store(key_hash);

    CREATE TABLE IF NOT EXISTS versionstamp_sequence (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      sequence INTEGER NOT NULL DEFAULT 0
    );

    INSERT OR IGNORE INTO versionstamp_sequence (id, sequence) VALUES (1, 0);
    `)

    const statements = {
      get: db.prepare(
        'SELECT key_hash, value, versionstamp FROM kv_store WHERE key_hash = ? AND (expires_at IS NULL OR expires_at > ?)',
      ),
      set: db.prepare(
        'INSERT OR REPLACE INTO kv_store (key_hash, value, versionstamp) VALUES (?, ?, ?)',
      ),
      setWithExpiry: db.prepare(
        'INSERT OR REPLACE INTO kv_store (key_hash, value, versionstamp, expires_at) VALUES (?, ?, ?, ?)',
      ),
      delete: db.prepare('DELETE FROM kv_store WHERE key_hash = ?'),
      list: db.prepare(
        'SELECT key_hash, value, versionstamp FROM kv_store WHERE key_hash >= ? AND key_hash < ? AND key_hash != ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY key_hash ASC LIMIT ?',
      ),
      listReverse: db.prepare(
        'SELECT key_hash, value, versionstamp FROM kv_store WHERE key_hash >= ? AND key_hash < ? AND key_hash != ? AND (expires_at IS NULL OR expires_at > ?) ORDER BY key_hash DESC LIMIT ?',
      ),
      cleanup: db.prepare('DELETE FROM kv_store WHERE expires_at <= ?'),
      clear: db.prepare('DELETE FROM kv_store'),
      generateVersionstamp: db.prepare(
        'UPDATE versionstamp_sequence SET sequence = sequence + 1 WHERE id = 1 RETURNING sequence',
      ),
    }

    // Use the provided serializer or default to v8Serializer
    const serializer = await (customSerializer
      ? customSerializer()
      : v8Serializer())

    // Global watch queue to store watchers
    const watchQueue: Array<{
      keyHashes: string[]
      controller: ReadableStreamDefaultController<
        (DriverValue | { keyHash: string; value: null; versionstamp: null })[]
      >
    }> = []

    /**
     * Publishes committed values. A transaction publishes once after commit,
     * so its intermediate writes and rolled-back changes stay out of the stream.
     * A watcher that cannot read its values gets the error on its stream and
     * leaves the queue. The write has already happened, so the error does not
     * reach the writer or the other watchers.
     *
     * @returns Resolves after the current values are queued for watchers.
     */
    async function notifyWatchers() {
      if (transactionState.inTransaction) return

      const now = Date.now()
      for (const watcher of [...watchQueue]) {
        try {
          const results = await Promise.all(
            watcher.keyHashes.map(async (keyHash) => {
              const result = statements.get.get(keyHash, now) as
                | SqlRow
                | undefined
              if (!result) {
                return {
                  keyHash,
                  value: null,
                  versionstamp: null,
                }
              }
              return {
                keyHash: result.key_hash,
                value: serializer.deserialize(result.value),
                versionstamp: result.versionstamp,
              }
            }),
          )
          watcher.controller.enqueue(results)
        } catch (error) {
          const index = watchQueue.indexOf(watcher)
          if (index !== -1) watchQueue.splice(index, 1)
          watcher.controller.error(error)
        }
      }
    }

    return {
      close: async () => {
        // Cancel all watchers
        for (const watcher of watchQueue) {
          try {
            watcher.controller.close()
          } catch (error: unknown) {
            // Ignore errors from already closed controllers
            if (
              error &&
              typeof error === 'object' &&
              'code' in error &&
              error.code === 'ERR_INVALID_STATE'
            ) {
              continue
            }
            throw error
          }
        }
        watchQueue.length = 0
        db.close()
      },
      destroy: async () => {
        if (path !== MEMORY_PATH) {
          await Promise.allSettled([
            unlink(path),
            unlink(`${path}-shm`),
            unlink(`${path}-wal`),
          ])
        } else {
          statements.clear.run()
          await notifyWatchers()
        }
      },
      clear: async () => {
        statements.clear.run()
        await notifyWatchers()
      },
      get: async (keyHash: string, now: number) => {
        const result = statements.get.get(keyHash, now) as SqlRow | undefined
        if (!result) {
          return undefined
        }
        return {
          keyHash: result.key_hash,
          value: serializer.deserialize(result.value),
          versionstamp: result.versionstamp,
        }
      },
      set: async (key, value, versionstamp, expiresAt) => {
        const serialized = serializer.serialize(value)
        if (expiresAt) {
          statements.setWithExpiry.run(key, serialized, versionstamp, expiresAt)
        } else {
          statements.set.run(key, serialized, versionstamp)
        }
        await notifyWatchers()
      },
      delete: async (keyHash) => {
        statements.delete.run(keyHash)
        await notifyWatchers()
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
              )) as SqlRow[]
        ).map((r) => ({
          keyHash: r.key_hash,
          value: serializer.deserialize(r.value),
          versionstamp: r.versionstamp,
        }))
      },
      cleanup: async (now) => {
        statements.cleanup.run(now)
        await notifyWatchers()
      },
      generateVersionstamp: () =>
        generateVersionstamp(
          db,
          statements.generateVersionstamp,
          transactionState,
        ),
      withTransaction: async <T>(callback: () => Promise<T>): Promise<T> => {
        if (transactionState.inTransaction) {
          // If already in transaction, just run the callback
          return await callback()
        }

        while (true) {
          try {
            db.exec('BEGIN IMMEDIATE TRANSACTION')
            transactionState.inTransaction = true
            const result = await callback()
            db.exec('COMMIT')
            transactionState.inTransaction = false
            await notifyWatchers()
            return result
          } catch (error: unknown) {
            try {
              if (transactionState.inTransaction) {
                db.exec('ROLLBACK')
                transactionState.inTransaction = false
              }
            } catch (rollbackError) {
              // Ignore rollback errors if transaction is already rolled back
            }
            // Check if the error is a SQLite busy error
            const sqliteError = error as SqliteError
            if (
              sqliteError?.code === 'SQLITE_BUSY' ||
              sqliteError?.message?.includes('database is locked')
            ) {
              // Random backoff between 5-20ms
              const backoff = 5 + Math.random() * 15
              await setTimeout(backoff)
              continue
            }
            throw error
          }
        }
      },
      watch: (keyHashes: string[]) => {
        let watchController:
          | ReadableStreamDefaultController<
              (
                | DriverValue
                | { keyHash: string; value: null; versionstamp: null }
              )[]
            >
          | undefined
        return new ReadableStream({
          start(controller) {
            watchController = controller
            watchQueue.push({ keyHashes, controller })
            // Send initial values
            const now = Date.now()
            Promise.all(
              keyHashes.map(async (keyHash) => {
                const result = statements.get.get(keyHash, now) as
                  | SqlRow
                  | undefined
                if (!result) {
                  return {
                    keyHash,
                    value: null,
                    versionstamp: null,
                  }
                }
                return {
                  keyHash: result.key_hash,
                  value: serializer.deserialize(result.value),
                  versionstamp: result.versionstamp,
                }
              }),
            ).then((results) => {
              controller.enqueue(results)
            })
          },
          cancel() {
            // cancel() receives a reason parameter, not the controller
            // The controller should be accessed from the closure
            // Note: Don't call controller.close() here - the stream infrastructure handles that
            if (watchController) {
              const index = watchQueue.findIndex(
                (w) => w.controller === watchController,
              )
              if (index !== -1) {
                watchQueue.splice(index, 1)
              }
            }
          },
        })
      },
    }
  },
)
