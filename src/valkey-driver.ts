import { AsyncLocalStorage } from 'node:async_hooks'
import { once } from 'node:events'
import { setTimeout } from 'node:timers/promises'
import type { ChainableCommander, Cluster, Redis } from 'ioredis'
import type { Driver, DriverValue } from './driver.ts'
import { v8Serializer } from './serializers/v8.ts'
import type { DriverFactory } from './valkeyrie.ts'

/** Options for {@link valkeyDriver}. */
export interface ValkeyDriverOptions {
  /**
   * An ioredis-compatible client connected to Redis or Valkey. The caller
   * owns it: the driver never closes it. The driver opens one more
   * connection, a duplicate of this client, for atomic commits.
   */
  client: Redis | Cluster
  /**
   * Names the database. Every key lives under `{namespace}:`, so one namespace
   * is one Valkeyrie database and stays on one cluster shard. It must not be
   * empty and must not contain `{` or `}`.
   */
  namespace: string
}

type Client = Redis | Cluster

// A commit in progress: its pending writes by key hash (the entry to store,
// or null to delete), and whether it has watched any key.
interface Transaction {
  writes: Map<string, Buffer | null>
  watched: boolean
}

// An entry is one string: the versionstamp, the expiry time in milliseconds
// (big-endian, 0 for none), then the serialized value.
const VERSIONSTAMP_BYTES = 10
const HEADER_BYTES = VERSIONSTAMP_BYTES + 8

// How often a commit runs again on fresh data after a watched key changed.
const COMMIT_ATTEMPTS = 10

// The most index members `list` reads at once.
const LIST_PAGE_SIZE = 1000

function notImplemented(method: string): Error {
  return new Error(`valkeyDriver: ${method} is not implemented yet`)
}

// Sends a MULTI/EXEC. Returns false when EXEC was aborted because a watched
// key changed.
async function tryExec(transaction: ChainableCommander): Promise<boolean> {
  const results = await transaction.exec()
  if (results === null) {
    return false
  }
  for (const [error] of results) {
    if (error) {
      throw error
    }
  }
  return true
}

async function exec(
  transaction: ChainableCommander,
  abortedMessage: string,
): Promise<void> {
  if (!(await tryExec(transaction))) {
    throw new Error(`valkeyDriver: ${abortedMessage}`)
  }
}

// Runs tasks one at a time, in call order.
function createLock(): <T>(task: () => Promise<T>) => Promise<T> {
  let tail: Promise<unknown> = Promise.resolve()
  return (task) => {
    const result = tail.then(task)
    tail = result.catch(() => {})
    return result
  }
}

// List bounds. Valkeyrie compares hex key strings with bound strings, as the
// SQLite driver does. A bound is a key's hex, optionally followed by one
// character that sorts before every hex digit ('\0': just after that key) or
// after them ('\xff', '\uffff': past that key and every key it starts).
// These helpers turn a bound into the raw-byte bound ZRANGEBYLEX takes, or
// null when no key is on the right side of it.
type BoundPosition = 'at' | 'after' | 'past'

function parseBound(bound: string): {
  bytes: Buffer
  position: BoundPosition
} {
  const hex = /^(?:[0-9a-f]{2})*/.exec(bound)?.[0] ?? ''
  const next = bound.charAt(hex.length)
  const bytes = Buffer.from(hex, 'hex')
  if (next === '') {
    return { bytes, position: 'at' }
  }
  if (next < '0') {
    return { bytes, position: 'after' }
  }
  if (next > 'f') {
    return { bytes, position: 'past' }
  }
  throw new TypeError(
    `valkeyDriver: unsupported list bound ${JSON.stringify(bound)}`,
  )
}

// The smallest byte string above every string that starts with `bytes`, or
// null when there is none.
function successor(bytes: Buffer): Buffer | null {
  let end = bytes.length
  while (end > 0 && bytes[end - 1] === 0xff) {
    end--
  }
  if (end === 0) {
    return null
  }
  const next = Buffer.from(bytes.subarray(0, end))
  next[end - 1] = (next[end - 1] as number) + 1
  return next
}

const inclusive = (bytes: Buffer) => Buffer.concat([Buffer.from('['), bytes])
const exclusive = (bytes: Buffer) => Buffer.concat([Buffer.from('('), bytes])

// The ZRANGEBYLEX bound for keys at or above `bound`.
function lowerBound(bound: string): Buffer | string | null {
  const { bytes, position } = parseBound(bound)
  if (position === 'at') {
    return bytes.length === 0 ? '-' : inclusive(bytes)
  }
  if (position === 'after') {
    return exclusive(bytes)
  }
  const next = successor(bytes)
  return next === null ? null : inclusive(next)
}

// The ZRANGEBYLEX bound for keys below `bound`.
function upperBound(bound: string): Buffer | string | null {
  const { bytes, position } = parseBound(bound)
  if (position === 'at') {
    return bytes.length === 0 ? null : exclusive(bytes)
  }
  if (position === 'after') {
    return inclusive(bytes)
  }
  const next = successor(bytes)
  return next === null ? '+' : exclusive(next)
}

async function connectDuplicate(client: Client): Promise<Client> {
  const connection = client.duplicate()
  try {
    if (connection.status === 'wait') {
      await connection.connect()
    } else if (connection.status !== 'ready') {
      await once(connection, 'ready')
    }
    return connection
  } catch (error) {
    connection.disconnect()
    throw error
  }
}

/**
 * Creates a driver that stores a Valkeyrie database in Redis (6.2 or later) or
 * Valkey, so several processes can share it.
 *
 * @example
 * ```typescript
 * import { Redis } from 'ioredis'
 * import { Valkeyrie } from 'valkeyrie'
 * import { valkeyDriver } from 'valkeyrie/drivers/valkey'
 *
 * const client = new Redis('redis://localhost:6379')
 * const db = await Valkeyrie.open(valkeyDriver({ client, namespace: 'my-app' }))
 * ```
 */
export function valkeyDriver({
  client,
  namespace,
}: ValkeyDriverOptions): DriverFactory {
  if (namespace === '' || /[{}]/.test(namespace)) {
    throw new TypeError(
      `Invalid namespace "${namespace}": it must not be empty or contain "{" or "}"`,
    )
  }
  const prefix = `{${namespace}}:`
  const entryPrefix = Buffer.from(`${prefix}e:`)
  const indexKey = `${prefix}idx`
  const sequenceKey = `${prefix}seq`
  // Valkeyrie passes keys as hex; Redis stores the raw bytes.
  const entryKey = (keyHash: string) =>
    Buffer.concat([entryPrefix, Buffer.from(keyHash, 'hex')])

  // Queues the commands that store `entry` under `keyHash`, or delete it.
  const write = (
    commands: ChainableCommander,
    keyHash: string,
    entry: Buffer | null,
  ): ChainableCommander =>
    entry === null
      ? commands
          .unlink(entryKey(keyHash))
          .zrem(indexKey, Buffer.from(keyHash, 'hex'))
      : commands
          .set(entryKey(keyHash), entry)
          .zadd(indexKey, 0, Buffer.from(keyHash, 'hex'))

  return async (serializerInit) => {
    const serializer = (serializerInit ?? v8Serializer)()
    const transactions = new AsyncLocalStorage<Transaction>()
    const lock = createLock()

    // The connection for atomic commits, opened on first use. WATCH applies
    // to a whole connection, so commits must not share the caller's client.
    let transactionConnection: Promise<Client> | undefined
    const transactionClient = (): Promise<Client> => {
      transactionConnection ??= connectDuplicate(client).catch((error) => {
        transactionConnection = undefined
        throw error
      })
      return transactionConnection
    }

    const decode = (
      keyHash: string,
      entry: Buffer,
      now: number,
    ): DriverValue | undefined => {
      const expiresAt = entry.readBigUInt64BE(VERSIONSTAMP_BYTES)
      if (expiresAt !== 0n && expiresAt <= BigInt(now)) {
        return undefined
      }
      return {
        keyHash,
        value: serializer.deserialize(entry.subarray(HEADER_BYTES)),
        versionstamp: entry.toString('hex', 0, VERSIONSTAMP_BYTES),
      }
    }

    const driver: Driver = {
      get: async (keyHash, now) => {
        const transaction = transactions.getStore()
        if (transaction === undefined) {
          const entry = await client.getBuffer(entryKey(keyHash))
          return entry === null ? undefined : decode(keyHash, entry, now)
        }

        // Inside a commit, a key it already wrote reads as that write.
        const pending = transaction.writes.get(keyHash)
        if (pending !== undefined) {
          return pending === null ? undefined : decode(keyHash, pending, now)
        }
        // Otherwise WATCH the key, so EXEC fails if anyone changes it.
        const connection = await transactionClient()
        const key = entryKey(keyHash)
        transaction.watched = true
        const [, entry] = await Promise.all([
          connection.watch(key),
          connection.getBuffer(key),
        ])
        return entry === null ? undefined : decode(keyHash, entry, now)
      },
      set: async (keyHash, value, versionstamp, expiresAt) => {
        if (expiresAt) {
          throw notImplemented('expiry')
        }
        const header = Buffer.alloc(HEADER_BYTES)
        header.write(versionstamp, 'hex')
        const entry = Buffer.concat([header, serializer.serialize(value)])

        const transaction = transactions.getStore()
        if (transaction !== undefined) {
          transaction.writes.set(keyHash, entry)
          return
        }
        await exec(
          write(client.multi(), keyHash, entry),
          'transaction was aborted',
        )
      },
      delete: async (keyHash) => {
        const transaction = transactions.getStore()
        if (transaction !== undefined) {
          transaction.writes.set(keyHash, null)
          return
        }
        await exec(
          write(client.multi(), keyHash, null),
          'transaction was aborted',
        )
      },
      generateVersionstamp: async () => {
        const sequence = await client.incr(sequenceKey)
        const timestamp = BigInt(Date.now() * 1000) // microseconds
        // Same format as the SQLite driver: the timestamp shifted left by 20
        // bits, OR the low 20 bits of the sequence, as 20 hex characters.
        return ((timestamp << 20n) | (BigInt(sequence) & 0xfffffn))
          .toString(16)
          .padStart(20, '0')
      },
      // Reads inside `callback` WATCH their keys on the commit connection, and
      // writes wait until `callback` resolves, then go out in one MULTI/EXEC.
      // When a watched key changed, EXEC fails and `callback` runs again on
      // fresh data, so a check that no longer holds returns `{ ok: false }`
      // and other commits succeed. After COMMIT_ATTEMPTS the driver throws,
      // and Valkeyrie reports the commit as `{ ok: false }`.
      withTransaction: async <T>(callback: () => Promise<T>): Promise<T> => {
        if (transactions.getStore() !== undefined) {
          return await callback()
        }
        const attempt = async (): Promise<{
          committed: boolean
          result: T
        }> => {
          const transaction: Transaction = { writes: new Map(), watched: false }
          try {
            const result = await transactions.run(transaction, callback)
            if (transaction.writes.size === 0) {
              return { committed: true, result }
            }
            let commands = (await transactionClient()).multi()
            for (const [keyHash, entry] of transaction.writes) {
              commands = write(commands, keyHash, entry)
            }
            return { committed: await tryExec(commands), result }
          } finally {
            if (transaction.watched) {
              // EXEC has already unwatched. An error here must not turn a
              // committed transaction into a failure.
              await (await transactionClient()).unwatch().catch(() => {})
            }
          }
        }
        return await lock(async () => {
          for (let attempts = 1; ; attempts++) {
            const { committed, result } = await attempt()
            if (committed) {
              return result
            }
            if (attempts === COMMIT_ATTEMPTS) {
              throw new Error(
                `valkeyDriver: keys read by the transaction changed ${COMMIT_ATTEMPTS} times`,
              )
            }
            // Back off 5–20 ms, like the SQLite driver on a busy database.
            await setTimeout(5 + Math.random() * 15)
          }
        })
      },
      // Reads the index in key order and fetches the entries with MGET. It
      // skips the prefix key itself and members whose entry has expired or is
      // gone, and reads on until it has `limit` entries or the range ends.
      list: async (
        startHash,
        endHash,
        prefixHash,
        now,
        limit,
        reverse = false,
      ) => {
        const results: DriverValue[] = []
        const min = lowerBound(startHash)
        const max = upperBound(endHash)
        if (min === null || max === null) {
          return results
        }
        // The bound that moves past each page read.
        let from = reverse ? max : min
        const to = reverse ? min : max
        while (results.length < limit) {
          const count = Math.min(limit - results.length, LIST_PAGE_SIZE)
          const members = reverse
            ? await client.zrevrangebylexBuffer(
                indexKey,
                from,
                to,
                'LIMIT',
                0,
                count,
              )
            : await client.zrangebylexBuffer(
                indexKey,
                from,
                to,
                'LIMIT',
                0,
                count,
              )
          const keyHashes = members
            .map((member) => member.toString('hex'))
            .filter((keyHash) => keyHash !== prefixHash)
          const entries =
            keyHashes.length === 0
              ? []
              : await client.mgetBuffer(...keyHashes.map(entryKey))
          for (const [index, keyHash] of keyHashes.entries()) {
            const entry = entries[index]
            const value = entry ? decode(keyHash, entry, now) : undefined
            if (value !== undefined) {
              results.push(value)
            }
          }
          const last = members.at(-1)
          if (members.length < count || last === undefined) {
            break
          }
          from = exclusive(last)
        }
        return results
      },
      // Nothing can expire until expiry is implemented.
      cleanup: async () => {},
      watch: () => {
        throw notImplemented('watch')
      },
      clear: async () => {
        throw notImplemented('clear')
      },
      destroy: async () => {
        throw notImplemented('destroy')
      },
      // Waits for a running commit, then closes the commit connection.
      close: () =>
        lock(async () => {
          const connection = await transactionConnection?.catch(() => undefined)
          transactionConnection = undefined
          await connection?.quit()
        }),
    }
    return driver
  }
}
