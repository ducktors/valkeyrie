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

type WatchEvent = (
  | DriverValue
  | { keyHash: string; value: null; versionstamp: null }
)[]

interface Watcher {
  keyHashes: string[]
  controller: ReadableStreamDefaultController<WatchEvent>
}

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

// The most index members `list`, `cleanup` and `clear` handle at once.
const PAGE_SIZE = 1000

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
  const expiryKey = `${prefix}exp`
  const sequenceKey = `${prefix}seq`
  // Valkeyrie passes keys as hex; Redis stores the raw bytes.
  const entryKeyOf = (member: Buffer) => Buffer.concat([entryPrefix, member])
  const entryKey = (keyHash: string) => entryKeyOf(Buffer.from(keyHash, 'hex'))

  // Queues the commands that store `entry` under `keyHash`, or delete it. An
  // entry with an expiry time keeps that time as its score in the expiry
  // index, and its Redis TTL is the time left on this process's clock, so a
  // server clock that runs ahead does not expire it early. An entry that has
  // already expired is deleted. An entry without an expiry time has neither.
  const write = (
    commands: ChainableCommander,
    keyHash: string,
    entry: Buffer | null,
  ): ChainableCommander => {
    const member = Buffer.from(keyHash, 'hex')
    const deleteEntry = () =>
      commands
        .unlink(entryKeyOf(member))
        .zrem(indexKey, member)
        .zrem(expiryKey, member)
    if (entry === null) {
      return deleteEntry()
    }
    const expiresAt = Number(entry.readBigUInt64BE(VERSIONSTAMP_BYTES))
    if (expiresAt === 0) {
      return commands
        .set(entryKeyOf(member), entry)
        .zadd(indexKey, 0, member)
        .zrem(expiryKey, member)
    }
    const timeToLive = expiresAt - Date.now()
    if (timeToLive <= 0) {
      return deleteEntry()
    }
    return commands
      .set(entryKeyOf(member), entry, 'PX', timeToLive)
      .zadd(indexKey, 0, member)
      .zadd(expiryKey, expiresAt, member)
  }

  return async (serializerInit) => {
    const serializer = (serializerInit ?? v8Serializer)()
    const transactions = new AsyncLocalStorage<Transaction>()
    const lock = createLock()

    // The connection for atomic commits, opened on first use. WATCH applies
    // to a whole connection, so commits must not share the caller's client.
    let transactionConnection: Promise<Client> | undefined
    // Set by `close`: from then on the driver opens no connection.
    let closed = false
    const transactionClient = (): Promise<Client> => {
      if (closed) {
        return Promise.reject(new Error('valkeyDriver: the driver is closed'))
      }
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

    // Watchers see writes made through this driver instance only.
    const watchers = new Set<Watcher>()
    const emit = async (watcher: Watcher) => {
      try {
        const now = Date.now()
        const entries = await client.mgetBuffer(
          ...watcher.keyHashes.map(entryKey),
        )
        const event: WatchEvent = watcher.keyHashes.map((keyHash, index) => {
          const entry = entries[index]
          return (
            (entry ? decode(keyHash, entry, now) : undefined) ?? {
              keyHash,
              value: null,
              versionstamp: null,
            }
          )
        })
        if (watchers.has(watcher)) {
          watcher.controller.enqueue(event)
        }
      } catch (error) {
        if (watchers.delete(watcher)) {
          watcher.controller.error(error)
        }
      }
    }
    // Sends the current values to the watchers of the changed keys, or to all
    // watchers when the changed keys are unknown.
    const notify = async (keyHashes?: Iterable<string>) => {
      if (watchers.size === 0) {
        return
      }
      const changed = keyHashes === undefined ? undefined : new Set(keyHashes)
      await Promise.all(
        [...watchers]
          .filter(
            (watcher) =>
              changed === undefined ||
              watcher.keyHashes.some((keyHash) => changed.has(keyHash)),
          )
          .map(emit),
      )
    }

    // Deletes the expired entries listed in the expiry index, one page at a
    // time. A page's entry keys are WATCHed and the index is read again, so
    // a key set again in the meantime is left alone. A page whose EXEC fails
    // is read again, up to COMMIT_ATTEMPTS times: on Redis 6.2, Redis
    // expiring a watched key also fails EXEC. Returns whether it removed
    // anything.
    const removeExpired = async (now: number): Promise<boolean> => {
      let removed = false
      let attempts = 0
      while (!closed) {
        const candidates = await client.zrangebyscoreBuffer(
          expiryKey,
          '-inf',
          now,
          'LIMIT',
          0,
          PAGE_SIZE,
        )
        if (candidates.length === 0) {
          return removed
        }
        const committed = await lock(async () => {
          if (closed) {
            return undefined
          }
          const connection = await transactionClient()
          try {
            await connection.watch(...candidates.map(entryKeyOf))
            const stillExpired = new Set(
              (
                await connection.zrangebyscoreBuffer(
                  expiryKey,
                  '-inf',
                  now,
                  'LIMIT',
                  0,
                  PAGE_SIZE,
                )
              ).map((member) => member.toString('hex')),
            )
            const members = candidates.filter((member) =>
              stillExpired.has(member.toString('hex')),
            )
            if (members.length === 0) {
              return false
            }
            return await tryExec(
              connection
                .multi()
                .unlink(...members.map(entryKeyOf))
                .zrem(indexKey, ...members)
                .zrem(expiryKey, ...members),
            )
          } finally {
            await connection.unwatch().catch(() => {})
          }
        })
        if (committed === undefined) {
          return removed
        }
        if (!committed) {
          // A page that keeps losing races with writers waits for the next run.
          if (++attempts === COMMIT_ATTEMPTS) {
            return removed
          }
          await setTimeout(5 + Math.random() * 15)
          continue
        }
        attempts = 0
        removed = true
        if (candidates.length < PAGE_SIZE) {
          return removed
        }
      }
      return removed
    }

    // Deletes every entry listed in the index, one page at a time. Redis
    // deletes each index once it is empty.
    const clear = async () => {
      while (true) {
        const members = await client.zrangebylexBuffer(
          indexKey,
          '-',
          '+',
          'LIMIT',
          0,
          PAGE_SIZE,
        )
        if (members.length > 0) {
          await exec(
            client
              .multi()
              .unlink(...members.map(entryKeyOf))
              .zrem(indexKey, ...members)
              .zrem(expiryKey, ...members),
            'transaction was aborted',
          )
        }
        if (members.length < PAGE_SIZE) {
          return
        }
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
        const header = Buffer.alloc(HEADER_BYTES)
        header.write(versionstamp, 'hex')
        // Redis takes whole milliseconds; a fractional time rounds up.
        header.writeBigUInt64BE(
          BigInt(Math.ceil(expiresAt || 0)),
          VERSIONSTAMP_BYTES,
        )
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
        await notify([keyHash])
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
        await notify([keyHash])
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
          written: string[]
        }> => {
          const transaction: Transaction = { writes: new Map(), watched: false }
          try {
            const result = await transactions.run(transaction, callback)
            const written = [...transaction.writes.keys()]
            if (written.length === 0) {
              return { committed: true, result, written }
            }
            let commands = (await transactionClient()).multi()
            for (const [keyHash, entry] of transaction.writes) {
              commands = write(commands, keyHash, entry)
            }
            return { committed: await tryExec(commands), result, written }
          } finally {
            if (transaction.watched) {
              // EXEC has already unwatched. An error here must not turn a
              // committed transaction into a failure.
              await (await transactionClient()).unwatch().catch(() => {})
            }
          }
        }
        const { result, written } = await lock(async () => {
          for (let attempts = 1; ; attempts++) {
            const outcome = await attempt()
            if (outcome.committed) {
              return outcome
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
        await notify(written)
        return result
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
          const count = Math.min(limit - results.length, PAGE_SIZE)
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
      // Redis drops expired entries by itself; this also removes them from
      // both indexes. Several processes can run it at once.
      cleanup: async (now) => {
        if (await removeExpired(now)) {
          await notify()
        }
      },
      // Emits the current values, then the new values each time a write
      // through this driver instance changes a watched key.
      watch: (keyHashes) => {
        let watcher: Watcher | undefined
        return new ReadableStream<WatchEvent>({
          start: (controller) => {
            watcher = { keyHashes, controller }
            watchers.add(watcher)
            void emit(watcher)
          },
          cancel: () => {
            if (watcher) {
              watchers.delete(watcher)
            }
          },
        })
      },
      // Keeps the versionstamp sequence, like the SQLite driver.
      clear: async () => {
        await clear()
        await notify()
      },
      destroy: async () => {
        await clear()
        await client.unlink(sequenceKey)
        await notify()
      },
      // Ends the watches, waits for a running commit, then closes the commit
      // connection. The caller's client stays open.
      close: async () => {
        closed = true
        for (const watcher of watchers) {
          try {
            watcher.controller.close()
          } catch {
            // Already closed or cancelled.
          }
        }
        watchers.clear()
        await lock(async () => {
          const connection = await transactionConnection?.catch(() => undefined)
          transactionConnection = undefined
          connection?.disconnect()
        })
      },
    }
    return driver
  }
}
