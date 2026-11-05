import type { ChainableCommander, Cluster, Redis } from 'ioredis'
import type { Driver } from './driver.ts'
import { v8Serializer } from './serializers/v8.ts'
import type { DriverFactory } from './valkeyrie.ts'

/** Options for {@link valkeyDriver}. */
export interface ValkeyDriverOptions {
  /**
   * An ioredis-compatible client connected to Redis or Valkey. The caller
   * owns it: the driver never closes it.
   */
  client: Redis | Cluster
  /**
   * Names the database. Every key lives under `{namespace}:`, so one namespace
   * is one Valkeyrie database and stays on one cluster shard. It must not be
   * empty and must not contain `{` or `}`.
   */
  namespace: string
}

// An entry is one string: the versionstamp, the expiry time in milliseconds
// (big-endian, 0 for none), then the serialized value.
const VERSIONSTAMP_BYTES = 10
const HEADER_BYTES = VERSIONSTAMP_BYTES + 8

function notImplemented(method: string): Error {
  return new Error(`valkeyDriver: ${method} is not implemented yet`)
}

async function exec(transaction: ChainableCommander): Promise<void> {
  const results = await transaction.exec()
  if (results === null) {
    throw new Error('valkeyDriver: transaction was aborted')
  }
  for (const [error] of results) {
    if (error) {
      throw error
    }
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

  return async (serializerInit) => {
    const serializer = (serializerInit ?? v8Serializer)()

    const driver: Driver = {
      get: async (keyHash, now) => {
        const entry = await client.getBuffer(entryKey(keyHash))
        if (entry === null) {
          return undefined
        }
        const expiresAt = entry.readBigUInt64BE(VERSIONSTAMP_BYTES)
        if (expiresAt !== 0n && expiresAt <= BigInt(now)) {
          return undefined
        }
        return {
          keyHash,
          value: serializer.deserialize(entry.subarray(HEADER_BYTES)),
          versionstamp: entry.toString('hex', 0, VERSIONSTAMP_BYTES),
        }
      },
      set: async (keyHash, value, versionstamp, expiresAt) => {
        if (expiresAt) {
          throw notImplemented('expiry')
        }
        const header = Buffer.alloc(HEADER_BYTES)
        header.write(versionstamp, 'hex')
        const entry = Buffer.concat([header, serializer.serialize(value)])
        await exec(
          client
            .multi()
            .set(entryKey(keyHash), entry)
            .zadd(indexKey, 0, Buffer.from(keyHash, 'hex')),
        )
      },
      delete: async (keyHash) => {
        await exec(
          client
            .multi()
            .unlink(entryKey(keyHash))
            .zrem(indexKey, Buffer.from(keyHash, 'hex')),
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
      list: async () => {
        throw notImplemented('list')
      },
      withTransaction: async () => {
        throw notImplemented('withTransaction')
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
      // The driver owns no connection yet.
      close: async () => {},
    }
    return driver
  }
}
