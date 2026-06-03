import { serialize } from 'node:v8'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Driver } from './driver.ts'
import { KvU64 } from './kv-u64.ts'
import type { SchemaRegistry } from './schema-registry.ts'
import { validateReservedKeyParts, validateValue } from './schema-validator.ts'
import type { Serializer } from './serializers/serializer.ts'
import { sqliteDriver } from './sqlite-driver.ts'
import {
  kCommitVersionstamp,
  kFrom,
  kFromAsync,
  kOpen,
  kSchemaRegistry,
  kValkeyrie,
} from './symbols.ts'
import type {
  InferTypeForKey,
  InferTypeForPrefix,
  SchemaRegistry as SchemaRegistryType,
} from './types/schema-registry-types.ts'
import { ValkeyrieBuilder } from './valkeyrie-builder.ts'

export type KeyPart = Uint8Array | string | number | bigint | boolean | symbol
export type Key = readonly KeyPart[]

interface AtomicCheck {
  key: Key
  versionstamp: string | null
}

type ListSelector =
  | { prefix: Key }
  | { prefix: Key; start: Key }
  | { prefix: Key; end: Key }
  | { start: Key; end: Key }

interface ListOptions {
  limit?: number
  cursor?: string
  reverse?: boolean
  consistency?: 'strong' | 'eventual'
  batchSize?: number
}

interface SetOptions {
  expireIn?: number
}

/**
 * Options for creating a Valkeyrie database from an iterable.
 */
export interface FromOptions<T> {
  /** Prefix for all keys */
  prefix: Key
  /** Property name or function to extract the key part from each item */
  keyProperty: keyof T | ((item: T) => KeyPart)
  /** Optional path to the database file (defaults to in-memory if neither path nor driverFn are given) */
  path?: string
  /** Optional function to provide a driver; takes precedence over path */
  driverFn?: (serializer?: () => Serializer) => Promise<Driver>
  /** Optional custom serializer */
  serializer?: () => Serializer
  /** Optional destroyOnClose flag (default: false) */
  destroyOnClose?: boolean
  /** Optional TTL for all entries (milliseconds) */
  expireIn?: number
  /** Optional progress callback */
  onProgress?: (processed: number, total?: number) => void
  /** Optional error handling strategy (default: 'stop') */
  onError?: 'stop' | 'continue'
  /** Optional callback for errors when onError is 'continue' */
  onErrorCallback?: (error: Error, item: T) => void
}
export interface Check {
  key: Key
  versionstamp: string | null
}

export type Mutation<T = unknown> = { key: Key } & (
  | { type: 'set'; value: T; expireIn?: number }
  | { type: 'delete' }
  | { type: 'sum'; value: KvU64 }
  | { type: 'max'; value: KvU64 }
  | { type: 'min'; value: KvU64 }
)

interface Entry<T = unknown> {
  key: Key
  value: T
  versionstamp: string
}
export type EntryMaybe<T = unknown> =
  | Entry<T>
  | {
      key: Key
      value: null
      versionstamp: null
    }

/**
 * Valkeyrie database instance with optional schema registry type tracking.
 *
 * @template TRegistry - Compile-time schema registry for automatic type inference
 */
export class Valkeyrie<TRegistry extends SchemaRegistryType = readonly []> {
  #driver: Driver
  #isClosed = false
  #destroyOnClose = false
  readonly #schemaRegistry?: SchemaRegistry

  private constructor(
    functions: Driver,
    options: { destroyOnClose: boolean; schemaRegistry?: SchemaRegistry },
    symbol?: symbol,
  ) {
    if (kValkeyrie !== symbol) {
      throw new TypeError(
        'Valkeyrie can not be constructed: use Valkeyrie.open() to create a new instance',
      )
    }
    this.#driver = functions
    this.#destroyOnClose = options.destroyOnClose

    if (options.schemaRegistry !== undefined) {
      this.#schemaRegistry = options.schemaRegistry
    }
  }

  commitVersionstamp(): symbol {
    return kCommitVersionstamp
  }

  /**
   * Creates a builder for registering schemas before opening the database.
   * Uses `const` type parameter to automatically infer literal types without `as const`.
   *
   * @param pattern Key pattern with optional '*' wildcards
   * @param schema Standard schema for validation
   * @returns A builder instance for chaining with type tracking
   */
  public static withSchema<
    const TPattern extends Key,
    TSchema extends StandardSchemaV1,
  >(pattern: TPattern, schema: TSchema) {
    return new ValkeyrieBuilder().withSchema(pattern, schema)
  }

  /**
   * Opens a new Valkeyrie database instance
   * @param path Optional path to the database file (defaults to in-memory)
   * @param options Optional configuration options
   * @returns A new Valkeyrie instance
   */
  public static async open(
    path?: string,
    options: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    } = {},
  ): Promise<Valkeyrie> {
    return Valkeyrie.openWithDriver(
      (serializer?: () => Serializer) => sqliteDriver(path, serializer),
      options,
    )
  }

  /**
   * Opens a new Valkeyrie database instance backed by a custom driver.
   *
   * Use this instead of {@link Valkeyrie.open} when you want to supply your own
   * storage backend. The driver function receives the configured serializer
   * (if any) and must resolve to a {@link Driver}. For the built-in SQLite
   * backend, prefer {@link Valkeyrie.open}.
   *
   * @example
   * ```typescript
   * // `createMyDriver` returns an object implementing the Driver interface
   * const db = await Valkeyrie.openWithDriver(
   *   async (serializer) => createMyDriver(serializer),
   * )
   * ```
   *
   * @param driverFn Function that creates the driver, optionally using the serializer
   * @param options Optional configuration options
   * @returns A new Valkeyrie instance
   */
  public static async openWithDriver(
    driverFn: (serializer?: () => Serializer) => Promise<Driver>,
    options: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    } = {},
  ): Promise<Valkeyrie> {
    return Valkeyrie[kOpen](driverFn, options, undefined)
  }

  /**
   * Internal method to open a database with schemas.
   * Used by ValkeyrieBuilder.
   */
  static async [kOpen](
    driverFn: (serializer?: () => Serializer) => Promise<Driver>,
    options: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    } = {},
    schemaRegistry?: SchemaRegistry,
  ): Promise<Valkeyrie> {
    const destroyOnClose = options.destroyOnClose ?? false
    const constructorOptions: {
      destroyOnClose: boolean
      schemaRegistry?: SchemaRegistry
    } = {
      destroyOnClose,
    }
    if (schemaRegistry !== undefined) {
      constructorOptions.schemaRegistry = schemaRegistry
    }
    const db = new Valkeyrie(
      await driverFn(options.serializer),
      constructorOptions,
      kValkeyrie,
    )
    await db.cleanup()
    return db
  }

  /**
   * Helper function to extract key part from an item
   */
  private static extractKeyPart<T>(
    item: T,
    keyProperty: keyof T | ((item: T) => KeyPart),
  ): KeyPart {
    if (typeof keyProperty === 'function') {
      return keyProperty(item)
    }
    const value = item[keyProperty]
    // Validate that the extracted value is a valid KeyPart
    if (
      value instanceof Uint8Array ||
      typeof value === 'string' ||
      typeof value === 'number' ||
      typeof value === 'bigint' ||
      typeof value === 'boolean' ||
      typeof value === 'symbol'
    ) {
      return value
    }
    throw new TypeError(
      `Key property '${String(keyProperty)}' must be a valid KeyPart (Uint8Array, string, number, bigint, boolean, or symbol)`,
    )
  }

  /**
   * Creates and populates a Valkeyrie database from a synchronous iterable.
   *
   * @example
   * ```typescript
   * const users = [
   *   { id: 1, name: 'Alice' },
   *   { id: 2, name: 'Bob' }
   * ]
   * const db = await Valkeyrie.from(users, {
   *   prefix: ['users'],
   *   keyProperty: 'id'
   * })
   * ```
   *
   * @param iterable The iterable to populate the database from
   * @param options Configuration options including prefix and key extraction
   * @returns A populated Valkeyrie instance
   */
  public static async from<T>(
    iterable: Iterable<T>,
    options: FromOptions<T>,
  ): Promise<Valkeyrie> {
    return Valkeyrie[kFrom](iterable, options, undefined)
  }

  /**
   * Internal method to create and populate a database with schemas.
   * Used by ValkeyrieBuilder.
   */
  static async [kFrom]<T>(
    iterable: Iterable<T>,
    options: FromOptions<T>,
    schemaRegistry?: SchemaRegistry,
  ): Promise<Valkeyrie> {
    // Open or create the database
    const openOptions: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    } = {}
    if (options.serializer !== undefined) {
      openOptions.serializer = options.serializer
    }
    if (options.destroyOnClose !== undefined) {
      openOptions.destroyOnClose = options.destroyOnClose
    }
    let driverFn = options.driverFn
    if (driverFn === undefined) {
      driverFn = (serializer?: () => Serializer) =>
        sqliteDriver(options.path, serializer)
    }

    const db: Valkeyrie = await Valkeyrie[kOpen](
      driverFn,
      openOptions,
      schemaRegistry,
    )

    const {
      prefix,
      keyProperty,
      expireIn,
      onProgress,
      onError = 'stop',
      onErrorCallback,
    } = options

    // Validate prefix
    db.validateKeys([prefix] as unknown[])

    const errors: Array<{ error: Error; item: T }> = []
    let processed = 0
    let currentBatch: Array<{ key: Key; value: T }> = []

    const BATCH_SIZE = 1000

    const flushBatch = async (): Promise<void> => {
      if (currentBatch.length === 0) return

      const atomic = db.atomic()
      for (const { key, value } of currentBatch) {
        atomic.set(key, value, expireIn ? { expireIn } : {})
      }
      await atomic.commit()

      currentBatch = []
    }

    try {
      for (const item of iterable) {
        try {
          // Extract key part and construct full key
          const keyPart = Valkeyrie.extractKeyPart(item, keyProperty)
          const key = [...prefix, keyPart]

          // Add to current batch
          currentBatch.push({ key, value: item })

          // Flush batch if it reaches the limit
          if (currentBatch.length >= BATCH_SIZE) {
            await flushBatch()
          }

          processed++
          if (onProgress) {
            onProgress(processed)
          }
        } catch (error) {
          if (onError === 'stop') {
            throw error
          }
          errors.push({ error: error as Error, item })
          if (onErrorCallback) {
            onErrorCallback(error as Error, item)
          }
        }
      }

      // Flush remaining items
      await flushBatch()

      if (onProgress) {
        onProgress(processed, processed)
      }

      return db
    } catch (error) {
      // Close and clean up on error
      await db.close()
      throw error
    }
  }

  /**
   * Creates and populates a Valkeyrie database from an asynchronous iterable.
   *
   * @example
   * ```typescript
   * async function* generateUsers() {
   *   yield { id: 1, name: 'Alice' }
   *   yield { id: 2, name: 'Bob' }
   * }
   *
   * const db = await Valkeyrie.fromAsync(generateUsers(), {
   *   prefix: ['users'],
   *   keyProperty: 'id'
   * })
   * ```
   *
   * @param iterable The async iterable to populate the database from
   * @param options Configuration options including prefix and key extraction
   * @returns A populated Valkeyrie instance
   */
  public static async fromAsync<T>(
    iterable: AsyncIterable<T>,
    options: FromOptions<T>,
  ): Promise<Valkeyrie> {
    return Valkeyrie[kFromAsync](iterable, options, undefined)
  }

  /**
   * Internal method to create and populate a database with schemas from async iterable.
   * Used by ValkeyrieBuilder.
   */
  static async [kFromAsync]<T>(
    iterable: AsyncIterable<T>,
    options: FromOptions<T>,
    schemaRegistry?: SchemaRegistry,
  ): Promise<Valkeyrie> {
    // Open or create the database
    const openOptions: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    } = {}
    if (options.serializer !== undefined) {
      openOptions.serializer = options.serializer
    }
    if (options.destroyOnClose !== undefined) {
      openOptions.destroyOnClose = options.destroyOnClose
    }
    let driverFn = options.driverFn
    if (driverFn === undefined) {
      driverFn = (serializer?: () => Serializer) =>
        sqliteDriver(options.path, serializer)
    }

    const db: Valkeyrie = await Valkeyrie[kOpen](
      driverFn,
      openOptions,
      schemaRegistry,
    )

    const {
      prefix,
      keyProperty,
      expireIn,
      onProgress,
      onError = 'stop',
      onErrorCallback,
    } = options

    // Validate prefix
    db.validateKeys([prefix] as unknown[])

    const errors: Array<{ error: Error; item: T }> = []
    let processed = 0
    let currentBatch: Array<{ key: Key; value: T }> = []

    const BATCH_SIZE = 1000

    const flushBatch = async (): Promise<void> => {
      if (currentBatch.length === 0) return

      const atomic = db.atomic()
      for (const { key, value } of currentBatch) {
        atomic.set(key, value, expireIn ? { expireIn } : {})
      }
      await atomic.commit()

      currentBatch = []
    }

    try {
      for await (const item of iterable) {
        try {
          // Extract key part and construct full key
          const keyPart = Valkeyrie.extractKeyPart(item, keyProperty)
          const key = [...prefix, keyPart]

          // Add to current batch
          currentBatch.push({ key, value: item })

          // Flush batch if it reaches the limit
          if (currentBatch.length >= BATCH_SIZE) {
            await flushBatch()
          }

          processed++
          if (onProgress) {
            // For async iterables, we don't know the total count
            onProgress(processed)
          }
        } catch (error) {
          if (onError === 'stop') {
            throw error
          }
          errors.push({ error: error as Error, item })
          if (onErrorCallback) {
            onErrorCallback(error as Error, item)
          }
        }
      }

      // Flush remaining items
      await flushBatch()

      if (onProgress) {
        onProgress(processed, processed)
      }

      return db
    } catch (error) {
      // Close and clean up on error
      await db.close()
      throw error
    }
  }

  public async close(): Promise<void> {
    if (this.#destroyOnClose) {
      await this.destroy()
    }
    await this.#driver.close()
    this.#isClosed = true
  }

  /**
   * Destroys the database by removing the underlying database file.
   * This operation cannot be undone and will result in permanent data loss.
   * @returns A promise that resolves when the database has been destroyed
   */
  public async destroy(): Promise<void> {
    await this.#driver.destroy()
  }

  /**
   * Clears all data from the database but keeps the database file.
   * This operation cannot be undone and will result in permanent data loss.
   * @returns A promise that resolves when the database has been cleared
   */
  public async clear(): Promise<void> {
    await this.#driver.clear()
  }

  /**
   * Validates that the provided keys are arrays.
   *
   * @param keys - The keys to validate.
   * @throws {TypeError} If any key is not an array.
   */

  public validateKeys(keys: unknown[]): asserts keys is Key[] {
    for (const key of keys) {
      if (!Array.isArray(key)) {
        throw new TypeError('Key must be an array')
      }
    }
  }

  /**
   * Generates a unique versionstamp for each operation using database-level sequence.
   * This ensures cross-process atomicity and prevents versionstamp collisions
   * between multiple instances sharing the same database.
   *
   * @returns A string representing the generated versionstamp.
   */
  private async generateVersionstamp(): Promise<string> {
    return await this.#driver.generateVersionstamp()
  }

  /**
   * Generates a hash for a given key. This method is crucial for indexing and storing keys in the database.
   * It converts each part of the key into a specific byte format based on its type, following Deno.KV's encoding format.
   * The format for each type is as follows:
   * - Uint8Array: 0x01 + bytes + 0x00
   * - String: 0x02 + utf8 bytes + 0x00
   * - BigInt: 0x03 + 8 bytes int64 + 0x00
   * - Number: 0x04 + 8 bytes double + 0x00
   * - Boolean: 0x05 + single byte + 0x00
   *
   * After converting each part, they are concatenated with a null byte delimiter to form the full key.
   * This method ensures that keys are consistently formatted and can be reliably hashed for storage and retrieval.
   * Note that key ordering is determined by a lexicographical comparison of their parts, with the first part being the most significant and the last part being the least significant. Additionally, key comparisons are case sensitive.
   *
   * @param {Key} key - The key to be hashed.
   * @returns {Buffer} - The buffer representation of the hashed key.
   */
  private keyToBuffer(
    key: Key,
    operation: 'write' | 'read' = 'read',
  ): Uint8Array {
    const parts = key.map((part) => {
      let bytes: Buffer

      if (part instanceof Uint8Array) {
        // Uint8Array format: 0x01 + bytes + 0x00
        bytes = Buffer.alloc(part.length + 2)
        bytes[0] = 0x01 // Uint8Array type marker
        Buffer.from(part).copy(bytes, 1)
        bytes[bytes.length - 1] = 0x00
      } else if (typeof part === 'string') {
        // String format: 0x02 + utf8 bytes + 0x00
        const strBytes = Buffer.from(part, 'utf8')
        bytes = Buffer.alloc(strBytes.length + 2)
        bytes[0] = 0x02 // String type marker
        strBytes.copy(bytes, 1)
        bytes[bytes.length - 1] = 0x00
      } else if (typeof part === 'bigint') {
        // Bigint format: 0x03 + 8 bytes int64 + 0x00
        bytes = Buffer.alloc(10)
        bytes[0] = 0x03 // Bigint type marker
        const hex = part.toString(16).padStart(16, '0')
        Buffer.from(hex, 'hex').copy(bytes, 1)
        bytes[bytes.length - 1] = 0x00
      } else if (typeof part === 'number') {
        // Number format: 0x04 + 8 bytes double + 0x00
        bytes = Buffer.alloc(10)
        bytes[0] = 0x04 // Number type marker
        bytes.writeDoubleBE(part, 1)
        bytes[bytes.length - 1] = 0x00
      } else if (typeof part === 'boolean') {
        // Boolean format: 0x05 + single byte + 0x00
        bytes = Buffer.alloc(3)
        bytes[0] = 0x05 // Boolean type marker
        bytes[1] = part ? 1 : 0
        bytes[bytes.length - 1] = 0x00
      } else {
        throw new Error(`Unsupported key part type: ${typeof part}`)
      }

      return new Uint8Array(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    })

    // Join all parts with a null byte delimiter
    const fullKey = Buffer.concat([...parts])
    if (fullKey.length > 2048) {
      throw new TypeError(
        `Key too large for ${operation} (max ${
          operation === 'write' ? 2048 : 2049
        } bytes)`,
      )
    }
    return fullKey
  }

  /**
   * Hashes a key.
   *
   * @param {Key} key - The key to hash.
   * @returns {string} - The hex string representation of the hashed key.
   */
  private hashKey(key: Key, operation?: 'write' | 'read'): string {
    return Buffer.from(this.keyToBuffer(key, operation)).toString('hex')
  }

  /**
   * Hashes a key and returns a base64-encoded string.
   *
   * @param {Key} key - The key to get the cursor from.
   * @returns {string} - The base64 string representation of the hashed key.
   */
  private getCursorFromKey(key: Key): string {
    return Buffer.from(this.keyToBuffer(key))
      .toString('base64')
      .replace(/=+$/, '')
  }

  /**
   * Decodes a base64-encoded key hash back into its original key parts.
   * This method reverses the encoding process performed by hashKey.
   * It handles the following formats:
   * - Uint8Array: 0x01 + bytes + 0x00
   * - String: 0x02 + utf8 bytes + 0x00
   * - BigInt: 0x03 + 8 bytes int64 + 0x00
   * - Number: 0x04 + 8 bytes double + 0x00
   * - Boolean: 0x05 + single byte + 0x00
   *
   * @param {string} hash - The base64-encoded key hash to decode
   * @returns {Key} The decoded key parts array
   * @throws {Error} If the hash format is invalid or contains an unknown type marker
   */
  private decodeKeyHash(hash: string): Key {
    const buffer = Buffer.from(hash, 'hex')
    const parts: KeyPart[] = []
    let pos = 0

    while (pos < buffer.length) {
      const typeMarker = buffer[pos] as number
      pos++

      switch (typeMarker) {
        case 0x01: {
          // Uint8Array
          let end = pos
          // Find the terminator (0x00) that marks the end of the Uint8Array
          // We need to scan for it rather than stopping at the first 0 value
          // since the Uint8Array itself might contain zeros
          while (end < buffer.length) {
            // Check if this position is the terminator
            if (buffer[end] === 0x00) {
              const nextPos = end + 1
              // Check if we're at the end of the buffer
              if (nextPos >= buffer.length) {
                break
              }

              // Check if the next byte is a valid type marker
              const nextByte = buffer[nextPos]
              if (
                nextByte === 0x01 ||
                nextByte === 0x02 ||
                nextByte === 0x03 ||
                nextByte === 0x04 ||
                nextByte === 0x05
              ) {
                break
              }
            }
            end++
          }

          if (end >= buffer.length)
            throw new Error('Invalid key hash: unterminated Uint8Array')
          const bytes = buffer.subarray(pos, end)
          parts.push(new Uint8Array(bytes))
          pos = end + 1
          break
        }
        case 0x02: {
          // String
          let end = pos
          while (end < buffer.length && buffer[end] !== 0x00) end++
          if (end >= buffer.length)
            throw new Error('Invalid key hash: unterminated String')
          const str = buffer.subarray(pos, end).toString('utf8')
          parts.push(str)
          pos = end + 1
          break
        }
        case 0x03: {
          // BigInt
          if (pos + 8 >= buffer.length)
            throw new Error('Invalid key hash: BigInt too short')
          if (buffer[pos + 8] !== 0x00)
            throw new Error('Invalid key hash: BigInt not terminated')
          const hex = buffer.subarray(pos, pos + 8).toString('hex')
          parts.push(BigInt(`0x${hex}`))
          pos += 9
          break
        }
        case 0x04: {
          // Number
          if (pos + 8 >= buffer.length)
            throw new Error('Invalid key hash: Number too short')
          if (buffer[pos + 8] !== 0x00)
            throw new Error('Invalid key hash: Number not terminated')
          const num = buffer.readDoubleBE(pos)
          parts.push(num)
          pos += 9
          break
        }
        case 0x05: {
          // Boolean
          if (pos >= buffer.length)
            throw new Error('Invalid key hash: Boolean too short')
          if (buffer[pos + 1] !== 0x00)
            throw new Error('Invalid key hash: Boolean not terminated')
          parts.push(buffer[pos] === 1)
          pos += 2
          break
        }
        default:
          throw new Error(
            `Invalid key hash: unknown type marker 0x${typeMarker.toString(16)}`,
          )
      }
    }

    return parts
  }

  /**
   * Gets a value from the database with automatic type inference based on registered schemas.
   * Uses `const` type parameter to automatically infer literal key types without `as const`.
   *
   * @param key - The key to retrieve (supports literal type inference)
   * @returns Entry with the value or null if not found. Type is automatically inferred from schema registry.
   */
  public async get<const TKey extends Key>(
    key: TKey,
  ): Promise<EntryMaybe<InferTypeForKey<TRegistry, TKey>>> {
    this.throwIfClosed()
    this.validateKeys([key])
    if (key.length === 0) {
      throw new Error('Key cannot be empty')
    }
    const keyHash = this.hashKey(key, 'read')
    const now = Date.now()
    const result = await this.#driver.get(keyHash, now)

    if (!result) {
      return { key, value: null, versionstamp: null }
    }

    return {
      key: this.decodeKeyHash(result.keyHash),
      value: result.value as InferTypeForKey<TRegistry, TKey>,
      versionstamp: result.versionstamp,
    }
  }

  /**
   * Gets multiple values from the database.
   * Note: For type inference, use individual get() calls instead.
   *
   * @param keys - Array of keys to retrieve
   * @returns Array of entries with values or nulls
   */
  public async getMany<T = unknown>(keys: Key[]): Promise<EntryMaybe<T>[]> {
    this.throwIfClosed()
    this.validateKeys(keys)
    if (keys.length > 10) {
      throw new TypeError('Too many ranges (max 10)')
    }
    return Promise.all(keys.map((key) => this.get(key))) as Promise<
      EntryMaybe<T>[]
    >
  }

  /**
   * Sets a value in the database with automatic type checking based on registered schemas.
   * Uses `const` type parameter to automatically infer literal key types without `as const`.
   *
   * @param key - The key to set (supports literal type inference)
   * @param value - The value to set. Type is automatically checked against schema registry.
   * @param options - Optional settings like expireIn
   * @returns Result with versionstamp
   */
  public async set<const TKey extends Key>(
    key: TKey,
    value: InferTypeForKey<TRegistry, TKey>,
    options: SetOptions = {},
  ): Promise<{ ok: true; versionstamp: string }> {
    this.throwIfClosed()
    this.validateKeys([key])
    if (key.length === 0) {
      throw new Error('Key cannot be empty')
    }

    // Validate that key doesn't contain reserved '*' wildcard
    validateReservedKeyParts(key)

    // Validate value against schema if one is registered
    const validatedValue = await validateValue(key, value, this.#schemaRegistry)

    const keyHash = this.hashKey(key, 'write')
    const versionstamp = await this.generateVersionstamp()

    await this.#driver.set(
      keyHash,
      validatedValue,
      versionstamp,
      options.expireIn ? Date.now() + options.expireIn : undefined,
    )

    return { ok: true, versionstamp }
  }

  public async delete(key: Key): Promise<void> {
    this.throwIfClosed()
    this.validateKeys([key])
    const keyHash = this.hashKey(key)
    await this.#driver.delete(keyHash)
  }

  private validatePrefixKey(
    prefix: Key,
    key: Key,
    type: 'start' | 'end',
  ): void {
    if (key.length <= prefix.length) {
      throw new TypeError(
        `${
          type.charAt(0).toUpperCase() + type.slice(1)
        } key is not in the keyspace defined by prefix`,
      )
    }
    // Check if key has the same prefix
    const keyPrefix = key.slice(0, prefix.length)
    if (!keyPrefix.every((part, i) => part === prefix[i])) {
      throw new TypeError(
        `${
          type.charAt(0).toUpperCase() + type.slice(1)
        } key is not in the keyspace defined by prefix`,
      )
    }
  }

  private async *listBatch<T>(
    startHash: string,
    endHash: string,
    prefixHash: string,
    options: {
      limit: number
      batchSize: number
      reverse: boolean
    },
  ): AsyncIterableIterator<Entry<T>, void> {
    const { limit, batchSize, reverse } = options
    if (batchSize > 1000) {
      throw new TypeError('Too many entries (max 1000)')
    }
    const now = Date.now()
    let remainingLimit = limit
    let currentStartHash = startHash
    let currentEndHash = endHash

    // Continue fetching as long as we have a limit remaining or limit is Infinity
    while (remainingLimit > 0 || limit === Number.POSITIVE_INFINITY) {
      // If limit is Infinity, use batchSize, otherwise use the minimum of batchSize and remainingLimit
      const currentBatchSize =
        limit === Number.POSITIVE_INFINITY
          ? batchSize
          : Math.min(batchSize, remainingLimit)
      const results = await this.#driver.list(
        currentStartHash,
        currentEndHash,
        prefixHash,
        now,
        currentBatchSize,
        reverse,
      )
      if (results.length === 0) break

      for (const result of results) {
        yield {
          key: this.decodeKeyHash(result.keyHash),
          value: result.value as T,
          versionstamp: result.versionstamp,
        }
      }

      if (results.length < currentBatchSize) break

      // Only decrement remainingLimit if it's not Infinity
      if (limit !== Number.POSITIVE_INFINITY) {
        remainingLimit -= results.length
      }

      // Update hash bounds for next batch
      const lastResult = results[results.length - 1]
      if (!lastResult) break
      const lastKeyHash = lastResult.keyHash
      if (reverse) {
        currentEndHash = lastKeyHash
      } else {
        currentStartHash = `${lastKeyHash}\0` // Use next possible hash value
      }
    }
  }

  private decodeCursorKey(cursor: string): Key {
    const hash = Buffer.from(cursor, 'base64').toString('hex')
    const decoded = this.decodeKeyHash(hash)
    if (decoded.length === 0) {
      throw new Error('Invalid cursor: empty key')
    }
    return decoded
  }

  private calculatePrefixBounds(
    prefix: Key,
    cursor?: string,
    reverse = false,
  ): { startHash: string; endHash: string } {
    const prefixHash = this.hashKey(prefix)

    if (cursor) {
      const cursorKey = this.decodeCursorKey(cursor)
      const cursorHash = this.hashKey(cursorKey)

      return reverse
        ? { startHash: prefixHash, endHash: cursorHash }
        : { startHash: `${cursorHash}\0`, endHash: `${prefixHash}\xff` }
    }

    return {
      startHash: prefixHash,
      endHash: `${prefixHash}\xff`,
    }
  }

  private calculateRangeBounds(
    start: Key,
    end: Key,
    cursor?: string,
    reverse = false,
  ): { startHash: string; endHash: string } {
    // Compare start and end keys
    const startHash = this.hashKey(start)
    const endHash = this.hashKey(end)
    if (startHash > endHash) {
      throw new TypeError('Start key is greater than end key')
    }

    if (cursor) {
      const cursorKey = this.decodeCursorKey(cursor)
      const cursorHash = this.hashKey(cursorKey)

      return reverse
        ? { startHash, endHash: cursorHash }
        : { startHash: `${cursorHash}\0`, endHash }
    }

    return { startHash, endHash }
  }

  private calculateEmptyPrefixBounds(
    cursor?: string,
    reverse = false,
  ): { startHash: string; endHash: string } {
    if (cursor) {
      const cursorKey = this.decodeCursorKey(cursor)
      const cursorHash = this.hashKey(cursorKey)

      return reverse
        ? { startHash: '', endHash: cursorHash }
        : { startHash: `${cursorHash}\0`, endHash: '\uffff' }
    }

    return {
      startHash: '',
      endHash: '\uffff',
    }
  }

  private isPrefixWithStart(
    selector: ListSelector,
  ): selector is { prefix: Key; start: Key } {
    return 'prefix' in selector && 'start' in selector
  }

  private isPrefixWithEnd(
    selector: ListSelector,
  ): selector is { prefix: Key; end: Key } {
    return 'prefix' in selector && 'end' in selector
  }

  private isRangeSelector(
    selector: ListSelector,
  ): selector is { start: Key; end: Key } {
    return 'start' in selector && 'end' in selector
  }

  private validateSelector(selector: ListSelector): void {
    // Cannot have prefix + start + end together
    if ('prefix' in selector && 'start' in selector && 'end' in selector) {
      throw new TypeError('Cannot specify prefix with both start and end keys')
    }

    // Cannot have start without end (unless with prefix)
    if (
      !('prefix' in selector) &&
      'start' in selector &&
      !('end' in selector)
    ) {
      throw new TypeError('Cannot specify start key without prefix')
    }

    // Cannot have end without start (unless with prefix)
    if (
      !('prefix' in selector) &&
      !('start' in selector) &&
      'end' in selector
    ) {
      throw new TypeError('Cannot specify end key without prefix')
    }

    // Validate prefix constraints
    if ('prefix' in selector) {
      if ('start' in selector) {
        this.validatePrefixKey(selector.prefix, selector.start, 'start')
      }
      if ('end' in selector) {
        this.validatePrefixKey(selector.prefix, selector.end, 'end')
      }
    }
  }

  private getBoundsForPrefix(
    prefix: Key,
    cursor?: string,
    reverse = false,
  ): { startHash: string; endHash: string; prefixHash: string } {
    if (prefix.length === 0) {
      const bounds = this.calculateEmptyPrefixBounds(cursor, reverse)
      return { ...bounds, prefixHash: '' }
    }

    const prefixHash = this.hashKey(prefix)
    const bounds = this.calculatePrefixBounds(prefix, cursor, reverse)
    return { ...bounds, prefixHash }
  }

  private getBoundsForPrefixWithRange(
    prefix: Key,
    start: Key,
    end: Key,
    cursor?: string,
    reverse = false,
  ): { startHash: string; endHash: string; prefixHash: string } {
    const prefixHash = this.hashKey(prefix)
    const bounds = this.calculateRangeBounds(start, end, cursor, reverse)
    return { ...bounds, prefixHash }
  }

  // Overload for prefix-based selectors with type inference
  public list<const TPrefix extends Key>(
    selector:
      | { prefix: TPrefix }
      | { prefix: TPrefix; start: Key }
      | { prefix: TPrefix; end: Key },
    options?: ListOptions,
  ): AsyncIterableIterator<
    Entry<InferTypeForPrefix<TRegistry, TPrefix>>,
    void
  > & {
    readonly cursor: string
    [Symbol.asyncDispose](): Promise<void>
  }
  // Overload for range-based selectors without prefix (returns unknown)
  public list<T = unknown>(
    selector: { start: Key; end: Key },
    options?: ListOptions,
  ): AsyncIterableIterator<Entry<T>, void> & {
    readonly cursor: string
    [Symbol.asyncDispose](): Promise<void>
  }
  public list<T = unknown>(
    selector: ListSelector,
    options: ListOptions = {},
  ): AsyncIterableIterator<Entry<T>, void> & {
    readonly cursor: string
    [Symbol.asyncDispose](): Promise<void>
  } {
    this.throwIfClosed()
    this.validateSelector(selector)

    const {
      limit = Number.POSITIVE_INFINITY,
      reverse = false,
      batchSize = 500,
      cursor,
    } = options
    let bounds: { startHash: string; endHash: string; prefixHash: string }

    if (this.isRangeSelector(selector)) {
      bounds = this.getBoundsForPrefixWithRange(
        [],
        selector.start,
        selector.end,
        cursor,
        reverse,
      )
    } else if ('prefix' in selector) {
      if (this.isPrefixWithStart(selector)) {
        bounds = this.getBoundsForPrefixWithRange(
          selector.prefix,
          selector.start,
          [...selector.prefix, '\xff'],
          cursor,
          reverse,
        )
      } else if (this.isPrefixWithEnd(selector)) {
        bounds = this.getBoundsForPrefixWithRange(
          selector.prefix,
          selector.prefix,
          selector.end,
          cursor,
          reverse,
        )
      } else {
        bounds = this.getBoundsForPrefix(selector.prefix, cursor, reverse)
      }
    } else {
      throw new TypeError(
        'Invalid selector: must specify either prefix or start/end range',
      )
    }

    const generator = this.listBatch<T>(
      bounds.startHash,
      bounds.endHash,
      bounds.prefixHash,
      { limit, batchSize, reverse },
    )

    let lastKey: Key | null = null
    const self = this

    const wrapper = {
      [Symbol.asyncIterator]() {
        return this
      },
      async next() {
        const result = await generator.next()
        if (!result.done && result.value) {
          lastKey = result.value.key
        }
        return result
      },
      get cursor() {
        if (!lastKey) return ''
        return self.getCursorFromKey(lastKey)
      },
      async [Symbol.asyncDispose]() {
        await self.close()
      },
    }

    return wrapper
  }

  public async cleanup(): Promise<void> {
    this.throwIfClosed()
    const now = Date.now()
    this.#driver.cleanup(now)
  }

  public atomic(): AtomicOperation<TRegistry> {
    this.throwIfClosed()
    return new AtomicOperation(this)
  }

  /**
   * Internal symbol-based accessor for schema registry.
   * Used by AtomicOperation for validation.
   */
  get [kSchemaRegistry](): SchemaRegistry | undefined {
    return this.#schemaRegistry
  }

  public async executeAtomicOperation(
    checks: Check[],
    mutations: Mutation[],
  ): Promise<{ ok: true; versionstamp: string } | { ok: false }> {
    this.throwIfClosed()
    const versionstamp = await this.generateVersionstamp()

    try {
      return await this.#driver.withTransaction(async () => {
        // Verify all checks pass within the transaction
        for (const check of checks) {
          const result = await this.get(check.key)
          if (result.versionstamp !== check.versionstamp) {
            return { ok: false }
          }
        }

        // Apply mutations - all using the same versionstamp
        for (const mutation of mutations) {
          const keyHash = this.hashKey(mutation.key)

          if (mutation.type === 'delete') {
            await this.#driver.delete(keyHash)
          } else if (mutation.type === 'set') {
            const serializedValue = mutation.value

            if (mutation.expireIn) {
              const expiresAt = Date.now() + mutation.expireIn
              await this.#driver.set(
                keyHash,
                serializedValue,
                versionstamp,
                expiresAt,
              )
            } else {
              await this.#driver.set(keyHash, serializedValue, versionstamp)
            }
          } else if (
            mutation.type === 'sum' ||
            mutation.type === 'max' ||
            mutation.type === 'min'
          ) {
            const currentValue = await this.get(mutation.key)
            let newValue: KvU64

            if (currentValue.value === null) {
              newValue = mutation.value
            } else if (!(currentValue.value instanceof KvU64)) {
              throw new TypeError(
                `Failed to perform '${mutation.type}' mutation on a non-U64 value in the database`,
              )
            } else {
              const current = currentValue.value.value
              if (mutation.type === 'sum') {
                newValue = new KvU64(
                  (current + mutation.value.value) & 0xffffffffffffffffn,
                )
              } else if (mutation.type === 'max') {
                newValue = new KvU64(
                  current > mutation.value.value
                    ? current
                    : mutation.value.value,
                )
              } else {
                newValue = new KvU64(
                  current < mutation.value.value
                    ? current
                    : mutation.value.value,
                )
              }
            }

            await this.#driver.set(keyHash, newValue, versionstamp)
          }
        }

        return { ok: true, versionstamp }
      })
    } catch (error) {
      if (error instanceof TypeError) {
        throw error
      }
      /* c8 ignore start */
      return { ok: false }
    } /* c8 ignore end */
  }

  [Symbol.dispose](): void {
    this.close().catch(() => {})
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close()
  }

  private throwIfClosed(): void {
    if (this.#isClosed) {
      throw new Error('Database is closed')
    }
  }

  /**
   * Watches multiple keys for changes with automatic type inference based on registered schemas.
   * Uses `const` type parameter to automatically infer literal key types without `as const`.
   *
   * @param keys - Array of keys to watch (supports literal type inference)
   * @returns ReadableStream of entry arrays. Types are automatically inferred from schema registry.
   */
  public watch<const TKeys extends readonly Key[]>(
    keys: [...TKeys],
  ): ReadableStream<{
    [K in keyof TKeys]: TKeys[K] extends Key
      ? EntryMaybe<InferTypeForKey<TRegistry, TKeys[K]>>
      : never
  }>
  public watch<T extends readonly unknown[]>(
    keys: Key[],
  ): ReadableStream<EntryMaybe<T[number]>[]> {
    this.throwIfClosed()
    this.validateKeys(keys)
    if (keys.length === 0) {
      throw new Error('Keys cannot be empty')
    }
    const keyHashes = keys.map((key) => this.hashKey(key))
    return this.#driver.watch(keyHashes).pipeThrough(
      new TransformStream({
        transform: (chunk, controller) => {
          controller.enqueue(
            chunk.map((entry) => ({
              key: this.decodeKeyHash(entry.keyHash),
              value: entry.value as T[number],
              versionstamp: entry.versionstamp as string,
            })),
          )
        },
      }),
    )
  }
}

// Internal class - not exported
export class AtomicOperation<
  TRegistry extends SchemaRegistryType = readonly [],
> {
  private checks: Check[] = []
  private mutations: Mutation[] = []
  private valkeyrie: Valkeyrie<TRegistry>
  private totalMutationSize = 0
  private totalKeySize = 0

  constructor(valkeyrie: Valkeyrie<TRegistry>) {
    this.valkeyrie = valkeyrie
  }

  private validateVersionstamp(versionstamp: string | null): void {
    if (versionstamp === null) return
    if (typeof versionstamp !== 'string') {
      throw new TypeError('Versionstamp must be a string or null')
    }
    if (versionstamp.length !== 20) {
      throw new TypeError('Versionstamp must be 20 characters long')
    }
    if (!/^[0-9a-f]{20}$/.test(versionstamp)) {
      throw new TypeError('Versionstamp must be a hex string')
    }
  }

  check(...checks: AtomicCheck[]): AtomicOperation<TRegistry> {
    for (const check of checks) {
      if (this.checks.length >= 100) {
        throw new TypeError('Too many checks (max 100)')
      }
      this.valkeyrie.validateKeys([check.key])
      this.validateVersionstamp(check.versionstamp)
      this.checks.push(check)
    }
    return this
  }

  mutate(...mutations: Mutation[]): AtomicOperation<TRegistry> {
    for (const mutation of mutations) {
      if (this.mutations.length >= 1000) {
        throw new TypeError('Too many mutations (max 1000)')
      }
      this.valkeyrie.validateKeys([mutation.key])
      if (mutation.key.length === 0) {
        throw new Error('Key cannot be empty')
      }

      // Validate that key doesn't contain reserved '*' wildcard
      validateReservedKeyParts(mutation.key)

      const keySize = serialize(mutation.key).length
      this.totalKeySize += keySize

      // Track mutation size without validation
      let mutationSize = keySize
      if ('value' in mutation) {
        if (
          mutation.type === 'sum' ||
          mutation.type === 'max' ||
          mutation.type === 'min'
        ) {
          mutationSize += 8 // 64-bit integer size
        } else {
          mutationSize += serialize(mutation.value).length
        }
      }
      this.totalMutationSize += mutationSize

      // Validate mutation type and required fields
      switch (mutation.type) {
        case 'set':
          if (!('value' in mutation)) {
            throw new TypeError('Set mutation requires a value')
          }
          break
        case 'delete':
          if ('value' in mutation) {
            throw new TypeError('Delete mutation cannot have a value')
          }
          break
        case 'sum':
          if (!('value' in mutation) || !(mutation.value instanceof KvU64)) {
            throw new TypeError('Cannot sum KvU64 with Number')
          }
          break
        case 'max':
        case 'min':
          if (!('value' in mutation) || !(mutation.value instanceof KvU64)) {
            throw new TypeError(
              `Failed to perform '${mutation.type}' mutation on a non-U64 operand`,
            )
          }
          break
        default:
          throw new TypeError('Invalid mutation type')
      }

      this.mutations.push(mutation)
    }
    return this
  }

  set<const TKey extends Key>(
    key: TKey,
    value: InferTypeForKey<TRegistry, TKey>,
    options: SetOptions = {},
  ): AtomicOperation<TRegistry> {
    return this.mutate({
      type: 'set',
      key,
      value,
      ...(options.expireIn ? { expireIn: options.expireIn } : {}),
    })
  }

  delete(key: Key): AtomicOperation<TRegistry> {
    return this.mutate({ type: 'delete', key })
  }

  sum(key: Key, value: bigint | KvU64): AtomicOperation<TRegistry> {
    const u64Value = value instanceof KvU64 ? value : new KvU64(BigInt(value))
    return this.mutate({ type: 'sum', key, value: u64Value })
  }

  max(key: Key, value: bigint | KvU64): AtomicOperation<TRegistry> {
    const u64Value = value instanceof KvU64 ? value : new KvU64(BigInt(value))
    return this.mutate({ type: 'max', key, value: u64Value })
  }

  min(key: Key, value: bigint | KvU64): AtomicOperation<TRegistry> {
    const u64Value = value instanceof KvU64 ? value : new KvU64(BigInt(value))
    return this.mutate({ type: 'min', key, value: u64Value })
  }

  async commit(): Promise<{ ok: true; versionstamp: string } | { ok: false }> {
    // Validate total sizes before executing the atomic operation
    if (this.totalKeySize > 81920) {
      throw new TypeError('Total key size too large (max 81920 bytes)')
    }
    if (this.totalMutationSize > 819200) {
      throw new TypeError('Total mutation size too large (max 819200 bytes)')
    }

    // Validate all 'set' mutations against schemas before committing
    const schemaRegistry = this.valkeyrie[kSchemaRegistry]
    const validatedMutations: Mutation[] = []

    for (const mutation of this.mutations) {
      if (mutation.type === 'set') {
        // Validate the value against the schema
        const validatedValue = await validateValue(
          mutation.key,
          mutation.value,
          schemaRegistry,
        )

        // Create new mutation with validated value
        validatedMutations.push({
          ...mutation,
          value: validatedValue,
        })
      } else {
        // Non-set mutations don't need validation
        validatedMutations.push(mutation)
      }
    }

    return this.valkeyrie.executeAtomicOperation(
      this.checks,
      validatedMutations,
    )
  }
}
