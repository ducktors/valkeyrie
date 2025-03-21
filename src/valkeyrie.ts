import { serialize } from 'node:v8'
import type { Driver } from './driver.js'
import { KvU64 } from './kv-u64.js'
import type { Serializer } from './serializers/serializer.js'
import { sqliteDriver } from './sqlite-driver.js'

export type KeyPart = Uint8Array | string | number | bigint | boolean
export type Key = KeyPart[]

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

export class Valkeyrie {
  private static internalConstructor = false
  private driver: Driver
  private lastVersionstamp: bigint
  private closed = false
  private constructor(functions: Driver) {
    if (!Valkeyrie.internalConstructor) {
      throw new TypeError('Use Valkeyrie.open() to create a new instance')
    }
    this.driver = functions
    this.lastVersionstamp = 0n
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
    } = {},
  ): Promise<Valkeyrie> {
    Valkeyrie.internalConstructor = true
    const db = new Valkeyrie(await sqliteDriver(path, options.serializer))
    Valkeyrie.internalConstructor = false
    await db.cleanup()
    return db
  }

  public async close(): Promise<void> {
    await this.driver.close()
    this.closed = true
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
   * Generates a unique versionstamp for each operation.
   * This method ensures that each versionstamp is monotonically increasing,
   * even within the same microsecond, by using the current timestamp in microseconds
   * and incrementing the last used versionstamp if it's not greater than the current timestamp.
   * The generated versionstamp is a hexadecimal string representation of the BigInt value.
   *
   * @returns A string representing the generated versionstamp.
   */
  private generateVersionstamp(): string {
    // Get current timestamp in microseconds
    const now = BigInt(Date.now()) * 1000n

    // Ensure monotonically increasing values even within the same microsecond
    this.lastVersionstamp =
      this.lastVersionstamp < now ? now : this.lastVersionstamp + 1n

    // Convert the BigInt to a hexadecimal string and pad it to 20 characters
    return this.lastVersionstamp.toString(16).padStart(20, '0')
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

  public async get<T = unknown>(key: Key): Promise<EntryMaybe<T>> {
    this.throwIfClosed()
    this.validateKeys([key])
    if (key.length === 0) {
      throw new Error('Key cannot be empty')
    }
    const keyHash = this.hashKey(key, 'read')
    const now = Date.now()
    const result = await this.driver.get(keyHash, now)

    if (!result) {
      return { key, value: null, versionstamp: null }
    }

    return {
      key: this.decodeKeyHash(result.keyHash),
      value: result.value as T,
      versionstamp: result.versionstamp,
    }
  }

  public async getMany(keys: Key[]): Promise<EntryMaybe[]> {
    this.throwIfClosed()
    this.validateKeys(keys)
    if (keys.length > 10) {
      throw new TypeError('Too many ranges (max 10)')
    }
    return Promise.all(keys.map((key) => this.get(key)))
  }

  public async set<T = unknown>(
    key: Key,
    value: T,
    options: SetOptions = {},
  ): Promise<{ ok: true; versionstamp: string }> {
    this.throwIfClosed()
    this.validateKeys([key])
    if (key.length === 0) {
      throw new Error('Key cannot be empty')
    }
    const keyHash = this.hashKey(key, 'write')
    const versionstamp = this.generateVersionstamp()

    await this.driver.set(
      keyHash,
      value,
      versionstamp,
      options.expireIn ? Date.now() + options.expireIn : undefined,
    )

    return { ok: true, versionstamp }
  }

  public async delete(key: Key): Promise<void> {
    this.throwIfClosed()
    this.validateKeys([key])
    const keyHash = this.hashKey(key)
    await this.driver.delete(keyHash)
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
  ): AsyncIterableIterator<Entry<T>> {
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
      const results = await this.driver.list(
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

  private decodeCursorValue(cursor: string): string {
    const bytes = Buffer.from(cursor, 'base64')
    // Skip type marker (0x02) and get the value bytes (excluding terminator 0x00)
    return bytes.subarray(1, bytes.length - 1).toString('utf8')
  }

  private calculatePrefixBounds(
    prefix: Key,
    cursor?: string,
    reverse = false,
  ): { startHash: string; endHash: string } {
    const prefixHash = this.hashKey(prefix)

    if (cursor) {
      const cursorValue = this.decodeCursorValue(cursor)
      const cursorKey = [...prefix, cursorValue]
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
      const cursorValue = this.decodeCursorValue(cursor)
      // For range queries, we need to reconstruct the full key
      // by taking all parts from the start key except the last one
      // and appending the cursor value
      const cursorKey = [...start.slice(0, -1), cursorValue]
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
      return reverse
        ? { startHash: '', endHash: cursor }
        : { startHash: `${cursor}\0`, endHash: '\uffff' }
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

  public list<T = unknown>(
    selector: ListSelector,
    options: ListOptions = {},
  ): AsyncIterableIterator<Entry<T>> & {
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
        const lastPart = lastKey[lastKey.length - 1]
        if (!lastPart) return ''
        return self.getCursorFromKey([lastPart])
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
    this.driver.cleanup(now)
  }

  public atomic(): Atomic {
    this.throwIfClosed()
    return new Atomic(this)
  }

  public async executeAtomicOperation(
    checks: Check[],
    mutations: Mutation[],
  ): Promise<{ ok: true; versionstamp: string } | { ok: false }> {
    this.throwIfClosed()
    const versionstamp = this.generateVersionstamp()

    try {
      return await this.driver.withTransaction(async () => {
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
            await this.driver.delete(keyHash)
          } else if (mutation.type === 'set') {
            const serializedValue = mutation.value

            if (mutation.expireIn) {
              const expiresAt = Date.now() + mutation.expireIn
              await this.driver.set(
                keyHash,
                serializedValue,
                versionstamp,
                expiresAt,
              )
            } else {
              await this.driver.set(keyHash, serializedValue, versionstamp)
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
            } else if (
              (mutation.type === 'sum' ||
                mutation.type === 'min' ||
                mutation.type === 'max') &&
              !(currentValue.value instanceof KvU64)
            ) {
              throw new TypeError(
                `Failed to perform '${mutation.type}' mutation on a non-U64 value in the database`,
              )
            } else if (
              typeof currentValue.value === 'number' ||
              typeof currentValue.value === 'bigint' ||
              currentValue.value instanceof KvU64
            ) {
              const current = BigInt(
                currentValue.value instanceof KvU64
                  ? currentValue.value.value
                  : currentValue.value,
              )
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
            } else {
              throw new TypeError(
                `Invalid value type for ${mutation.type} operation`,
              )
            }

            await this.driver.set(keyHash, newValue, versionstamp)
          }
        }

        return { ok: true, versionstamp }
      })
    } catch (error) {
      if (error instanceof TypeError) {
        throw error
      }
      return { ok: false }
    }
  }

  [Symbol.dispose](): void {
    this.close().catch(() => {})
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close()
  }

  private throwIfClosed(): void {
    if (this.closed) {
      throw new Error('Database is closed')
    }
  }
}

// Internal class - not exported
class Atomic {
  private checks: Check[] = []
  private mutations: Mutation[] = []
  private valkeyrie: Valkeyrie
  private totalMutationSize = 0
  private totalKeySize = 0

  constructor(valkeyrie: Valkeyrie) {
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

  check(...checks: AtomicCheck[]): Atomic {
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

  mutate(...mutations: Mutation[]): Atomic {
    for (const mutation of mutations) {
      if (this.mutations.length >= 1000) {
        throw new TypeError('Too many mutations (max 1000)')
      }
      this.valkeyrie.validateKeys([mutation.key])
      if (mutation.key.length === 0) {
        throw new Error('Key cannot be empty')
      }

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

  set<T = unknown>(key: Key, value: T, options: SetOptions = {}): Atomic {
    return this.mutate({
      type: 'set',
      key,
      value,
      ...(options.expireIn ? { expireIn: options.expireIn } : {}),
    })
  }

  delete(key: Key): Atomic {
    return this.mutate({ type: 'delete', key })
  }

  sum(key: Key, value: bigint | KvU64): Atomic {
    const u64Value = value instanceof KvU64 ? value : new KvU64(BigInt(value))
    return this.mutate({ type: 'sum', key, value: u64Value })
  }

  max(key: Key, value: bigint | KvU64): Atomic {
    const u64Value = value instanceof KvU64 ? value : new KvU64(BigInt(value))
    return this.mutate({ type: 'max', key, value: u64Value })
  }

  min(key: Key, value: bigint | KvU64): Atomic {
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
    return this.valkeyrie.executeAtomicOperation(this.checks, this.mutations)
  }
}
