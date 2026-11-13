import { existsSync } from 'node:fs'
import { unlink } from 'node:fs/promises'
import { DatabaseSync } from 'node:sqlite'
import { KEY_HASH_END, encodeKey } from './key-codec.ts'
import type { Serializer } from './serializers/serializer.ts'
import { type Key, type KeyPart, Valkeyrie } from './valkeyrie.ts'

/**
 * Options of {@link migrateSqliteKeyFormat}.
 */
export interface MigrateSqliteKeyFormatOptions {
  /**
   * The serializer of the store. The migration copies the values as stored
   * bytes, and writes the key format row with this serializer. Default: the
   * v8 serializer, as in `Valkeyrie.open()`.
   */
  serializer?: () => Serializer
  /**
   * Copy the other rows when some rows cannot be migrated, and report the
   * rows that were not copied. Default: false, which stops before the
   * migration creates the target.
   */
  skipUnmigratable?: boolean
}

/**
 * A row that the migration did not copy.
 */
export interface UnmigratableRow {
  /** The hex key hash of the row in key format 1. */
  keyHash: string
  /** Why the migration cannot copy the row. */
  reason: string
}

/**
 * The result of {@link migrateSqliteKeyFormat}.
 */
export interface MigrateSqliteKeyFormatResult {
  /** The number of rows that the migration copied to the target. */
  migrated: number
  /**
   * The number of copied rows with a new key hash. Their key has a 0x00 byte
   * in a string or Uint8Array part.
   */
  rekeyed: number
  /** The rows that the migration did not copy. */
  skipped: UnmigratableRow[]
}

interface SourceRow {
  key_hash: string
  value: Uint8Array
  versionstamp: string
  expires_at: number | null
}

const PAGE_SIZE = 1000

/**
 * Decodes a key hash in key format 1, as Valkeyrie 0.8.4 did. This is the
 * decoder of 0.8.4 without changes. A Uint8Array part ends at the first 0x00
 * byte before a type marker or the end of the hash, so a hash with a 0x00 byte
 * in a part can have more than one key. The decoder returns the key that
 * `list()` of Valkeyrie 0.8 returned for the row.
 *
 * @param hash - The hex key hash in key format 1.
 * @returns The key.
 * @throws {Error} If the hash is not a valid key in key format 1.
 */
function decodeLegacyKeyHash(hash: string): Key {
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
 * Turns the key hash of a row in key format 1 (Valkeyrie 0.8 and earlier)
 * into the key hash of the same key in the current key format. Use it to
 * migrate the rows of a custom driver into a new, empty store.
 *
 * Key format 1 does not escape a 0x00 byte inside a string or Uint8Array part,
 * so some hashes have more than one key. This function uses the key that
 * `list()` of Valkeyrie 0.8 returned for the row.
 *
 * @param legacyKeyHash - The hex key hash in key format 1.
 * @returns The hex key hash in the current key format.
 * @throws {Error} If the hash is not a valid key in key format 1, or the key
 * is longer than 2048 bytes in the current key format.
 */
export function migrateKeyHash(legacyKeyHash: string): string {
  return encodeKey(decodeLegacyKeyHash(legacyKeyHash), 'write').toString('hex')
}

/**
 * Reads all rows of the source store in key order, one page at a time.
 *
 * @param source - The source database.
 * @returns The rows.
 */
function* readRows(source: DatabaseSync): Generator<SourceRow> {
  const page = source.prepare(
    'SELECT key_hash, value, versionstamp, expires_at FROM kv_store WHERE key_hash > ? ORDER BY key_hash ASC LIMIT ?',
  )
  let after = ''
  while (true) {
    const rows = page.all(after, PAGE_SIZE) as unknown as SourceRow[]
    yield* rows
    const last = rows.at(-1)
    if (rows.length < PAGE_SIZE || last === undefined) return
    after = last.key_hash
  }
}

/**
 * Deletes a SQLite database file and its WAL files.
 *
 * @param path - The path of the database file.
 */
async function removeSqliteFiles(path: string): Promise<void> {
  await Promise.allSettled([
    unlink(path),
    unlink(`${path}-shm`),
    unlink(`${path}-wal`),
  ])
}

/**
 * Copies a SQLite store in key format 1 (Valkeyrie 0.8 and earlier) to a new
 * SQLite store in the current key format. Stop every process that writes the
 * source before you start: the migration reads the source once to check it,
 * then a second time to copy it.
 *
 * The migration does not change the source. It copies the value bytes, the
 * versionstamp and the expiry time of each row, so every value type of the
 * serializer stays the same. It gives each key its hash in the current key
 * format. Key format 1 does not escape a 0x00 byte inside a string or
 * Uint8Array part, so some hashes have more than one key. The migration uses
 * the key that `list()` of Valkeyrie 0.8 returned for the row.
 *
 * @param sourcePath - The path of the store in key format 1.
 * @param targetPath - The path of the new store. No file can exist there.
 * @param options - The serializer of the store, and what to do with rows that
 * cannot be migrated.
 * @returns The number of copied rows, the number of rows with a new key hash,
 * and the rows that were not copied.
 * @throws {Error} If the target exists, the source has a key format row, a row
 * cannot be migrated and `skipUnmigratable` is not set, or the migration cannot
 * create or write the target. The migration then leaves no target files, so
 * you can run it again with the same target path.
 */
export async function migrateSqliteKeyFormat(
  sourcePath: string,
  targetPath: string,
  options: MigrateSqliteKeyFormatOptions = {},
): Promise<MigrateSqliteKeyFormatResult> {
  if (existsSync(targetPath)) {
    throw new Error(
      `The target ${targetPath} exists. Give the path of a new file.`,
    )
  }
  const source = new DatabaseSync(sourcePath, { readOnly: true })
  try {
    if (
      source
        .prepare('SELECT key_hash FROM kv_store WHERE key_hash >= ? LIMIT 1')
        .get(KEY_HASH_END) !== undefined
    ) {
      throw new Error(
        `The source ${sourcePath} is not in key format 1: it has a key format row.`,
      )
    }

    const skipped: UnmigratableRow[] = []
    for (const row of readRows(source)) {
      try {
        migrateKeyHash(row.key_hash)
      } catch (error) {
        skipped.push({
          keyHash: row.key_hash,
          reason: error instanceof Error ? error.message : String(error),
        })
      }
    }
    if (skipped.length > 0 && options.skipUnmigratable !== true) {
      const listed = skipped
        .slice(0, 10)
        .map((row) => `${row.keyHash} (${row.reason})`)
        .join(', ')
      const more =
        skipped.length > 10 ? `, and ${skipped.length - 10} more` : ''
      throw new Error(
        `The migration cannot copy these rows: ${listed}${more}. Set skipUnmigratable to copy the other rows.`,
      )
    }
    const skippedHashes = new Set(skipped.map((row) => row.keyHash))

    const openOptions: { serializer?: () => Serializer } = {}
    if (options.serializer !== undefined) {
      openOptions.serializer = options.serializer
    }
    let target: DatabaseSync | undefined
    let migrated = 0
    let rekeyed = 0
    try {
      // An empty store gets the key format row with the serializer of the
      // store. The driver creates the file before it calls the serializer, so
      // this step can also fail after the file exists.
      const targetStore = await Valkeyrie.open(targetPath, openOptions)
      await targetStore.close()

      target = new DatabaseSync(targetPath)
      target.exec('BEGIN IMMEDIATE TRANSACTION')
      // INSERT, not INSERT OR REPLACE: two rows with one new hash stop the
      // migration instead of losing a row
      const insert = target.prepare(
        'INSERT INTO kv_store (key_hash, value, versionstamp, expires_at) VALUES (?, ?, ?, ?)',
      )
      for (const row of readRows(source)) {
        if (skippedHashes.has(row.key_hash)) continue
        const keyHash = migrateKeyHash(row.key_hash)
        insert.run(keyHash, row.value, row.versionstamp, row.expires_at)
        migrated++
        if (keyHash !== row.key_hash) rekeyed++
      }
      target.exec('COMMIT')
    } catch (error) {
      if (target !== undefined) {
        try {
          target.exec('ROLLBACK')
        } catch {
          // Ignore: no transaction is open
        }
        target.close()
      }
      await removeSqliteFiles(targetPath)
      throw error
    }
    target.close()
    return { migrated, rekeyed, skipped }
  } finally {
    source.close()
  }
}
