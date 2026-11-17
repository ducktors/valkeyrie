import type { Key, KeyPart } from './valkeyrie.ts'

/**
 * A hex list bound above every key hash. A key hash starts with a type marker
 * from 0x01 to 0x05, and no type marker is 0xFF.
 */
export const KEY_HASH_END = 'ff'

/**
 * Escapes each 0x00 byte of a string or Uint8Array key part as 0x00 0xFF, as
 * the FoundationDB tuple layer and Deno KV do. The 0x00 terminator then always
 * ends the part, so a part with a NUL byte cannot alias another key, and keys
 * keep their byte order.
 *
 * @param bytes - The bytes of the key part.
 * @returns The escaped bytes.
 */
function escapeNulBytes(bytes: Uint8Array): Buffer {
  const nulCount = bytes.reduce(
    (count, byte) => (byte === 0x00 ? count + 1 : count),
    0,
  )
  if (nulCount === 0) return Buffer.from(bytes)
  const escaped = Buffer.alloc(bytes.length + nulCount)
  let pos = 0
  for (const byte of bytes) {
    escaped[pos++] = byte
    if (byte === 0x00) escaped[pos++] = 0xff
  }
  return escaped
}

/**
 * Reads an escaped string or Uint8Array key part up to its 0x00 terminator,
 * and turns each 0x00 0xFF back into 0x00. No type marker is 0xFF, so a 0x00
 * before 0xFF is always an escaped byte.
 *
 * @param buffer - The encoded key.
 * @param start - The position of the first byte of the part.
 * @param type - The type of the part, for the error message.
 * @returns The bytes of the part and the position after its terminator.
 * @throws {Error} If the part has no terminator.
 */
function readEscapedPart(
  buffer: Buffer,
  start: number,
  type: 'Uint8Array' | 'String',
): { bytes: Buffer; next: number } {
  const chunks: Buffer[] = []
  let chunkStart = start
  let pos = start
  while (pos < buffer.length) {
    if (buffer[pos] !== 0x00) {
      pos++
    } else if (buffer[pos + 1] === 0xff) {
      chunks.push(buffer.subarray(chunkStart, pos + 1))
      pos += 2
      chunkStart = pos
    } else {
      chunks.push(buffer.subarray(chunkStart, pos))
      return { bytes: Buffer.concat(chunks), next: pos + 1 }
    }
  }
  throw new Error(`Invalid key hash: unterminated ${type}`)
}

/**
 * Generates a hash for a given key. This method is crucial for indexing and storing keys in the database.
 * It converts each part of the key into a specific byte format based on its type, following Deno.KV's encoding format.
 * The format for each type is as follows:
 * - Uint8Array: 0x01 + escaped bytes + 0x00
 * - String: 0x02 + escaped utf8 bytes + 0x00
 * - BigInt: 0x03 + 8 bytes int64 + 0x00
 * - Number: 0x04 + 8 bytes double + 0x00
 * - Boolean: 0x05 + single byte + 0x00
 *
 * An escaped part writes each 0x00 byte as 0x00 0xFF, so the 0x00 terminator
 * always ends the part.
 *
 * After converting each part, they are concatenated with a null byte delimiter to form the full key.
 * This method ensures that keys are consistently formatted and can be reliably hashed for storage and retrieval.
 * Note that key ordering is determined by a lexicographical comparison of their parts, with the first part being the most significant and the last part being the least significant. Additionally, key comparisons are case sensitive.
 *
 * @param {Key} key - The key to be hashed.
 * @param operation - The operation, for the key size limit and its error message.
 * @returns {Buffer} - The buffer representation of the hashed key.
 * @throws {TypeError} If the encoded key is longer than 2048 bytes.
 * @throws {Error} If a key part has an unsupported type.
 */
export function encodeKey(
  key: Key,
  operation: 'write' | 'read' = 'read',
): Buffer {
  const parts = key.map((part) => {
    let bytes: Buffer

    if (part instanceof Uint8Array) {
      // Uint8Array format: 0x01 + escaped bytes + 0x00
      const escaped = escapeNulBytes(part)
      bytes = Buffer.alloc(escaped.length + 2)
      bytes[0] = 0x01 // Uint8Array type marker
      escaped.copy(bytes, 1)
      bytes[bytes.length - 1] = 0x00
    } else if (typeof part === 'string') {
      // String format: 0x02 + escaped utf8 bytes + 0x00
      const strBytes = escapeNulBytes(Buffer.from(part, 'utf8'))
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
 * Decodes a hex key hash back into its original key parts.
 * This function reverses the encoding of encodeKey.
 * It handles the following formats:
 * - Uint8Array: 0x01 + escaped bytes + 0x00
 * - String: 0x02 + escaped utf8 bytes + 0x00
 * - BigInt: 0x03 + 8 bytes int64 + 0x00
 * - Number: 0x04 + 8 bytes double + 0x00
 * - Boolean: 0x05 + single byte + 0x00
 *
 * @param {string} hash - The base64-encoded key hash to decode
 * @returns {Key} The decoded key parts array
 * @throws {Error} If the hash format is invalid or contains an unknown type marker
 */
export function decodeKey(hash: string): Key {
  const buffer = Buffer.from(hash, 'hex')
  const parts: KeyPart[] = []
  let pos = 0

  while (pos < buffer.length) {
    const typeMarker = buffer[pos] as number
    pos++

    switch (typeMarker) {
      case 0x01: {
        // Uint8Array
        const { bytes, next } = readEscapedPart(buffer, pos, 'Uint8Array')
        parts.push(new Uint8Array(bytes))
        pos = next
        break
      }
      case 0x02: {
        // String
        const { bytes, next } = readEscapedPart(buffer, pos, 'String')
        parts.push(bytes.toString('utf8'))
        pos = next
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
