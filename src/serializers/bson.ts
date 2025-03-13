import { BSON } from 'bson'
import { KvU64 } from '../kv-u64.js'
import { defineSerializer } from './serializer.js'

/**
 * BSON serializer implementation
 *
 * This serializer uses the MongoDB BSON format for serialization.
 * BSON (Binary JSON) is a binary-encoded serialization format that supports a variety of data types
 * including:
 * - Basic types: String, Number, Boolean, Null, Array, Object
 * - Special types: Date, RegExp, Binary data, ObjectId
 * - Extended types: Int32, Int64, Decimal128, Timestamp, etc.
 */
export const bsonSerializer = defineSerializer({
  serialize: (value: unknown) => {
    try {
      // BSON only supports objects and arrays as root values, so we need to wrap primitives
      const seen = new WeakSet()
      const preparedValue = prepareForBsonSerialization(value, seen)

      // Wrap the value in an object to ensure BSON can serialize it
      const wrappedValue = { value: preparedValue }
      const serialized = BSON.serialize(wrappedValue)

      if (serialized.length > 65536) {
        throw new TypeError('Value too large (max 65536 bytes)')
      }

      return {
        serialized: Buffer.from(serialized),
        isU64: 0,
      }
    } catch (error) {
      if (error instanceof Error) {
        throw new TypeError(`BSON serialization error: ${error.message}`)
      }
      throw error
    }
  },

  deserialize: (value: Uint8Array, isU64: number) => {
    const bsonBuffer = Buffer.from(value)
    const deserialized = BSON.deserialize(bsonBuffer)

    if (isU64) {
      // Convert BSON Long back to KvU64
      const longValue = deserialized.value
      // BSON Long doesn't have toBigInt method in some versions, so we convert manually
      const bigintValue = BigInt(longValue.toString())
      return new KvU64(bigintValue)
    }

    // For regular values, restore special types
    return restoreFromBsonDeserialization(deserialized.value)
  },
})

/**
 * Prepares a value for BSON serialization by converting unsupported types
 * to supported ones with special markers.
 *
 * @param value The value to prepare
 * @param seen WeakSet to track already seen objects (for circular reference detection)
 * @returns A BSON-serializable value
 */
function prepareForBsonSerialization(
  value: unknown,
  seen = new WeakSet(),
): unknown {
  // Handle primitive types
  if (
    typeof value === 'string' ||
    typeof value === 'number' ||
    typeof value === 'boolean' ||
    value === null ||
    value === undefined
  ) {
    return value
  }

  // Handle BigInt
  if (typeof value === 'bigint') {
    return { $type: 'bigint', value: value.toString() }
  }

  if (typeof value === 'function') {
    throw new TypeError('Cannot serialize function')
  }

  if (typeof value === 'symbol') {
    throw new TypeError('Cannot serialize symbol')
  }

  if (value instanceof Date) {
    // BSON has native Date support, but we need to wrap it to ensure proper handling
    return { $type: 'date', value: value }
  }

  if (value instanceof RegExp) {
    // BSON has native RegExp support, but we need to wrap it to ensure proper handling
    return {
      $type: 'regexp',
      pattern: value.source,
      flags: value.flags,
    }
  }

  if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
    // Convert to BSON Binary
    const buffer = value instanceof ArrayBuffer ? new Uint8Array(value) : value
    // Add a marker to distinguish between Uint8Array and ArrayBuffer
    return {
      $type: value instanceof ArrayBuffer ? 'arraybuffer' : 'binary',
      value: new BSON.Binary(buffer),
    }
  }

  // Handle objects
  if (typeof value === 'object' && value !== null) {
    // Check for circular references
    if (seen.has(value)) {
      throw new TypeError('Cannot serialize circular structure')
    }

    // Add this object to seen objects
    seen.add(value)

    // Check for unsupported types
    if (
      value instanceof WeakMap ||
      value instanceof WeakSet ||
      value instanceof SharedArrayBuffer
    ) {
      throw new TypeError(`Cannot serialize ${value.constructor.name}`)
    }

    if (value instanceof Map) {
      // Convert Map to an object with special marker
      const entries = Array.from(value.entries()).map(([k, v]) => {
        // Keys in BSON must be strings, so we need to handle non-string keys
        const keyStr = typeof k === 'string' ? k : JSON.stringify(k)
        return [keyStr, prepareForBsonSerialization(v, seen)]
      })
      return { $type: 'map', value: Object.fromEntries(entries) }
    }

    if (value instanceof Set) {
      // Convert Set to an array with special marker
      return {
        $type: 'set',
        value: Array.from(value).map((v) =>
          prepareForBsonSerialization(v, seen),
        ),
      }
    }

    if (value instanceof KvU64) {
      // Convert KvU64 to a special marker
      return { $type: 'kvu64', value: value.toString() }
    }

    if (Array.isArray(value)) {
      // Process arrays recursively
      return value.map((item) => prepareForBsonSerialization(item, seen))
    }

    // Process objects recursively
    const result: Record<string, unknown> = {}
    for (const [key, val] of Object.entries(value)) {
      result[key] = prepareForBsonSerialization(val, seen)
    }
    return result
  }

  // If we get here, we have an unsupported type
  throw new TypeError(`Cannot serialize ${typeof value}`)
}

/**
 * Restores special types from BSON deserialization
 */
function restoreFromBsonDeserialization(value: unknown): unknown {
  if (value === null) {
    return null
  }

  // Handle BSON Binary type
  if (value instanceof BSON.Binary) {
    return Buffer.from(value.buffer)
  }

  // Handle BSON Long type (not part of KvU64)
  if (value instanceof BSON.Long) {
    // Convert BSON Long to BigInt
    return BigInt(value.toString())
  }

  // Handle BSON Decimal128 type
  if (value instanceof BSON.Decimal128) {
    return value.toString()
  }

  if (Array.isArray(value)) {
    return value.map((item) => restoreFromBsonDeserialization(item))
  }

  if (typeof value === 'object' && value !== null) {
    const obj = value as Record<string, unknown>

    // Check for our special type markers
    if (obj.$type === 'undefined') {
      return undefined
    }

    if (obj.$type === 'bigint' && typeof obj.value === 'string') {
      // Convert string back to BigInt
      return BigInt(obj.value)
    }

    if (obj.$type === 'date' && obj.value instanceof Date) {
      return obj.value
    }

    if (
      obj.$type === 'regexp' &&
      typeof obj.pattern === 'string' &&
      typeof obj.flags === 'string'
    ) {
      return new RegExp(obj.pattern, obj.flags)
    }

    if (
      obj.$type === 'map' &&
      typeof obj.value === 'object' &&
      obj.value !== null
    ) {
      // Restore Map
      const mapEntries: Array<[unknown, unknown]> = []

      for (const [k, v] of Object.entries(
        obj.value as Record<string, unknown>,
      )) {
        // Try to restore non-string keys
        let key: unknown = k
        try {
          // If the key was JSON stringified, try to parse it
          if (
            k.startsWith('"') ||
            k.startsWith('[') ||
            k.startsWith('{') ||
            k === 'true' ||
            k === 'false' ||
            k === 'null' ||
            !Number.isNaN(Number(k))
          ) {
            key = JSON.parse(k)
          }
        } catch {
          // If parsing fails, use the string key as is
          key = k
        }
        mapEntries.push([key, restoreFromBsonDeserialization(v)])
      }

      return new Map(mapEntries)
    }

    if (obj.$type === 'set' && Array.isArray(obj.value)) {
      // Restore Set
      return new Set(
        (obj.value as unknown[]).map((v) => restoreFromBsonDeserialization(v)),
      )
    }

    if (obj.$type === 'binary' && obj.value instanceof BSON.Binary) {
      return Buffer.from((obj.value as BSON.Binary).buffer)
    }

    if (obj.$type === 'arraybuffer' && obj.value instanceof BSON.Binary) {
      const buffer = Buffer.from((obj.value as BSON.Binary).buffer)
      const arrayBuffer = new ArrayBuffer(buffer.length)
      const view = new Uint8Array(arrayBuffer)
      for (let i = 0; i < buffer.length; i++) {
        view[i] = buffer[i] as number
      }
      return arrayBuffer
    }

    if (obj.$type === 'kvu64' && typeof obj.value === 'string') {
      // Convert string back to KvU64
      return new KvU64(BigInt(obj.value))
    }

    // Regular object
    const result: Record<string, unknown> = {}
    for (const [key, val] of Object.entries(obj)) {
      result[key] = restoreFromBsonDeserialization(val)
    }
    return result
  }

  // Primitive values and native BSON types can be returned as is
  return value
}
