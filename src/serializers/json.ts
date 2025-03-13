import { KvU64 } from '../kv-u64.js'
import { defineSerializer } from './serializer.js'

/**
 * JSON serializer implementation
 *
 * Note: This serializer has limitations compared to the V8 serializer:
 * - Cannot serialize functions, symbols, or circular references
 * - BigInt values are converted to strings with a special prefix
 * - Uint8Array and other binary data are base64 encoded
 * - Recursive objects are not supported and will throw an error
 */
export const jsonSerializer = defineSerializer({
  serialize: (value: unknown) => {
    const isU64 = value instanceof KvU64 ? 1 : 0

    // Handle KvU64 specially
    if (isU64) {
      const u64Value = (value as KvU64).value
      // Store the bigint value directly
      const jsonValue = JSON.stringify({
        type: 'KvU64',
        value: u64Value.toString(),
      })
      return {
        serialized: Buffer.from(jsonValue, 'utf8'),
        isU64,
      }
    }

    // For regular values, we need to handle special types before serialization
    // Use a Set to track objects we've already seen to detect circular references
    const seen = new Set<object>()

    try {
      const preparedValue = prepareForSerialization(value, seen)
      const jsonValue = JSON.stringify(preparedValue)

      const serialized = Buffer.from(jsonValue, 'utf8')
      if (serialized.length > 65536) {
        throw new TypeError('Value too large (max 65536 bytes)')
      }

      return {
        serialized,
        isU64: 0,
      }
    } catch (error) {
      if (
        error instanceof Error &&
        error.message === 'Circular reference detected'
      ) {
        throw new TypeError('Cannot serialize object with circular references')
      }
      throw error
    }
  },

  deserialize: (value: Uint8Array, isU64: number) => {
    const jsonString = Buffer.from(value).toString('utf8')
    const parsed = JSON.parse(jsonString)

    if (isU64) {
      // Handle KvU64 specially
      return new KvU64(BigInt(parsed.value))
    }

    // For regular values, restore special types
    return restoreFromDeserialization(parsed)
  },
})

/**
 * Prepares a value for JSON serialization by handling special types
 * @param value The value to serialize
 * @param seen Set of already seen objects to detect circular references
 * @returns A serializable value
 */
function prepareForSerialization(value: unknown, seen: Set<object>): unknown {
  if (value === undefined) {
    return { type: 'undefined' }
  }

  if (value === null) {
    return null
  }

  if (typeof value === 'bigint') {
    return { type: 'bigint', value: value.toString() }
  }

  if (value instanceof Date) {
    return { type: 'date', value: value.toISOString() }
  }

  if (value instanceof Uint8Array || value instanceof ArrayBuffer) {
    const buffer = value instanceof ArrayBuffer ? new Uint8Array(value) : value
    return {
      type: value instanceof ArrayBuffer ? 'arraybuffer' : 'binary',
      value: Buffer.from(buffer).toString('base64'),
    }
  }

  // Handle objects and check for circular references
  if (typeof value === 'object' && value !== null) {
    // Check for unsupported types
    if (value instanceof WeakMap) {
      throw new TypeError('Cannot serialize WeakMap')
    }

    if (value instanceof WeakSet) {
      throw new TypeError('Cannot serialize WeakSet')
    }

    if (value instanceof SharedArrayBuffer) {
      throw new TypeError('Cannot serialize SharedArrayBuffer')
    }

    // Check if we've seen this object before (circular reference)
    if (seen.has(value as object)) {
      throw new Error('Circular reference detected')
    }

    // Add this object to our seen set
    seen.add(value as object)

    try {
      if (value instanceof Map) {
        return {
          type: 'map',
          value: Array.from(value.entries()).map(([k, v]) => [
            prepareForSerialization(k, seen),
            prepareForSerialization(v, seen),
          ]),
        }
      }

      if (value instanceof Set) {
        return {
          type: 'set',
          value: Array.from(value).map((v) => prepareForSerialization(v, seen)),
        }
      }

      if (value instanceof RegExp) {
        return {
          type: 'regexp',
          source: value.source,
          flags: value.flags,
        }
      }

      if (Array.isArray(value)) {
        return value.map((item) => prepareForSerialization(item, seen))
      }

      // Regular object
      const result: Record<string, unknown> = {}
      for (const [key, val] of Object.entries(
        value as Record<string, unknown>,
      )) {
        result[key] = prepareForSerialization(val, seen)
      }
      return result
    } finally {
      // Remove this object from the seen set when we're done with it
      // This allows the same object to appear multiple times in the structure
      // as long as it's not recursive
      seen.delete(value as object)
    }
  }

  // Primitive values (string, number, boolean) can be returned as is
  return value
}

/**
 * Restores special types from JSON deserialization
 */
function restoreFromDeserialization(value: unknown): unknown {
  if (value === null) {
    return null
  }

  if (Array.isArray(value)) {
    return value.map((item) => restoreFromDeserialization(item))
  }

  if (typeof value === 'object' && value !== null) {
    // Check if it's a special type object
    const obj = value as Record<string, unknown>

    if (typeof obj.type === 'string') {
      // It's a special type object
      const type = obj.type

      if (type === 'undefined') {
        return undefined
      }

      if (type === 'bigint' && typeof obj.value === 'string') {
        return BigInt(obj.value)
      }

      if (type === 'date' && typeof obj.value === 'string') {
        return new Date(obj.value)
      }

      if (type === 'binary' && typeof obj.value === 'string') {
        return Buffer.from(obj.value, 'base64')
      }

      if (type === 'arraybuffer' && typeof obj.value === 'string') {
        const buffer = Buffer.from(obj.value, 'base64')
        const arrayBuffer = new ArrayBuffer(buffer.length)
        const view = new Uint8Array(arrayBuffer)
        for (let i = 0; i < buffer.length; i++) {
          view[i] = buffer[i] as number
        }
        return arrayBuffer
      }

      if (type === 'map' && Array.isArray(obj.value)) {
        return new Map(
          (obj.value as Array<[unknown, unknown]>).map(([k, v]) => [
            restoreFromDeserialization(k),
            restoreFromDeserialization(v),
          ]),
        )
      }

      if (type === 'set' && Array.isArray(obj.value)) {
        return new Set(
          (obj.value as unknown[]).map((v) => restoreFromDeserialization(v)),
        )
      }

      if (
        type === 'regexp' &&
        typeof obj.source === 'string' &&
        typeof obj.flags === 'string'
      ) {
        return new RegExp(obj.source, obj.flags)
      }
    }

    // Regular object
    const result: Record<string, unknown> = {}
    for (const [key, val] of Object.entries(obj)) {
      result[key] = restoreFromDeserialization(val)
    }
    return result
  }

  // Primitive values (string, number, boolean) can be returned as is
  return value
}
