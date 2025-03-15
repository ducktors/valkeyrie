import { BSON } from 'bson'
import { KvU64 } from '../kv-u64.js'
import { type SerializedStruct, defineSerializer } from './serializer.js'

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
  serialize: (value: unknown): Uint8Array => {
    throwUnsupportedType(value)
    try {
      const isU64 = value instanceof KvU64 ? 1 : 0

      // Wrap the value in an object to ensure BSON can serialize it
      const wrappedValue = {
        value: {
          value: isU64 ? (value as KvU64).value.toString() : value,
          isU64,
        } satisfies SerializedStruct,
      }
      const serialized = BSON.serialize(wrappedValue)

      // 65536 + 40 bytes for the wrapper object + bson overhead
      if (serialized.length > 65576) {
        throw new TypeError('Value too large (max 65536 bytes)')
      }

      return serialized
    } catch (error) {
      if (error instanceof Error) {
        throw new TypeError(`BSON serialization error: ${error.message}`)
      }
      throw error
    }
  },

  deserialize: (value: Uint8Array): unknown => {
    const {
      value: { value: deserialized, isU64 },
    } = BSON.deserialize(value) as {
      value: SerializedStruct
    }

    if (isU64) {
      return new KvU64(BigInt(deserialized as string))
    }

    return deserialized
  },
})

function throwUnsupportedType(value: unknown) {
  switch (true) {
    case value instanceof Uint8Array:
      throw new TypeError('Uint8Array is not supported')
    case value instanceof ArrayBuffer:
      throw new TypeError('ArrayBuffer is not supported')
    case value instanceof SharedArrayBuffer:
      throw new TypeError('SharedArrayBuffer is not supported')
    case value instanceof WeakMap:
      throw new TypeError('WeakMap is not supported')
    case value instanceof WeakSet:
      throw new TypeError('WeakSet is not supported')
    case value instanceof Symbol || typeof value === 'symbol':
      throw new TypeError('Symbol is not supported')
    case value instanceof Function:
      throw new TypeError('Function is not supported')
    case value instanceof Map:
      throw new TypeError('Map is not supported')
    case typeof value === 'bigint':
      throw new TypeError('BigInt is not supported')
  }
}
