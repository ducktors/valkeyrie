import { decode, encode } from 'cbor-x'
import { KvU64 } from '../kv-u64.js'
import { type SerializedStruct, defineSerializer } from './serializer.js'

/**
 * CBOR-X serializer implementation using cbor-x
 *
 * This serializer uses the cbor-x library for CBOR-X serialization.
 * CBOR-X is an efficient binary serialization format similar to JSON but faster and smaller.
 *
 * Supported types include:
 * - Basic types: String, Number, Boolean, null, undefined, Array, Object
 * - Binary data: Uint8Array, Buffer
 * - Date objects
 * - Map and Set objects
 * - BigInt values
 */
export const cborXSerializer = defineSerializer({
  serialize: (value: unknown): Uint8Array => {
    const isU64 = value instanceof KvU64 ? 1 : 0

    const serialized = encode({
      value: isU64 ? (value as KvU64).value : value,
      isU64,
    } satisfies SerializedStruct)

    if (serialized.length > 65536 + 23) {
      throw new TypeError('Value too large (max 65536 bytes)')
    }

    return serialized
  },

  deserialize: (value: Uint8Array): unknown => {
    const { value: deserialized, isU64 } = decode(value) as SerializedStruct

    if (isU64) {
      return new KvU64(deserialized as bigint)
    }

    return deserialized
  },
})
