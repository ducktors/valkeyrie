import { Packr } from 'msgpackr'
import { KvU64 } from '../kv-u64.js'
import { type SerializedStruct, defineSerializer } from './serializer.js'

const packr = new Packr({ moreTypes: true })

/**
 * MessagePack serializer implementation using msgpackr
 *
 * This serializer uses the msgpackr library for MessagePack serialization.
 * MessagePack is an efficient binary serialization format similar to JSON but faster and smaller.
 *
 * Supported types include:
 * - Basic types: String, Number, Boolean, null, undefined, Array, Object
 * - Binary data: Uint8Array, Buffer
 * - Date objects
 * - Map and Set objects
 * - BigInt values
 */
export const msgpackrSerializer = defineSerializer({
  serialize: (value: unknown): Uint8Array => {
    const isU64 = value instanceof KvU64 ? 1 : 0

    const serialized = packr.pack({
      value: isU64 ? (value as KvU64).value : value,
      isU64,
    } satisfies SerializedStruct)
    if (serialized.length > 65536 + 24) {
      throw new TypeError('Value too large (max 65536 bytes)')
    }

    return serialized
  },

  deserialize: (value: Uint8Array): unknown => {
    const { value: deserialized, isU64 } = packr.unpack(
      value,
    ) as SerializedStruct

    if (isU64) {
      return new KvU64(deserialized as bigint)
    }

    return deserialized
  },
})
