import { deserialize, serialize } from 'node:v8'
import { KvU64 } from '../kv-u64.js'
import { defineSerializer } from './serializer.js'

/**
 * Default serializer implementation using Node.js V8 serialization
 */
export const v8Serializer = defineSerializer({
  serialize: (value: unknown) => {
    const isU64 = value instanceof KvU64 ? 1 : 0
    const serialized = serialize(isU64 ? (value as KvU64).value : value)

    if (serialized.length > 65536 + 7) {
      throw new TypeError('Value too large (max 65536 bytes)')
    }
    return {
      serialized,
      isU64,
    }
  },

  deserialize: (value: Uint8Array, isU64: number) => {
    const deserialized = deserialize(value)
    if (isU64) {
      return new KvU64(deserialized)
    }
    return deserialized
  },
})
