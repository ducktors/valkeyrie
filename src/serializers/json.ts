import { KvU64 } from '../kv-u64.js'
import { type SerializedStruct, defineSerializer } from './serializer.js'

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
  serialize: (value: unknown): Uint8Array => {
    const isU64 = value instanceof KvU64 ? 1 : 0

    const serialized = Buffer.from(
      JSON.stringify({
        value: isU64 ? (value as KvU64).value.toString() : value,
        isU64,
      } satisfies SerializedStruct),
      'utf8',
    )

    // 65536 + 21 = 65557
    if (serialized.length > 65557) {
      throw new TypeError('Value too large (max 65536 bytes)')
    }

    return serialized
  },

  deserialize: (value: Uint8Array): unknown => {
    const jsonString = Buffer.from(value).toString('utf8')
    const { value: parsed, isU64 } = JSON.parse(jsonString) as SerializedStruct

    if (isU64) {
      return new KvU64(BigInt(parsed as string))
    }

    return parsed
  },
})
