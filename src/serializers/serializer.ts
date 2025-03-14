export interface Serializer {
  /**
   * Serializes a value to a binary format
   * @param value The value to serialize
   * @returns A tuple containing the serialized value and a flag indicating if it's a KvU64
   */
  serialize: (value: unknown) => Uint8Array

  /**
   * Deserializes a binary value back to its original form
   * @param value The binary value to deserialize
   * @param isU64 Flag indicating if the value is a KvU64
   * @returns The deserialized value
   */
  deserialize: (value: Uint8Array) => unknown
}

/**
 * Helper function to define a serializer
 * @param initSerializer A function that returns a Serializer or a Serializer instance
 * @returns A function that returns a Promise resolving to a Serializer
 */
export function defineSerializer(
  initSerializer: (() => Serializer) | Serializer,
): () => Serializer {
  if (initSerializer instanceof Function) {
    return initSerializer
  }

  return () => initSerializer
}
