# Valkeyrie Serializers

Valkeyrie supports pluggable serializers that allow you to customize how values are stored in the database. This document explains how to use the built-in serializers and how to create your own custom serializers.

## Built-in Serializers

Valkeyrie comes with three built-in serializers:

### V8 Serializer (Default)

The V8 serializer uses Node.js's built-in `node:v8` module to serialize and deserialize values. This is the default serializer used by Valkeyrie.

**Features:**
- Supports most JavaScript data types
- Preserves object references and circular references
- Efficient binary format

**Limitations:**
- The serialized data is not human-readable
- Not compatible with other programming languages
- Tied to the specific V8 version
- Unsupported types: WeakMap, WeakSet, Function, Symbol, SharedArrayBuffer

**Supported Types:**
- String
- Number
- Boolean
- null
- undefined
- Date
- RegExp
- Array
- Object
- Map
- Set
- Uint8Array/ArrayBuffer
- BigInt
- KvU64 (Valkeyrie's 64-bit unsigned integer type)

### JSON Serializer

The JSON serializer uses JSON.stringify/parse with additional handling for special types that JSON doesn't natively support.

**Features:**
- Human-readable format
- Compatible with other programming languages
- Can be inspected and debugged easily

**Limitations:**
- Does not support circular references (will throw an error)
- Less efficient than V8 serializer for complex objects
- **Circular References**: The JSON serializer cannot handle circular references. If you try to store an object with circular 
references, it will throw an error.
- **Unsupported Types**: Some JavaScript types are not supported by JSON, such as `Function`, `Symbol`, `WeakMap`, `WeakSet`, 
and `SharedArrayBuffer`. These types will be serialized as `null`.
- **Binary Data Size**: When storing binary data (like `Uint8Array` or `ArrayBuffer`), be aware that the JSON serializer 
encodes this data as base64 strings, which increases the size by approximately 33%. This means that a binary value that is 
close to the 65KB limit might exceed the limit when serialized to JSON.
- Unsupported types: Function, Symbol, WeakMap, WeakSet, SharedArrayBuffer (these will be serialized as null)

**Supported Types:**
- String
- Number
- Boolean
- null
- Array
- Object
- Date
- Map
- Set
- Uint8Array/ArrayBuffer
- BigInt
- KvU64

### BSON Serializer

The BSON serializer uses the MongoDB BSON format for serialization, providing an efficient binary encoding with support for a variety of data types.

**Features:**
- Efficient binary encoding
- Support for MongoDB-specific types
- Preserves type information

**Limitations:**
- Unsupported types: Function, Symbol, WeakMap, WeakSet, SharedArrayBuffer
- The maximum serialized size is limited to 65536 bytes
- **Circular References**: The BSON serializer cannot handle circular references. If you try to store an object with circular 
references, it will throw an error.

**Supported Types:**
- String
- Number
- Boolean
- null
- undefined
- Date
- RegExp
- Array
- Object
- Map
- Set
- Uint8Array/ArrayBuffer
- BigInt
- KvU64

## Creating Custom Serializers

You can create your own custom serializers by implementing the `Serializer` interface:

```typescript
import { defineSerializer, type Serializer } from 'valkeyrie'
import { KvU64 } from 'valkeyrie'

// Create a custom serializer
export const myCustomSerializer = defineSerializer({
  serialize: (value: unknown) => {
    // Implement your serialization logic here
    // Must return { serialized: Uint8Array, isU64: number }
    
    // Example implementation:
    const isU64 = value instanceof KvU64 ? 1 : 0
    const serialized = /* your serialization logic */
    
    return {
      serialized,
      isU64,
    }
  },

  deserialize: (value: Uint8Array, isU64: number) => {
    // Implement your deserialization logic here
    // Must return the original value
    
    // Example implementation:
    if (isU64) {
      // Handle KvU64 specially
      return new KvU64(/* deserialized bigint value */)
    }
    
    // Handle regular values
    return /* your deserialization logic */
  }
})

// Use your custom serializer
const db = await Valkeyrie.open('./data/custom.db', {
  serializer: myCustomSerializer
})
```

## Serializer Interface

The `Serializer` interface is defined as follows:

```typescript
export interface Serializer {
  /**
   * Serializes a value to a binary format
   * @param value The value to serialize
   * @returns A tuple containing the serialized value and a flag indicating if it's a KvU64
   */
  serialize: (value: unknown) => { serialized: Uint8Array; isU64: number }

  /**
   * Deserializes a binary value back to its original form
   * @param value The binary value to deserialize
   * @param isU64 Flag indicating if the value is a KvU64
   * @returns The deserialized value
   */
  deserialize: (value: Uint8Array, isU64: number) => unknown
}
```

## Best Practices

- Choose the serializer based on your specific needs:
  - Use the V8 serializer for maximum performance and compatibility with most JavaScript types
  - Use the JSON serializer for human-readable storage or cross-language compatibility
  - Use the BSON serializer for efficient binary encoding with MongoDB compatibility
  - Create a custom serializer for specialized needs (e.g., compression, encryption)
- Be consistent with your serializer choice for a given database
- Consider the trade-offs between storage size, performance, and compatibility
- If you need to store objects with circular references, use the V8