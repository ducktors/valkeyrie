/**
 * Example demonstrating how to use different serializers with Valkeyrie
 */

import { KvU64 } from '../src/kv-u64.js'
import { jsonSerializer } from '../src/serializers/json.js'
import { v8Serializer } from '../src/serializers/v8.js'
import { Valkeyrie } from '../src/valkeyrie.js'

async function main() {
  console.log('Valkeyrie Serializers Example')
  console.log('============================')

  // Example 1: Using the default V8 serializer (implicit)
  console.log('\n1. Using default V8 serializer:')
  const dbDefault = await Valkeyrie.open('./data/default.db')

  // Store some data
  await dbDefault.set(['users', 'user1'], {
    name: 'John Doe',
    age: 30,
    createdAt: new Date(),
    tags: new Set(['admin', 'active']),
    metadata: new Map([['lastLogin', new Date()]]),
  })

  // Retrieve the data
  const user1 = await dbDefault.get(['users', 'user1'])
  console.log('Retrieved with V8 serializer:', user1.value)

  await dbDefault.close()

  // Example 2: Explicitly using the V8 serializer
  console.log('\n2. Explicitly using V8 serializer:')
  const dbV8 = await Valkeyrie.open('./data/v8.db', {
    serializer: v8Serializer,
  })

  // Store some data with a KvU64
  await dbV8.set(['counters', 'visits'], new KvU64(1000n))

  // Retrieve the data
  const counter = await dbV8.get(['counters', 'visits'])
  console.log('Retrieved counter with V8 serializer:', counter.value)

  await dbV8.close()

  // Example 3: Using the JSON serializer
  console.log('\n3. Using JSON serializer:')
  const dbJson = await Valkeyrie.open('./data/json.db', {
    serializer: jsonSerializer,
  })

  // Store the same data as in Example 1
  await dbJson.set(['users', 'user1'], {
    name: 'John Doe',
    age: 30,
    createdAt: new Date(),
    tags: new Set(['admin', 'active']),
    metadata: new Map([['lastLogin', new Date()]]),
  })

  // Retrieve the data
  const user2 = await dbJson.get(['users', 'user1'])
  console.log('Retrieved with JSON serializer:', user2.value)

  // Store a KvU64 with JSON serializer
  await dbJson.set(['counters', 'visits'], new KvU64(1000n))

  // Retrieve the KvU64
  const jsonCounter = await dbJson.get(['counters', 'visits'])
  console.log('Retrieved counter with JSON serializer:', jsonCounter.value)

  await dbJson.close()

  console.log('\nExample completed!')
}

main().catch(console.error)
