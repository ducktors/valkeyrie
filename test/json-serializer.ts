import assert from 'node:assert'
import { describe, test } from 'node:test'
import { KvU64 } from '../src/kv-u64.js'
import { jsonSerializer } from '../src/serializers/json.js'
import { type Key, type Mutation, Valkeyrie } from '../src/valkeyrie.js'
describe('json serializer', async () => {
  async function dbTest(
    name: string,
    fn: (db: Valkeyrie) => Promise<void> | void,
  ) {
    await test(name, async () => {
      const db: Valkeyrie = await Valkeyrie.open(':memory:', {
        serializer: jsonSerializer,
      })
      try {
        await fn(db)
      } finally {
        await db.close()
      }
    })
  }

  const ZERO_VERSIONSTAMP = '00000000000000000000'

  await dbTest('basic read-write-delete and versionstamps', async (db) => {
    const result1 = await db.get(['a'])
    assert.deepEqual(result1.key, ['a'])
    assert.deepEqual(result1.value, null)
    assert.deepEqual(result1.versionstamp, null)

    const setRes = await db.set(['a'], 'b')
    assert.ok(setRes.ok)
    assert.ok(setRes.versionstamp > ZERO_VERSIONSTAMP)
    const result2 = await db.get(['a'])
    assert.deepEqual(result2.key, ['a'])
    assert.deepEqual(result2.value, 'b')
    assert.deepEqual(result2.versionstamp, setRes.versionstamp)

    const setRes2 = await db.set(['a'], 'c')
    assert.ok(setRes2.ok)
    assert.ok(setRes2.versionstamp > setRes.versionstamp)
    const result3 = await db.get(['a'])
    assert.deepEqual(result3.key, ['a'])
    assert.deepEqual(result3.value, 'c')
    assert.deepEqual(result3.versionstamp, setRes2.versionstamp)

    await db.delete(['a'])
    const result4 = await db.get(['a'])
    assert.deepEqual(result4.key, ['a'])
    assert.deepEqual(result4.value, null)
    assert.deepEqual(result4.versionstamp, null)
  })

  await describe('set and get value cases', async () => {
    const VALUE_CASES: { name: string; value: unknown }[] = [
      { name: 'string', value: 'hello' },
      { name: 'number', value: 42 },
      { name: 'boolean', value: true },
      { name: 'null', value: null },
      { name: 'undefined', value: undefined },
      { name: 'array', value: [1, 2, 3] },
      { name: 'object', value: { a: 1, b: 2 } },
      {
        name: 'nested array',
        value: [
          [1, 2],
          [3, 4],
        ],
      },
      { name: 'nested object', value: { a: { b: 1 } } },
    ]

    for (const { name, value } of VALUE_CASES) {
      await dbTest(`set and get ${name} value`, async (db) => {
        await db.set(['a'], value)
        const result = await db.get(['a'])
        assert.deepEqual(result.key, ['a'])
        assert.deepEqual(result.value, value)
      })
    }

    await dbTest('serialize and deserialize KvU64', async (db) => {
      const value = new KvU64(42n)
      await db.set(['kvu64'], value)
      const result = await db.get(['kvu64'])
      assert.deepEqual(result.value, value)

      // Test large values
      const largeValue = new KvU64(0xffffffffffffffffn)
      await db.set(['large-kvu64'], largeValue)
      const largeResult = await db.get(['large-kvu64'])
      assert.deepEqual(largeResult.value, largeValue)
    })

    await dbTest('handle undefined values', async (db) => {
      // JSON serialization removes undefined when serialized
      const objWithUndefined = { a: undefined, b: 'defined' }
      await db.set(['undefined'], objWithUndefined)
      const result = await db.get(['undefined'])
      assert.deepEqual(result.value, { b: 'defined' })
    })

    await dbTest('handle Date values', async (db) => {
      const date = new Date(0)
      // JSON serialization transforms Date values to ISO strings
      await db.set(['date'], date)
      const result = await db.get(['date'])
      assert.deepEqual(result.value, date.toISOString())
    })
  })

  await describe('invalid value cases', async () => {
    await dbTest('set and get recursive object (invalid)', async (db) => {
      // biome-ignore lint/suspicious/noExplicitAny: testing
      const value: any = { a: undefined }
      value.a = value

      // Expect an error when trying to serialize a circular structure
      await assert.rejects(
        async () => await db.set(['a'], value),
        TypeError,
        'circular',
      )

      // Verify the key doesn't exist
      const result = await db.get(['a'])
      assert.deepEqual(result.key, ['a'])
      assert.equal(result.value, null)
    })

    await dbTest('set and get bigint (invalid)', async (db) => {
      const value = { a: 42n }

      // Expect an error when trying to serialize a circular structure
      await assert.rejects(async () => await db.set(['a'], value), TypeError)

      // Verify the key doesn't exist
      const result = await db.get(['a'])
      assert.deepEqual(result.key, ['a'])
      assert.equal(result.value, null)
    })

    await dbTest(
      'set and get Uint8Array (transformed to a not empty object)',
      async (db) => {
        const value = new Uint8Array([1, 2, 3])

        await db.set(['a'], value)

        const result = await db.get(['a'])
        assert.deepEqual(result.key, ['a'])
        assert.deepEqual(result.value, { 0: 1, 1: 2, 2: 3 })
      },
    )

    const INVALID_VALUES_TRANSFORMED_TO_UNDEFINED = [
      { name: 'function', value: () => {} },
      { name: 'symbol', value: Symbol() },
    ]

    for (const { name, value } of INVALID_VALUES_TRANSFORMED_TO_UNDEFINED) {
      await dbTest(
        `set and get ${name} value (transformed to undefined)`,
        async (db) => {
          await db.set(['a'], value)
          const res = await db.get(['a'])
          assert.deepEqual(res.key, ['a'])
          assert.deepEqual(res.value, undefined)
        },
      )
    }

    const INVALID_VALUE_CASES = [
      { name: 'WeakMap', value: new WeakMap() },
      { name: 'WeakSet', value: new WeakSet() },
      { name: 'SharedArrayBuffer', value: new SharedArrayBuffer(3) },
      { name: 'ArrayBuffer', value: new ArrayBuffer(3) },
      {
        name: 'Map',
        value: new Map<string | number, string | number>([
          ['key1', 'value1'],
          [1, 42],
        ]),
      },
      {
        name: 'Set',
        value: new Set<string | number | boolean>(['value1', 42, true]),
      },
      { name: 'RegExp', value: /pattern/gi },
    ]

    for (const { name, value } of INVALID_VALUE_CASES) {
      await dbTest(
        `set and get ${name} value (transformed to {})`,
        async (db) => {
          await db.set(['a'], value)
          const res = await db.get(['a'])
          assert.deepEqual(res.key, ['a'])
          assert.deepEqual(res.value, {})
        },
      )
    }
  })

  await dbTest('get many', async (db) => {
    const res = await db
      .atomic()
      .set(['a'], -1)
      .set(['a', 'a'], 0)
      .set(['a', 'b'], 1)
      .set(['a', 'c'], 2)
      .set(['a', 'd'], 3)
      .set(['a', 'e'], 4)
      .set(['b'], 99)
      .set(['b', 'a'], 100)
      .commit()
    assert(res.ok)
    const entries = await db.getMany([['b', 'a'], ['a'], ['c']])
    assert.deepEqual(entries, [
      { key: ['b', 'a'], value: 100, versionstamp: res.versionstamp },
      { key: ['a'], value: -1, versionstamp: res.versionstamp },
      { key: ['c'], value: null, versionstamp: null },
    ])
  })

  await dbTest('value size limit', async (db) => {
    // For JSON serializer, the limit is based on the serialized JSON string size
    const smallValue = 'a'.repeat(65535)
    const largeValue = 'a'.repeat(65536)

    const res = await db.set(['a'], smallValue)
    assert.deepEqual(await db.get(['a']), {
      key: ['a'],
      value: smallValue,
      versionstamp: res.versionstamp,
    })

    await assert.rejects(
      async () => await db.set(['b'], largeValue),
      TypeError,
      'Value too large (max 65536 bytes)',
    )
  })

  await dbTest('operation size limit', async (db) => {
    const lastValidKeys: Key[] = new Array(10).fill(0).map((_, i) => ['a', i])
    const firstInvalidKeys: Key[] = new Array(11)
      .fill(0)
      .map((_, i) => ['a', i])
    const invalidCheckKeys: Key[] = new Array(101)
      .fill(0)
      .map((_, i) => ['a', i])

    const res = await db.getMany(lastValidKeys)
    assert.deepEqual(res.length, 10)

    await assert.rejects(
      async () => await db.getMany(firstInvalidKeys),
      TypeError,
      'Too many ranges (max 10)',
    )

    const res2 = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { batchSize: 1000 }),
    )
    assert.deepEqual(res2.length, 0)

    await assert.rejects(
      async () =>
        await Array.fromAsync(db.list({ prefix: ['a'] }, { batchSize: 1001 })),
      TypeError,
      'Too many entries (max 1000)',
    )

    // when batchSize is not specified, limit is used but is clamped to 500
    assert.deepEqual(
      (await Array.fromAsync(db.list({ prefix: ['a'] }, { limit: 1001 })))
        .length,
      0,
    )

    const res3 = await db
      .atomic()
      .check(
        ...lastValidKeys.map((key) => ({
          key,
          versionstamp: null,
        })),
      )
      .mutate(
        ...lastValidKeys.map(
          (key) =>
            ({
              key,
              type: 'set',
              value: 1,
            }) satisfies Mutation,
        ),
      )
      .commit()
    assert(res3)

    await assert.rejects(
      async () => {
        await db
          .atomic()
          .check(
            ...invalidCheckKeys.map((key) => ({
              key,
              versionstamp: null,
            })),
          )
          .mutate(
            ...lastValidKeys.map(
              (key) =>
                ({
                  key,
                  type: 'set',
                  value: 1,
                }) satisfies Mutation,
            ),
          )
          .commit()
      },
      TypeError,
      'Too many checks (max 100)',
    )

    const validMutateKeys: Key[] = new Array(1000)
      .fill(0)
      .map((_, i) => ['a', i])
    const invalidMutateKeys: Key[] = new Array(1001)
      .fill(0)
      .map((_, i) => ['a', i])

    const res4 = await db
      .atomic()
      .check(
        ...lastValidKeys.map((key) => ({
          key,
          versionstamp: null,
        })),
      )
      .mutate(
        ...validMutateKeys.map(
          (key) =>
            ({
              key,
              type: 'set',
              value: 1,
            }) satisfies Mutation,
        ),
      )
      .commit()
    assert(res4)

    await assert.rejects(
      async () => {
        await db
          .atomic()
          .check(
            ...lastValidKeys.map((key) => ({
              key,
              versionstamp: null,
            })),
          )
          .mutate(
            ...invalidMutateKeys.map(
              (key) =>
                ({
                  key,
                  type: 'set',
                  value: 1,
                }) satisfies Mutation,
            ),
          )
          .commit()
      },
      TypeError,
      'Too many mutations (max 1000)',
    )
  })

  await dbTest('total mutation size limit', async (db) => {
    const keys: Key[] = new Array(1000).fill(0).map((_, i) => ['a', i])

    const atomic = db.atomic()
    for (const key of keys) {
      atomic.set(key, 'foo')
    }
    const res = await atomic.commit()
    assert(res)

    // Use bigger values to trigger "total mutation size too large" error
    await assert.rejects(
      async () => {
        const value = new Array(3000).fill('a').join('')
        const atomic = db.atomic()
        for (const key of keys) {
          atomic.set(key, value)
        }
        await atomic.commit()
      },
      TypeError,
      'Total mutation size too large (max 819200 bytes)',
    )
  })
})
