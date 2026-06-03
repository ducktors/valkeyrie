import assert, { AssertionError } from 'node:assert'
import { randomUUID } from 'node:crypto'
import { access, unlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, test } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import { inspect } from 'node:util'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Driver } from '../src/driver.ts'
import { KvU64 } from '../src/kv-u64.ts'
import { jsonSerializer } from '../src/serializers/json.ts'
import type { Serializer } from '../src/serializers/serializer.ts'
import { sqliteDriver } from '../src/sqlite-driver.ts'
import { ValkeyrieBuilder } from '../src/valkeyrie-builder.ts'
import {
  AtomicOperation,
  type EntryMaybe,
  type Key,
  type Mutation,
  Valkeyrie,
} from '../src/valkeyrie.ts'

/**
 * A spy that wraps the built-in in-memory SQLite driver so tests can assert the
 * supplied `driverFn` was actually invoked and used. Mirrors how a consumer
 * would provide a custom backend, without reimplementing the full Driver contract.
 */
type DriverSpy = {
  created: number
  serializerSeen: boolean
  setCalls: number
  getCalls: number
}

function createSpyDriverFn(
  spy: DriverSpy,
): (serializer?: () => Serializer) => Promise<Driver> {
  return async (serializer?: () => Serializer): Promise<Driver> => {
    spy.created += 1
    spy.serializerSeen = serializer !== undefined
    const inner = await sqliteDriver(':memory:', serializer)
    return {
      ...inner,
      set: (...args: Parameters<Driver['set']>) => {
        spy.setCalls += 1
        return inner.set(...args)
      },
      get: (...args: Parameters<Driver['get']>) => {
        spy.getCalls += 1
        return inner.get(...args)
      },
    }
  }
}

describe('test valkeyrie', async () => {
  async function dbTest(
    name: string,
    fn: (db: Valkeyrie) => Promise<void> | void,
  ) {
    await test(name, async () => {
      const db: Valkeyrie = await Valkeyrie.open()
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

  const VALUE_CASES: { name: string; value: unknown }[] = [
    { name: 'string', value: 'hello' },
    { name: 'number', value: 42 },
    { name: 'bigint', value: 42n },
    { name: 'boolean', value: true },
    { name: 'null', value: null },
    { name: 'undefined', value: undefined },
    { name: 'Date', value: new Date(0) },
    { name: 'Uint8Array', value: new Uint8Array([1, 2, 3]) },
    { name: 'ArrayBuffer', value: new ArrayBuffer(3) },
    { name: 'array', value: [1, 2, 3] },
    { name: 'object', value: { a: 1, b: 2 } },
    {
      name: 'Map',
      value: new Map([
        ['a', 1],
        ['b', 2],
      ]),
    },
    { name: 'Set', value: new Set([1, 2, 3]) },
    {
      name: 'nested array',
      value: [
        [1, 2],
        [3, 4],
      ],
    },
  ]

  for (const { name, value } of VALUE_CASES.concat({
    name: 'nested object',
    value: VALUE_CASES.reduce<Record<string, unknown>>((acc, curr) => {
      acc[curr.name] = curr.value
      return acc
    }, {}),
  })) {
    await dbTest(`set and get ${name} value`, async (db) => {
      await db.set(['a'], value)
      const result = await db.get(['a'])
      assert.deepEqual(result.key, ['a'])
      assert.deepEqual(result.value, value)
    })
  }

  await dbTest('set and get recursive object', async (db) => {
    // biome-ignore lint/suspicious/noExplicitAny: testing
    const value: any = { a: undefined }
    value.a = value
    await db.set(['a'], value)
    const result = await db.get(['a'])
    assert.deepEqual(result.key, ['a'])

    // biome-ignore lint/suspicious/noExplicitAny: testing
    const resultValue: any = result.value
    assert(resultValue.a === resultValue)
  })

  // invalid values (as per structured clone algorithm with _for storage_, NOT JSON)
  const INVALID_VALUE_CASES = [
    { name: 'function', value: () => {} },
    { name: 'symbol', value: Symbol() },
    { name: 'WeakMap', value: new WeakMap() },
    { name: 'WeakSet', value: new WeakSet() },
    {
      name: 'SharedArrayBuffer',
      value: new SharedArrayBuffer(3),
    },
  ]

  for (const { name, value } of INVALID_VALUE_CASES) {
    await dbTest(`set and get ${name} value (invalid)`, async (db) => {
      // @ts-ignore - we are testing invalid values
      await assert.rejects(async () => await db.set(['a'], value), Error)
      const res = await db.get(['a'])
      assert.deepEqual(res.key, ['a'])
      assert.deepEqual(res.value, null)
    })
  }

  const keys = [
    ['a'],
    ['a', 'b'],
    ['a', 'b', 'c'],
    [1],
    ['a', 1],
    ['a', 1, 'b'],
    [1n],
    ['a', 1n],
    ['a', 1n, 'b'],
    [true],
    ['a', true],
    ['a', true, 'b'],
    [new Uint8Array([1, 2, 3])],
    ['a', new Uint8Array([1, 2, 3])],
    ['a', new Uint8Array([1, 2, 3]), 'b'],
    [1, 1n, true, new Uint8Array([1, 2, 3]), 'a'],
  ]

  for (const key of keys) {
    await dbTest(`set and get ${inspect(key)} key`, async (db) => {
      await db.set(key, 'b')
      const result = await db.get(key)
      assert.deepEqual(result.key, key)
      assert.deepEqual(result.value, 'b')
    })
  }

  const INVALID_KEYS = [
    [null],
    [undefined],
    [],
    [{}],
    [new Date()],
    [new ArrayBuffer(3)],
    [new Uint8Array([1, 2, 3]).buffer],
    [['a', 'b']],
  ]

  for (const key of INVALID_KEYS) {
    await dbTest(`set and get invalid key ${inspect(key)}`, async (db) => {
      await assert.rejects(async () => {
        // @ts-ignore - we are testing invalid keys
        await db.set(key, 'b')
      }, Error)
    })
  }

  await dbTest('compare and mutate', async (db) => {
    await db.set(['t'], '1')

    const currentValue = await db.get(['t'])
    assert(currentValue.versionstamp)
    assert(currentValue.versionstamp > ZERO_VERSIONSTAMP)

    let res = await db
      .atomic()
      .check({ key: ['t'], versionstamp: currentValue.versionstamp })
      .set(currentValue.key, '2')
      .commit()
    assert(res.ok)
    assert(res.versionstamp > currentValue.versionstamp)

    const newValue = await db.get(['t'])
    assert(newValue.versionstamp)
    assert(newValue.versionstamp > currentValue.versionstamp)
    assert.deepEqual(newValue.value, '2')

    res = await db
      .atomic()
      .check({ key: ['t'], versionstamp: currentValue.versionstamp })
      .set(currentValue.key, '3')
      .commit()
    assert(!res.ok)

    const newValue2 = await db.get(['t'])
    assert.deepEqual(newValue2.versionstamp, newValue.versionstamp)
    assert.deepEqual(newValue2.value, '2')
  })

  await dbTest('compare and mutate not exists', async (db) => {
    let res = await db
      .atomic()
      .check({ key: ['t'], versionstamp: null })
      .set(['t'], '1')
      .commit()
    assert(res.ok)
    assert(res.versionstamp > ZERO_VERSIONSTAMP)

    const newValue = await db.get(['t'])
    assert.deepEqual(newValue.versionstamp, res.versionstamp)
    assert.deepEqual(newValue.value, '1')

    res = await db
      .atomic()
      .check({ key: ['t'], versionstamp: null })
      .set(['t'], '2')
      .commit()
    assert(!res.ok)
  })

  await dbTest('atomic mutation helper (sum)', async (db) => {
    await db.set(['t'], new KvU64(42n))
    assert.deepEqual((await db.get(['t'])).value, new KvU64(42n))

    await db.atomic().sum(['t'], new KvU64(1n)).commit()
    assert.deepEqual((await db.get(['t'])).value, new KvU64(43n))
  })

  await dbTest('atomic mutation helper (min)', async (db) => {
    await db.set(['t'], new KvU64(42n))
    assert.deepEqual((await db.get(['t'])).value, new KvU64(42n))

    await db.atomic().min(['t'], new KvU64(1n)).commit()
    assert.deepEqual((await db.get(['t'])).value, new KvU64(1n))

    await db.atomic().min(['t'], new KvU64(2n)).commit()
    assert.deepEqual((await db.get(['t'])).value, new KvU64(1n))
  })

  await dbTest('atomic mutation helper (max)', async (db) => {
    await db.set(['t'], new KvU64(42n))
    assert.deepEqual((await db.get(['t'])).value, new KvU64(42n))

    await db.atomic().max(['t'], new KvU64(41n)).commit()
    assert.deepEqual((await db.get(['t'])).value, new KvU64(42n))

    await db.atomic().max(['t'], new KvU64(43n)).commit()
    assert.deepEqual((await db.get(['t'])).value, new KvU64(43n))
  })

  await dbTest('compare multiple and mutate', async (db) => {
    const setRes1 = await db.set(['t1'], '1')
    const setRes2 = await db.set(['t2'], '2')
    assert(setRes1.ok)
    assert(setRes1.versionstamp > ZERO_VERSIONSTAMP)
    assert(setRes2.ok)
    assert(setRes2.versionstamp > ZERO_VERSIONSTAMP)

    const currentValue1 = await db.get(['t1'])
    assert(currentValue1.versionstamp)
    assert(currentValue1.versionstamp === setRes1.versionstamp)
    const currentValue2 = await db.get(['t2'])
    assert(currentValue2.versionstamp)
    assert(currentValue2.versionstamp === setRes2.versionstamp)

    const res = await db
      .atomic()
      .check({ key: ['t1'], versionstamp: currentValue1.versionstamp })
      .check({ key: ['t2'], versionstamp: currentValue2.versionstamp })
      .set(currentValue1.key, '3')
      .set(currentValue2.key, '4')
      .commit()
    assert(res.ok)
    assert(res.versionstamp > setRes2.versionstamp)

    const newValue1 = await db.get(['t1'])
    assert(newValue1.versionstamp)
    assert(newValue1.versionstamp > setRes1.versionstamp)
    assert.deepEqual(newValue1.value, '3')
    const newValue2 = await db.get(['t2'])
    assert(newValue2.versionstamp)
    assert(newValue2.versionstamp > setRes2.versionstamp)
    assert.deepEqual(newValue2.value, '4')

    // just one of the two checks failed
    const res2 = await db
      .atomic()
      .check({ key: ['t1'], versionstamp: newValue1.versionstamp })
      .check({ key: ['t2'], versionstamp: null })
      .set(newValue1.key, '5')
      .set(newValue2.key, '6')
      .commit()
    assert(!res2.ok)

    const newValue3 = await db.get(['t1'])
    assert.deepEqual(newValue3.versionstamp, res.versionstamp)
    assert.deepEqual(newValue3.value, '3')
    const newValue4 = await db.get(['t2'])
    assert.deepEqual(newValue4.versionstamp, res.versionstamp)
    assert.deepEqual(newValue4.value, '4')
  })

  await dbTest('atomic mutation ordering (set before delete)', async (db) => {
    await db.set(['a'], '1')
    const res = await db.atomic().set(['a'], '2').delete(['a']).commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, null)
  })

  await dbTest('atomic mutation ordering (delete before set)', async (db) => {
    await db.set(['a'], '1')
    const res = await db.atomic().delete(['a']).set(['a'], '2').commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, '2')
  })

  await dbTest('atomic mutation type=set', async (db) => {
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: '1', type: 'set' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, '1')
  })

  await dbTest('atomic mutation type=set overwrite', async (db) => {
    await db.set(['a'], '1')
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: '2', type: 'set' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, '2')
  })

  await dbTest('atomic mutation type=delete', async (db) => {
    await db.set(['a'], '1')
    const res = await db
      .atomic()
      .mutate({ key: ['a'], type: 'delete' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, null)
  })

  await dbTest('atomic mutation type=delete no exists', async (db) => {
    const res = await db
      .atomic()
      .mutate({ key: ['a'], type: 'delete' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, null)
  })

  await dbTest('atomic mutation type=sum', async (db) => {
    await db.set(['a'], new KvU64(10n))
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(1n), type: 'sum' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, new KvU64(11n))
  })

  await dbTest('atomic mutation type=sum no exists', async (db) => {
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(1n), type: 'sum' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert(result.value)
    assert.deepEqual(result.value, new KvU64(1n))
  })

  await dbTest('atomic mutation type=sum wrap around', async (db) => {
    await db.set(['a'], new KvU64(0xffffffffffffffffn))
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(10n), type: 'sum' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, new KvU64(9n))

    const res2 = await db
      .atomic()
      .mutate({
        key: ['a'],
        value: new KvU64(0xffffffffffffffffn),
        type: 'sum',
      })
      .commit()
    assert(res2)
    const result2 = await db.get(['a'])
    assert.deepEqual(result2.value, new KvU64(8n))
  })

  await dbTest('atomic mutation type=sum wrong type in db', async (db) => {
    await db.set(['a'], 1)
    await assert.rejects(
      async () => {
        await db
          .atomic()
          .mutate({ key: ['a'], value: new KvU64(1n), type: 'sum' })
          .commit()
      },
      {
        name: 'TypeError',
        message:
          "Failed to perform 'sum' mutation on a non-U64 value in the database",
      },
    )
  })

  await dbTest(
    'atomic mutation type=sum wrong type in mutation',
    async (db) => {
      await db.set(['a'], new KvU64(1n))
      await assert.rejects(
        async () => {
          await db
            .atomic()
            // @ts-expect-error wrong type is intentional
            .mutate({ key: ['a'], value: 1, type: 'sum' })
            .commit()
        },
        {
          name: 'TypeError',
          message: 'Cannot sum KvU64 with Number',
        },
      )
    },
  )

  await dbTest('atomic mutation type=min', async (db) => {
    await db.set(['a'], new KvU64(10n))
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(5n), type: 'min' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, new KvU64(5n))

    const res2 = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(15n), type: 'min' })
      .commit()
    assert(res2)
    const result2 = await db.get(['a'])
    assert.deepEqual(result2.value, new KvU64(5n))
  })

  await dbTest('atomic mutation type=min no exists', async (db) => {
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(1n), type: 'min' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert(result.value)
    assert.deepEqual(result.value, new KvU64(1n))
  })

  await dbTest('atomic mutation type=min wrong type in db', async (db) => {
    await db.set(['a'], 1)
    await assert.rejects(
      async () => {
        await db
          .atomic()
          .mutate({ key: ['a'], value: new KvU64(1n), type: 'min' })
          .commit()
      },
      {
        name: 'TypeError',
        message:
          "Failed to perform 'min' mutation on a non-U64 value in the database",
      },
    )
  })

  await dbTest(
    'atomic mutation type=min wrong type in mutation',
    async (db) => {
      await db.set(['a'], new KvU64(1n))
      await assert.rejects(
        async () => {
          await db
            .atomic()
            // @ts-expect-error wrong type is intentional
            .mutate({ key: ['a'], value: 1, type: 'min' })
            .commit()
        },
        {
          name: 'TypeError',
          message: "Failed to perform 'min' mutation on a non-U64 operand",
        },
      )
    },
  )

  await dbTest('atomic mutation type=max', async (db) => {
    await db.set(['a'], new KvU64(10n))
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(5n), type: 'max' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert.deepEqual(result.value, new KvU64(10n))

    const res2 = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(15n), type: 'max' })
      .commit()
    assert(res2)
    const result2 = await db.get(['a'])
    assert.deepEqual(result2.value, new KvU64(15n))
  })

  await dbTest('atomic mutation type=max no exists', async (db) => {
    const res = await db
      .atomic()
      .mutate({ key: ['a'], value: new KvU64(1n), type: 'max' })
      .commit()
    assert(res.ok)
    const result = await db.get(['a'])
    assert(result.value)
    assert.deepEqual(result.value, new KvU64(1n))
  })

  await dbTest('atomic mutation type=max wrong type in db', async (db) => {
    await db.set(['a'], 1)
    await assert.rejects(
      async () => {
        await db
          .atomic()
          .mutate({ key: ['a'], value: new KvU64(1n), type: 'max' })
          .commit()
      },
      {
        name: 'TypeError',
        message:
          "Failed to perform 'max' mutation on a non-U64 value in the database",
      },
    )
  })

  await dbTest(
    'atomic mutation type=max wrong type in mutation',
    async (db) => {
      await db.set(['a'], new KvU64(1n))
      await assert.rejects(
        async () => {
          await db
            .atomic()
            // @ts-expect-error wrong type is intentional
            .mutate({ key: ['a'], value: 1, type: 'max' })
            .commit()
        },
        {
          name: 'TypeError',
          message: "Failed to perform 'max' mutation on a non-U64 operand",
        },
      )
    },
  )

  test('KvU64 comparison', () => {
    const a = new KvU64(1n)
    const b = new KvU64(1n)
    assert.deepEqual(a, b)
    assert.throws(() => {
      assert.deepEqual(a, new KvU64(2n))
    }, AssertionError)
  })

  test('KvU64 overflow', () => {
    assert.throws(() => {
      new KvU64(2n ** 64n)
    }, RangeError)
  })

  test('KvU64 underflow', () => {
    assert.throws(() => {
      new KvU64(-1n)
    }, RangeError)
  })

  test('KvU64 unbox', () => {
    const a = new KvU64(1n)
    assert.strictEqual(a.value, 1n)
  })

  test('KvU64 unbox with valueOf', () => {
    const a = new KvU64(1n)
    assert.strictEqual(a.valueOf(), 1n)
  })

  test('KvU64 auto-unbox', () => {
    const a = new KvU64(1n)
    assert.strictEqual((a as unknown as bigint) + 1n, 2n)
  })

  test('KvU64 toString', () => {
    const a = new KvU64(1n)
    assert.strictEqual(a.toString(), '1')
  })

  test('KvU64 inspect', () => {
    const a = new KvU64(1n)
    assert.strictEqual(inspect(a), '[KvU64: 1n]')
  })

  async function setupData(db: Valkeyrie): Promise<string> {
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
    return res.versionstamp
  }

  await dbTest('get many', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await db.getMany([['b', 'a'], ['a'], ['c']])
    assert.deepEqual(entries, [
      { key: ['b', 'a'], value: 100, versionstamp },
      { key: ['a'], value: -1, versionstamp },
      { key: ['c'], value: null, versionstamp: null },
    ])
  })

  await dbTest('list prefix', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(db.list({ prefix: ['a'] }))
    assert.deepEqual(entries, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'e'], value: 4, versionstamp },
    ])
  })

  await dbTest('list prefix empty', async (db) => {
    await setupData(db)
    const entries = await Array.fromAsync(db.list({ prefix: ['c'] }))
    assert.deepEqual(entries.length, 0)

    const entries2 = await Array.fromAsync(db.list({ prefix: ['a', 'f'] }))
    assert.deepEqual(entries2.length, 0)
  })

  await dbTest('list prefix with start', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], start: ['a', 'c'] }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'e'], value: 4, versionstamp },
    ])
  })

  await dbTest('list prefix with start empty', async (db) => {
    await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], start: ['a', 'f'] }),
    )
    assert.deepEqual(entries.length, 0)
  })

  await dbTest('list prefix with start equal to prefix', async (db) => {
    await setupData(db)
    await assert.rejects(
      async () =>
        await Array.fromAsync(db.list({ prefix: ['a'], start: ['a'] })),
      {
        name: 'TypeError',
        message: 'Start key is not in the keyspace defined by prefix',
      },
    )
  })

  await dbTest('list prefix with start out of bounds', async (db) => {
    await setupData(db)
    await assert.rejects(
      async () =>
        await Array.fromAsync(db.list({ prefix: ['b'], start: ['a'] })),
      {
        name: 'TypeError',
        message: 'Start key is not in the keyspace defined by prefix',
      },
    )
  })

  await dbTest('list prefix with end', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], end: ['a', 'c'] }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
    ])
  })

  await dbTest('list prefix with end empty', async (db) => {
    await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], end: ['a', 'a'] }),
    )
    assert.deepEqual(entries.length, 0)
  })

  await dbTest('list prefix with end equal to prefix', async (db) => {
    await setupData(db)
    await assert.rejects(
      async () => await Array.fromAsync(db.list({ prefix: ['a'], end: ['a'] })),
      {
        name: 'TypeError',
        message: 'End key is not in the keyspace defined by prefix',
      },
    )
  })

  await dbTest('list prefix with end out of bounds', async (db) => {
    await setupData(db)
    await assert.rejects(
      async () => await Array.fromAsync(db.list({ prefix: ['a'], end: ['b'] })),
      {
        name: 'TypeError',
        message: 'End key is not in the keyspace defined by prefix',
      },
    )
  })

  await dbTest('list prefix with empty prefix', async (db) => {
    const res = await db.set(['a'], 1)
    const entries = await Array.fromAsync(db.list({ prefix: [] }))
    assert.deepEqual(entries, [
      { key: ['a'], value: 1, versionstamp: res.versionstamp },
    ])
  })

  await dbTest('list prefix reverse', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { reverse: true }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'a'], value: 0, versionstamp },
    ])
  })

  await dbTest('list prefix reverse with start', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], start: ['a', 'c'] }, { reverse: true }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
    ])
  })

  await dbTest('list prefix reverse with start empty', async (db) => {
    await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], start: ['a', 'f'] }, { reverse: true }),
    )
    assert.deepEqual(entries.length, 0)
  })

  await dbTest('list prefix reverse with end', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], end: ['a', 'c'] }, { reverse: true }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'a'], value: 0, versionstamp },
    ])
  })

  await dbTest('list prefix reverse with end empty', async (db) => {
    await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'], end: ['a', 'a'] }, { reverse: true }),
    )
    assert.deepEqual(entries.length, 0)
  })

  await dbTest('list prefix limit', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { limit: 2 }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
    ])
  })

  await dbTest('list prefix limit reverse', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { limit: 2, reverse: true }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
    ])
  })

  await dbTest('list prefix with small batch size', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { batchSize: 2 }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'e'], value: 4, versionstamp },
    ])
  })

  await dbTest('list prefix with small batch size reverse', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { batchSize: 2, reverse: true }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'a'], value: 0, versionstamp },
    ])
  })

  await dbTest('list prefix with small batch size and limit', async (db) => {
    const versionstamp = await setupData(db)
    const entries = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { batchSize: 2, limit: 3 }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
    ])
  })

  await dbTest(
    'list prefix with small batch size and limit reverse',
    async (db) => {
      const versionstamp = await setupData(db)
      const entries = await Array.fromAsync(
        db.list({ prefix: ['a'] }, { batchSize: 2, limit: 3, reverse: true }),
      )
      assert.deepEqual(entries, [
        { key: ['a', 'e'], value: 4, versionstamp },
        { key: ['a', 'd'], value: 3, versionstamp },
        { key: ['a', 'c'], value: 2, versionstamp },
      ])
    },
  )

  await dbTest('list prefix with manual cursor', async (db) => {
    const versionstamp = await setupData(db)
    const iterator = db.list({ prefix: ['a'] }, { limit: 2 })
    const values = await Array.fromAsync(iterator)
    assert.deepEqual(values, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
    ])

    const cursor = iterator.cursor
    assert.ok(cursor) // Cursor should exist and be non-empty

    const iterator2 = db.list({ prefix: ['a'] }, { cursor })
    const values2 = await Array.fromAsync(iterator2)
    assert.deepEqual(values2, [
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'e'], value: 4, versionstamp },
    ])
  })

  await dbTest('list prefix with manual cursor reverse', async (db) => {
    const versionstamp = await setupData(db)

    const iterator = db.list({ prefix: ['a'] }, { limit: 2, reverse: true })
    const values = await Array.fromAsync(iterator)
    assert.deepEqual(values, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
    ])

    const cursor = iterator.cursor
    assert.ok(cursor) // Cursor should exist and be non-empty

    const iterator2 = db.list({ prefix: ['a'] }, { cursor, reverse: true })
    const values2 = await Array.fromAsync(iterator2)
    assert.deepEqual(values2, [
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'a'], value: 0, versionstamp },
    ])
  })

  await dbTest('list prefix with manual cursor - numeric keys', async (db) => {
    // Setup numeric keys
    for (let i = 1; i <= 10; i++) {
      await db.set(['nums', i], i * 10)
    }

    // Test forward pagination
    const iterator = db.list({ prefix: ['nums'] }, { limit: 3 })
    const values = await Array.fromAsync(iterator)
    assert.strictEqual(values.length, 3)
    assert.ok(values[0])
    assert.ok(values[1])
    assert.ok(values[2])
    assert.deepEqual(values[0].key, ['nums', 1])
    assert.deepEqual(values[1].key, ['nums', 2])
    assert.deepEqual(values[2].key, ['nums', 3])

    const cursor = iterator.cursor
    assert.ok(cursor)

    const iterator2 = db.list({ prefix: ['nums'] }, { cursor })
    const values2 = await Array.fromAsync(iterator2)
    assert.strictEqual(values2.length, 7)
    assert.ok(values2[0])
    assert.ok(values2[6])
    assert.deepEqual(values2[0].key, ['nums', 4])
    assert.deepEqual(values2[6].key, ['nums', 10])
  })

  await dbTest(
    'list prefix with manual cursor reverse - numeric keys',
    async (db) => {
      // Setup numeric keys
      for (let i = 1; i <= 10; i++) {
        await db.set(['nums', i], i * 10)
      }

      // Test reverse pagination
      const iterator = db.list(
        { prefix: ['nums'] },
        { limit: 3, reverse: true },
      )
      const values = await Array.fromAsync(iterator)
      assert.strictEqual(values.length, 3)
      assert.ok(values[0])
      assert.ok(values[1])
      assert.ok(values[2])
      assert.deepEqual(values[0].key, ['nums', 10])
      assert.deepEqual(values[1].key, ['nums', 9])
      assert.deepEqual(values[2].key, ['nums', 8])

      const cursor = iterator.cursor
      assert.ok(cursor)

      const iterator2 = db.list({ prefix: ['nums'] }, { cursor, reverse: true })
      const values2 = await Array.fromAsync(iterator2)
      assert.strictEqual(values2.length, 7)
      assert.ok(values2[0])
      assert.ok(values2[6])
      assert.deepEqual(values2[0].key, ['nums', 7])
      assert.deepEqual(values2[6].key, ['nums', 1])
    },
  )

  await dbTest('list prefix with manual cursor - bigint keys', async (db) => {
    // Setup bigint keys
    for (let i = 1n; i <= 10n; i++) {
      await db.set(['bigs', i], Number(i) * 10)
    }

    // Test forward pagination
    const iterator = db.list({ prefix: ['bigs'] }, { limit: 3 })
    const values = await Array.fromAsync(iterator)
    assert.strictEqual(values.length, 3)
    assert.ok(values[0])
    assert.ok(values[2])
    assert.deepEqual(values[0].key, ['bigs', 1n])
    assert.deepEqual(values[2].key, ['bigs', 3n])

    const cursor = iterator.cursor
    assert.ok(cursor)

    const iterator2 = db.list({ prefix: ['bigs'] }, { cursor })
    const values2 = await Array.fromAsync(iterator2)
    assert.strictEqual(values2.length, 7)
    assert.ok(values2[0])
    assert.ok(values2[6])
    assert.deepEqual(values2[0].key, ['bigs', 4n])
    assert.deepEqual(values2[6].key, ['bigs', 10n])
  })

  await dbTest(
    'list prefix with manual cursor reverse - bigint keys',
    async (db) => {
      // Setup bigint keys
      for (let i = 1n; i <= 10n; i++) {
        await db.set(['bigs', i], Number(i) * 10)
      }

      // Test reverse pagination
      const iterator = db.list(
        { prefix: ['bigs'] },
        { limit: 3, reverse: true },
      )
      const values = await Array.fromAsync(iterator)
      assert.strictEqual(values.length, 3)
      assert.ok(values[0])
      assert.ok(values[1])
      assert.ok(values[2])
      assert.deepEqual(values[0].key, ['bigs', 10n])
      assert.deepEqual(values[1].key, ['bigs', 9n])
      assert.deepEqual(values[2].key, ['bigs', 8n])

      const cursor = iterator.cursor
      assert.ok(cursor)

      const iterator2 = db.list({ prefix: ['bigs'] }, { cursor, reverse: true })
      const values2 = await Array.fromAsync(iterator2)
      assert.strictEqual(values2.length, 7)
      assert.ok(values2[0])
      assert.ok(values2[6])
      assert.deepEqual(values2[0].key, ['bigs', 7n])
      assert.deepEqual(values2[6].key, ['bigs', 1n])
    },
  )

  await dbTest('list prefix with manual cursor - boolean keys', async (db) => {
    // Setup boolean keys - cursor stores only the last key part,
    // so we need single-level keys after the prefix
    await db.set(['items', false], 'false value')
    await db.set(['items', true], 'true value')

    // Test forward pagination
    const iterator = db.list({ prefix: ['items'] }, { limit: 1 })
    const values = await Array.fromAsync(iterator)
    assert.strictEqual(values.length, 1)
    assert.ok(values[0])
    assert.deepEqual(values[0].key, ['items', false])

    // Get all remaining items with cursor
    const cursor = iterator.cursor
    if (cursor) {
      const iterator2 = db.list({ prefix: ['items'] }, { cursor })
      const values2 = await Array.fromAsync(iterator2)
      assert.strictEqual(values2.length, 1)
      assert.ok(values2[0])
      assert.deepEqual(values2[0].key, ['items', true])
    }
  })

  await dbTest(
    'list prefix with manual cursor reverse - boolean keys',
    async (db) => {
      // Setup boolean keys - cursor stores only the last key part,
      // so we need single-level keys after the prefix
      await db.set(['items', false], 'false value')
      await db.set(['items', true], 'true value')

      // Test reverse pagination
      const iterator = db.list(
        { prefix: ['items'] },
        { limit: 1, reverse: true },
      )
      const values = await Array.fromAsync(iterator)
      assert.strictEqual(values.length, 1)
      assert.ok(values[0])
      assert.deepEqual(values[0].key, ['items', true])

      // Get all remaining items with cursor
      const cursor = iterator.cursor
      if (cursor) {
        const iterator2 = db.list(
          { prefix: ['items'] },
          { cursor, reverse: true },
        )
        const values2 = await Array.fromAsync(iterator2)
        assert.strictEqual(values2.length, 1)
        assert.ok(values2[0])
        assert.deepEqual(values2[0].key, ['items', false])
      }
    },
  )

  await dbTest(
    'list prefix with manual cursor - Uint8Array keys',
    async (db) => {
      // Setup Uint8Array keys
      const key1 = new Uint8Array([1, 2, 3])
      const key2 = new Uint8Array([4, 5, 6])
      const key3 = new Uint8Array([7, 8, 9])

      await db.set(['bytes', key1], 'value 1')
      await db.set(['bytes', key2], 'value 2')
      await db.set(['bytes', key3], 'value 3')

      // Test forward pagination
      const iterator = db.list({ prefix: ['bytes'] }, { limit: 1 })
      const values = await Array.fromAsync(iterator)
      assert.strictEqual(values.length, 1)
      assert.ok(values[0])
      assert.deepEqual(values[0].key, ['bytes', key1])

      const cursor = iterator.cursor
      assert.ok(cursor)

      const iterator2 = db.list({ prefix: ['bytes'] }, { cursor })
      const values2 = await Array.fromAsync(iterator2)
      assert.strictEqual(values2.length, 2)
      assert.ok(values2[0])
      assert.ok(values2[1])
      assert.deepEqual(values2[0].key, ['bytes', key2])
      assert.deepEqual(values2[1].key, ['bytes', key3])
    },
  )

  await dbTest(
    'list prefix with manual cursor reverse - Uint8Array keys',
    async (db) => {
      // Setup Uint8Array keys
      const key1 = new Uint8Array([1, 2, 3])
      const key2 = new Uint8Array([4, 5, 6])
      const key3 = new Uint8Array([7, 8, 9])

      await db.set(['bytes', key1], 'value 1')
      await db.set(['bytes', key2], 'value 2')
      await db.set(['bytes', key3], 'value 3')

      // Test reverse pagination
      const iterator = db.list(
        { prefix: ['bytes'] },
        { limit: 2, reverse: true },
      )
      const values = await Array.fromAsync(iterator)
      assert.strictEqual(values.length, 2)
      assert.ok(values[0])
      assert.ok(values[1])
      assert.deepEqual(values[0].key, ['bytes', key3])
      assert.deepEqual(values[1].key, ['bytes', key2])

      const cursor = iterator.cursor
      assert.ok(cursor)

      const iterator2 = db.list(
        { prefix: ['bytes'] },
        { cursor, reverse: true },
      )
      const values2 = await Array.fromAsync(iterator2)
      assert.strictEqual(values2.length, 1)
      assert.ok(values2[0])
      assert.deepEqual(values2[0].key, ['bytes', key1])
    },
  )

  await dbTest(
    'list prefix with manual cursor - mixed type keys',
    async (db) => {
      // Setup keys where second part has different types across entries
      // [string, number], [string, string], [string, bigint], [string, boolean]
      await db.set(['data', 1], { type: 'number' })
      await db.set(['data', 2], { type: 'number' })
      await db.set(['data', 'alice'], { type: 'string' })
      await db.set(['data', 'bob'], { type: 'string' })
      await db.set(['data', 100n], { type: 'bigint' })
      await db.set(['data', 200n], { type: 'bigint' })
      await db.set(['data', false], { type: 'boolean' })
      await db.set(['data', true], { type: 'boolean' })

      // Keys are ordered by type marker, then value within type:
      // Uint8Array(0x01) < String(0x02) < BigInt(0x03) < Number(0x04) < Boolean(0x05)
      // So order is: "alice", "bob", 100n, 200n, 1, 2, false, true

      // Test forward pagination
      const iterator = db.list({ prefix: ['data'] }, { limit: 3 })
      const values = await Array.fromAsync(iterator)
      assert.strictEqual(values.length, 3)
      assert.ok(values[0])
      assert.ok(values[1])
      assert.ok(values[2])
      // First 3: strings then first bigint
      assert.deepEqual(values[0].key, ['data', 'alice'])
      assert.deepEqual(values[1].key, ['data', 'bob'])
      assert.deepEqual(values[2].key, ['data', 100n])

      const cursor = iterator.cursor
      assert.ok(cursor)

      const iterator2 = db.list({ prefix: ['data'] }, { cursor })
      const values2 = await Array.fromAsync(iterator2)
      assert.strictEqual(values2.length, 5)
      assert.ok(values2[0])
      assert.ok(values2[1])
      assert.ok(values2[4])
      // Remaining: 200n, 1, 2, false, true
      assert.deepEqual(values2[0].key, ['data', 200n])
      assert.deepEqual(values2[1].key, ['data', 1])
      assert.deepEqual(values2[4].key, ['data', true])
    },
  )

  await dbTest(
    'list prefix with manual cursor reverse - mixed type keys',
    async (db) => {
      // Setup keys with mixed types: [string, bigint, Uint8Array]
      // Cursor works with prefix + single key part
      const tag1 = new Uint8Array([1, 0, 0])
      const tag2 = new Uint8Array([2, 0, 0])
      const tag3 = new Uint8Array([3, 0, 0])
      const tag4 = new Uint8Array([4, 0, 0])

      await db.set(['events', 1n, tag1], { event: 'login' })
      await db.set(['events', 1n, tag2], { event: 'click' })
      await db.set(['events', 1n, tag3], { event: 'view' })
      await db.set(['events', 1n, tag4], { event: 'logout' })

      // Test reverse pagination with prefix ['events', 1n]
      // Keys after prefix are Uint8Arrays: tag1, tag2, tag3, tag4
      const iterator = db.list(
        { prefix: ['events', 1n] },
        { limit: 2, reverse: true },
      )
      const values = await Array.fromAsync(iterator)
      assert.strictEqual(values.length, 2)
      assert.ok(values[0])
      assert.ok(values[1])
      assert.deepEqual(values[0].key, ['events', 1n, tag4])
      assert.deepEqual(values[1].key, ['events', 1n, tag3])

      const cursor = iterator.cursor
      assert.ok(cursor)

      const iterator2 = db.list(
        { prefix: ['events', 1n] },
        { cursor, reverse: true },
      )
      const values2 = await Array.fromAsync(iterator2)
      assert.strictEqual(values2.length, 2)
      assert.ok(values2[0])
      assert.ok(values2[1])
      assert.deepEqual(values2[0].key, ['events', 1n, tag2])
      assert.deepEqual(values2[1].key, ['events', 1n, tag1])
    },
  )

  await dbTest('list range', async (db) => {
    const versionstamp = await setupData(db)

    const entries = await Array.fromAsync(
      db.list({ start: ['a', 'a'], end: ['a', 'z'] }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'e'], value: 4, versionstamp },
    ])
  })

  await dbTest('list range reverse', async (db) => {
    const versionstamp = await setupData(db)

    const entries = await Array.fromAsync(
      db.list({ start: ['a', 'a'], end: ['a', 'z'] }, { reverse: true }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'a'], value: 0, versionstamp },
    ])
  })

  await dbTest('list range with limit', async (db) => {
    const versionstamp = await setupData(db)

    const entries = await Array.fromAsync(
      db.list({ start: ['a', 'a'], end: ['a', 'z'] }, { limit: 3 }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
    ])
  })

  await dbTest('list range with limit reverse', async (db) => {
    const versionstamp = await setupData(db)

    const entries = await Array.fromAsync(
      db.list(
        { start: ['a', 'a'], end: ['a', 'z'] },
        {
          limit: 3,
          reverse: true,
        },
      ),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
    ])
  })

  await dbTest('list range nesting', async (db) => {
    const versionstamp = await setupData(db)

    const entries = await Array.fromAsync(
      db.list({ start: ['a'], end: ['a', 'd'] }),
    )
    assert.deepEqual(entries, [
      { key: ['a'], value: -1, versionstamp },
      { key: ['a', 'a'], value: 0, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
    ])
  })

  await dbTest('list range short', async (db) => {
    const versionstamp = await setupData(db)

    const entries = await Array.fromAsync(
      db.list({ start: ['a', 'b'], end: ['a', 'd'] }),
    )
    assert.deepEqual(entries, [
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
    ])
  })

  await dbTest('list range with manual cursor', async (db) => {
    const versionstamp = await setupData(db)

    const iterator = db.list(
      { start: ['a', 'b'], end: ['a', 'z'] },
      {
        limit: 2,
      },
    )
    const entries = await Array.fromAsync(iterator)
    assert.deepEqual(entries, [
      { key: ['a', 'b'], value: 1, versionstamp },
      { key: ['a', 'c'], value: 2, versionstamp },
    ])

    const cursor = iterator.cursor
    const iterator2 = db.list(
      { start: ['a', 'b'], end: ['a', 'z'] },
      {
        cursor,
      },
    )
    const entries2 = await Array.fromAsync(iterator2)
    assert.deepEqual(entries2, [
      { key: ['a', 'd'], value: 3, versionstamp },
      { key: ['a', 'e'], value: 4, versionstamp },
    ])
  })

  await dbTest('list range with manual cursor reverse', async (db) => {
    const versionstamp = await setupData(db)

    const iterator = db.list(
      { start: ['a', 'b'], end: ['a', 'z'] },
      {
        limit: 2,
        reverse: true,
      },
    )
    const entries = await Array.fromAsync(iterator)
    assert.deepEqual(entries, [
      { key: ['a', 'e'], value: 4, versionstamp },
      { key: ['a', 'd'], value: 3, versionstamp },
    ])

    const cursor = iterator.cursor
    const iterator2 = db.list(
      { start: ['a', 'b'], end: ['a', 'z'] },
      {
        cursor,
        reverse: true,
      },
    )
    const entries2 = await Array.fromAsync(iterator2)
    assert.deepEqual(entries2, [
      { key: ['a', 'c'], value: 2, versionstamp },
      { key: ['a', 'b'], value: 1, versionstamp },
    ])
  })

  await dbTest('list range with start greater than end', async (db) => {
    await setupData(db)
    await assert.rejects(
      async () => await Array.fromAsync(db.list({ start: ['b'], end: ['a'] })),
      {
        name: 'TypeError',
        message: 'Start key is greater than end key',
      },
    )
  })

  await dbTest('list range with start equal to end', async (db) => {
    await setupData(db)
    const entries = await Array.fromAsync(db.list({ start: ['a'], end: ['a'] }))
    assert.deepEqual(entries.length, 0)
  })

  await dbTest('list invalid selector', async (db) => {
    await setupData(db)

    await assert.rejects(
      async () =>
        await Array.fromAsync(
          db.list({ prefix: ['a'], start: ['a', 'b'], end: ['a', 'c'] }),
        ),
      TypeError,
    )

    await assert.rejects(
      // @ts-expect-error invalid type
      async () => await Array.fromAsync(db.list({ start: ['a', 'b'] })),
      TypeError,
    )

    await assert.rejects(
      // @ts-expect-error invalid type
      async () => await Array.fromAsync(db.list({ end: ['a', 'b'] })),
      TypeError,
    )
  })

  await dbTest('invalid versionstamp in atomic check rejects', async (db) => {
    await assert.rejects(
      async () =>
        await db
          .atomic()
          .check({ key: ['a'], versionstamp: '' })
          .commit(),
      TypeError,
    )

    await assert.rejects(
      async () =>
        await db
          .atomic()
          .check({ key: ['a'], versionstamp: 'xx'.repeat(10) })
          .commit(),
      TypeError,
    )

    await assert.rejects(
      async () =>
        await db
          .atomic()
          .check({ key: ['a'], versionstamp: 'aa'.repeat(11) })
          .commit(),
      TypeError,
    )
  })

  await dbTest('invalid mutation type rejects', async (db) => {
    await assert.rejects(async () => {
      await db
        .atomic()
        // @ts-expect-error invalid type + value combo
        .mutate({ key: ['a'], type: 'set' })
        .commit()
    }, TypeError)

    await assert.rejects(async () => {
      await db
        .atomic()
        // @ts-expect-error invalid type + value combo
        .mutate({ key: ['a'], type: 'delete', value: '123' })
        .commit()
    }, TypeError)

    await assert.rejects(async () => {
      await db
        .atomic()
        // @ts-expect-error invalid type
        .mutate({ key: ['a'], type: 'foobar' })
        .commit()
    }, TypeError)

    await assert.rejects(async () => {
      await db
        .atomic()
        // @ts-expect-error invalid type
        .mutate({ key: ['a'], type: 'foobar', value: '123' })
        .commit()
    }, TypeError)
  })

  await dbTest('key ordering', async (db) => {
    await db
      .atomic()
      .set([new Uint8Array(0x1)], 0)
      .set(['a'], 0)
      .set([1n], 0)
      .set([3.14], 0)
      .set([false], 0)
      .set([true], 0)
      .commit()

    assert.deepEqual(
      (await Array.fromAsync(db.list({ prefix: [] }))).map((x) => x.key),
      [[new Uint8Array(0x1)], ['a'], [1n], [3.14], [false], [true]],
    )
  })

  await dbTest('key size limit', async (db) => {
    // 1 byte prefix + 1 byte suffix + 2045 bytes key
    const lastValidKey = new Uint8Array(2046).fill(1)
    const firstInvalidKey = new Uint8Array(2047).fill(1)

    const res = await db.set([lastValidKey], 1)

    assert.deepEqual(await db.get([lastValidKey]), {
      key: [lastValidKey],
      value: 1,
      versionstamp: res.versionstamp,
    })

    await assert.rejects(async () => await db.set([firstInvalidKey], 1), {
      name: 'TypeError',
      message: 'Key too large for write (max 2048 bytes)',
    })

    await assert.rejects(async () => await db.get([firstInvalidKey]), {
      name: 'TypeError',
      message: 'Key too large for read (max 2049 bytes)',
    })
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

    await assert.rejects(async () => await db.getMany(firstInvalidKeys), {
      name: 'TypeError',
      message: 'Too many ranges (max 10)',
    })

    const res2 = await Array.fromAsync(
      db.list({ prefix: ['a'] }, { batchSize: 1000 }),
    )
    assert.deepEqual(res2.length, 0)

    await assert.rejects(
      async () =>
        await Array.fromAsync(db.list({ prefix: ['a'] }, { batchSize: 1001 })),
      {
        name: 'TypeError',
        message: 'Too many entries (max 1000)',
      },
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
      {
        name: 'TypeError',
        message: 'Too many checks (max 100)',
      },
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
      {
        name: 'TypeError',
        message: 'Too many mutations (max 1000)',
      },
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
      {
        name: 'TypeError',
        message: 'Total mutation size too large (max 819200 bytes)',
      },
    )
  })

  await dbTest('total key size limit', async (db) => {
    const longString = new Array(1100).fill('a').join('')
    const keys: Key[] = new Array(80).fill(0).map(() => [longString])

    const atomic = db.atomic()
    for (const key of keys) {
      atomic.set(key, 'foo')
    }
    await assert.rejects(() => atomic.commit(), {
      name: 'TypeError',
      message: 'Total key size too large (max 81920 bytes)',
    })
  })

  await dbTest('keys must be arrays', async (db) => {
    await assert.rejects(
      // @ts-expect-error invalid type
      async () => await db.get('a'),
      TypeError,
    )

    await assert.rejects(
      // @ts-expect-error invalid type
      async () => await db.getMany(['a']),
      TypeError,
    )

    await assert.rejects(
      // @ts-expect-error invalid type
      async () => await db.set('a', 1),
      TypeError,
    )

    await assert.rejects(
      // @ts-expect-error invalid type
      async () => await db.delete('a'),
      TypeError,
    )

    await assert.rejects(
      async () =>
        await db
          .atomic()
          // @ts-expect-error invalid type
          .mutate({ key: 'a', type: 'set', value: 1 } satisfies Mutation)
          .commit(),
      TypeError,
    )

    await assert.rejects(
      async () =>
        await db
          .atomic()
          // @ts-expect-error invalid type
          .check({ key: 'a', versionstamp: null })
          .set(['a'], 1)
          .commit(),
      TypeError,
    )
  })

  await test('Valkeyrie constructor throws', async () => {
    assert.throws(
      () => {
        // @ts-expect-error invalid type
        new Valkeyrie()
      },
      TypeError,
      'Valkeyrie constructor throws',
    )
  })

  await dbTest('atomic operation is exposed', (db) => {
    assert(AtomicOperation)
    const ao = db.atomic()
    assert(ao instanceof AtomicOperation)
  })

  await test('racy open', async () => {
    for (let i = 0; i < 100; i++) {
      const filename = join(tmpdir(), randomUUID())

      try {
        const [db1, db2, db3] = await Promise.all([
          Valkeyrie.open(filename),
          Valkeyrie.open(filename),
          Valkeyrie.open(filename),
        ])
        await Promise.all([db1.close(), db2.close(), db3.close()])
      } finally {
        await unlink(filename)
      }
    }
  })

  await test('racy write', async () => {
    const filename = join(tmpdir(), randomUUID())
    const concurrency = 20
    const iterations = 5

    try {
      const dbs = await Promise.all(
        Array(concurrency)
          .fill(0)
          .map(() => Valkeyrie.open(filename)),
      )

      try {
        for (let i = 0; i < iterations; i++) {
          await Promise.all(
            dbs.map((db) => db.atomic().sum(['counter'], 1n).commit()),
          )
        }

        assert.deepEqual(
          // biome-ignore lint/style/noNonNullAssertion: testing
          ((await dbs[0]!.get(['counter'])).value as KvU64).value,
          concurrency * iterations,
        )
      } finally {
        if (dbs) {
          for (const db of dbs) {
            await db.close()
          }
        }
      }
    } finally {
      await unlink(filename)
    }
  })

  await test('kv expiration', async () => {
    const filename = join(tmpdir(), randomUUID())
    let db: Valkeyrie | null = null

    try {
      db = await Valkeyrie.open(filename)

      await db.set(['a'], 1, { expireIn: 1000 })
      await db.set(['b'], 2, { expireIn: 1000 })
      assert.deepEqual((await db.get(['a'])).value, 1)
      assert.deepEqual((await db.get(['b'])).value, 2)

      // Value overwrite should also reset expiration
      await db.set(['b'], 2, { expireIn: 3600 * 1000 })

      // Wait for expiration
      await setTimeout(1000)

      // Re-open to trigger immediate cleanup
      db.close()
      db = null
      db = await Valkeyrie.open(filename)

      let ok = false
      for (let i = 0; i < 50; i++) {
        await setTimeout(100)
        if (
          JSON.stringify(
            (await db.getMany([['a'], ['b']])).map((x) => x.value),
          ) === '[null,2]'
        ) {
          ok = true
          break
        }
      }

      if (!ok) {
        throw new Error('Values did not expire')
      }
    } finally {
      if (db) {
        try {
          db.close()
        } catch {
          // pass
        }
      }
      try {
        await unlink(filename)
      } catch {
        // pass
      }
    }
  })

  await test('kv expiration with atomic', async () => {
    const filename = join(tmpdir(), randomUUID())
    let db: Valkeyrie | null = null

    try {
      db = await Valkeyrie.open(filename)

      await db
        .atomic()
        .set(['a'], 1, { expireIn: 1000 })
        .set(['b'], 2, {
          expireIn: 1000,
        })
        .commit()
      assert.deepEqual(
        (await db.getMany([['a'], ['b']])).map((x) => x.value),
        [1, 2],
      )
      // Wait for expiration
      await setTimeout(1000)

      // Re-open to trigger immediate cleanup
      db.close()
      db = null
      db = await Valkeyrie.open(filename)

      let ok = false
      for (let i = 0; i < 50; i++) {
        await setTimeout(100)
        if (
          JSON.stringify(
            (await db.getMany([['a'], ['b']])).map((x) => x.value),
          ) === '[null,null]'
        ) {
          ok = true
          break
        }
      }

      if (!ok) {
        throw new Error('Values did not expire')
      }
    } finally {
      if (db) {
        try {
          db.close()
        } catch {
          // pass
        }
      }
      try {
        await unlink(filename)
      } catch {
        // pass
      }
    }
  })

  dbTest('key watch', async (db) => {
    const changeHistory: EntryMaybe<unknown>[] = []
    const watcher = db.watch([['key']])

    const reader = watcher.getReader()
    const expectedChanges = 2

    const work = (async () => {
      for (let i = 0; i < expectedChanges; i++) {
        const message = await reader.read()
        if (message.done) {
          throw new Error('Unexpected end of stream')
        }
        // biome-ignore lint/style/noNonNullAssertion: <explanation>
        changeHistory.push(message.value[0]!)
      }

      await reader.cancel()
    })()

    while (changeHistory.length !== 1) {
      await setTimeout(100)
    }
    assert.deepStrictEqual(changeHistory[0], {
      key: ['key'],
      value: null,
      versionstamp: null,
    })

    const { versionstamp } = await db.set(['key'], 1)
    while ((changeHistory.length as number) !== 2) {
      await setTimeout(100)
    }
    assert.deepStrictEqual(changeHistory[1], {
      key: ['key'],
      value: 1,
      versionstamp,
    })

    await work
    await reader.cancel()
  })

  await test('watch should stop when db closed', async () => {
    const db = await Valkeyrie.open(':memory:')

    const watch = db.watch([['a']])
    const completion = (async () => {
      for await (const _item of watch) {
        // pass
      }
    })()

    await setTimeout(100)
    await db.close()

    await completion
  })

  await dbTest('list with more than 1000 elements', async (db) => {
    // Create 1200 elements with prefix ['large'] in smaller batches
    // First batch of 600
    let atomic = db.atomic()
    for (let i = 0; i < 600; i++) {
      atomic.set(['large', i.toString().padStart(4, '0')], i)
    }
    let res = await atomic.commit()
    assert(res.ok)

    // Second batch of 600
    atomic = db.atomic()
    for (let i = 600; i < 1200; i++) {
      atomic.set(['large', i.toString().padStart(4, '0')], i)
    }
    res = await atomic.commit()
    assert(res.ok)

    // List all elements without a limit (should return all 1200)
    const allEntries = await Array.fromAsync(db.list({ prefix: ['large'] }))
    assert.strictEqual(
      allEntries.length,
      1200,
      'Should return all 1200 elements',
    )

    // Verify the first and last elements
    assert.strictEqual(allEntries[0]?.value, 0)
    assert.strictEqual(allEntries[1199]?.value, 1199)

    // Test with a specific limit
    const limitedEntries = await Array.fromAsync(
      db.list({ prefix: ['large'] }, { limit: 500 }),
    )
    assert.strictEqual(
      limitedEntries.length,
      500,
      'Should respect the specified limit',
    )
  })

  await test('destroy clears the db for in-memory db', async () => {
    const db = await Valkeyrie.open(':memory:')
    await db.destroy()
    assert.strictEqual((await db.get(['a'])).value, null)
  })

  await test('destroy deletes the file for persistent db', async () => {
    const filename = join(tmpdir(), randomUUID())
    const db = await Valkeyrie.open(filename)
    assert.strictEqual(await access(filename), undefined)
    await db.destroy()
    await assert.rejects(() => access(filename), {
      name: 'Error',
      message: `ENOENT: no such file or directory, access '${filename}'`,
    })
  })

  await test('clear removes all data', async () => {
    const db = await Valkeyrie.open(':memory:')
    await db.set(['a'], 1)
    assert.strictEqual((await db.get(['a'])).value, 1)
    await db.clear()
    assert.strictEqual((await db.get(['a'])).value, null)
  })

  await test('clear removes all data and keeps the file for persistent db', async () => {
    const filename = join(tmpdir(), randomUUID())
    const db = await Valkeyrie.open(filename)
    await db.set(['a'], 1)
    assert.strictEqual((await db.get(['a'])).value, 1)
    await db.clear()
    assert.strictEqual((await db.get(['a'])).value, null)
    assert.strictEqual(await access(filename), undefined)
  })

  await dbTest('list with invalid selector combinations', async (db) => {
    await assert.rejects(
      async () => {
        await Array.fromAsync(
          db.list({ prefix: ['a'], start: ['a', 'b'], end: ['a', 'c'] }),
        )
      },
      {
        name: 'TypeError',
        message: 'Cannot specify prefix with both start and end keys',
      },
    )
  })

  await dbTest('atomic operation with empty key', async (db) => {
    await assert.rejects(async () => db.atomic().set([], 'value').commit(), {
      name: 'Error',
      message: 'Key cannot be empty',
    })
  })

  await dbTest('atomic operation with key size exceeding limit', async (db) => {
    const longString = 'a'.repeat(82000) // exceeds 81920 byte limit

    await assert.rejects(
      async () => db.atomic().set([longString], 'value').commit(),
      {
        name: 'TypeError',
        message: 'Total key size too large (max 81920 bytes)',
      },
    )
  })

  await dbTest('commitVersionstamp returns expected symbol', async (db) => {
    const symbol = db.commitVersionstamp()
    assert.strictEqual(typeof symbol, 'symbol', 'Should return a symbol')

    const symbol2 = db.commitVersionstamp()
    assert.strictEqual(
      symbol,
      symbol2,
      'Should return the same symbol on multiple calls',
    )
  })

  await dbTest('decodeKeyHash throws on invalid type marker', async (db) => {
    // Create an invalid key hash with an unknown type marker (0x06)
    const invalidKeyHash = Buffer.from([0x06, 0x01, 0x02, 0x00]).toString('hex')

    assert.throws(
      () =>
        // @ts-expect-error Accessing private method for testing
        db.decodeKeyHash(invalidKeyHash),
      {
        name: 'Error',
        message: /Invalid key hash: unknown type marker 0x6/,
      },
    )
  })

  await dbTest('get method with empty key', async (db) => {
    await assert.rejects(async () => db.get([]), {
      name: 'Error',
      message: 'Key cannot be empty',
    })
  })

  await dbTest('list prefix with mismatched start key', async (db) => {
    await assert.rejects(
      async () => {
        // Using list with prefix and start key that doesn't share the prefix
        // This should trigger validatePrefixKey's check at lines 444-450
        const iterator = db.list({
          prefix: ['users'],
          start: ['products', 1], // Different prefix
        })

        // Force execution by iterating
        for await (const _ of iterator) {
          // This should not execute
        }
      },
      {
        name: 'TypeError',
        message: 'Start key is not in the keyspace defined by prefix',
      },
    )
  })

  await dbTest('list with empty prefix and cursor', async (db) => {
    // Set up test data with different top-level keys
    await db.set(['a'], 'value-a')
    await db.set(['b'], 'value-b')
    await db.set(['c'], 'value-c')
    await db.set(['d'], 'value-d')
    await db.set(['e'], 'value-e')

    // First, make sure we can retrieve all entries without a cursor
    const allEntries = await Array.fromAsync(db.list({ prefix: [] }))
    assert.strictEqual(
      allEntries.length,
      5,
      'Should retrieve all 5 entries with no cursor',
    )

    // Test forward direction with limit to get a valid cursor
    const firstBatchIterator = db.list({ prefix: [] }, { limit: 2 })
    const firstBatch = await Array.fromAsync(firstBatchIterator)
    assert.strictEqual(firstBatch.length, 2)
    const [first, second] = firstBatch
    assert.deepEqual(first?.key, ['a'])
    assert.deepEqual(second?.key, ['b'])

    // Get the cursor from the first iterator and log it
    const cursor = firstBatchIterator.cursor
    assert.ok(cursor, 'Should have a valid cursor')

    // Use the cursor to get the remaining entries
    const secondBatchIterator = db.list({ prefix: [] }, { cursor })
    const secondBatch = await Array.fromAsync(secondBatchIterator)

    assert.strictEqual(
      secondBatch.length,
      3,
      'Should retrieve 3 remaining entries',
    )
  })

  await dbTest('list reverse with empty prefix and cursor', async (db) => {
    // Set up test data with different top-level keys
    await db.set(['a'], 'value-a')
    await db.set(['b'], 'value-b')
    await db.set(['c'], 'value-c')
    await db.set(['d'], 'value-d')
    await db.set(['e'], 'value-e')

    // First, make sure we can retrieve all entries without a cursor
    const allEntries = await Array.fromAsync(
      db.list({ prefix: [] }, { reverse: true }),
    )
    assert.strictEqual(
      allEntries.length,
      5,
      'Should retrieve all 5 entries with no cursor',
    )

    // Test forward direction with limit to get a valid cursor
    const firstBatchIterator = db.list(
      { prefix: [] },
      { limit: 2, reverse: true },
    )
    const firstBatch = await Array.fromAsync(firstBatchIterator)
    assert.strictEqual(firstBatch.length, 2)
    const [first, second] = firstBatch
    assert.deepEqual(first?.key, ['e'])
    assert.deepEqual(second?.key, ['d'])

    // Get the cursor from the first iterator and log it
    const cursor = firstBatchIterator.cursor
    assert.ok(cursor, 'Should have a valid cursor')

    // Use the cursor to get the remaining entries
    const secondBatchIterator = db.list(
      { prefix: [] },
      { cursor, reverse: true },
    )
    const secondBatch = await Array.fromAsync(secondBatchIterator)

    const [third, fourth, fifth] = secondBatch
    assert.deepEqual(third?.key, ['c'])
    assert.deepEqual(fourth?.key, ['b'])
    assert.deepEqual(fifth?.key, ['a'])

    assert.strictEqual(
      secondBatch.length,
      3,
      'Should retrieve 3 remaining entries',
    )
  })

  await dbTest('list with invalid selector', async (db) => {
    const invalidSelector = {} as Record<string, never>

    await assert.rejects(
      async () => {
        // @ts-expect-error - Intentionally passing an invalid selector for testing
        for await (const _ of db.list(invalidSelector)) {
        }
      },
      {
        name: 'TypeError',
        message:
          'Invalid selector: must specify either prefix or start/end range',
      },
    )
  })

  await dbTest(
    'watch method with empty keys array throws error',
    async (db) => {
      assert.throws(() => db.watch([]), {
        name: 'Error',
        message: 'Keys cannot be empty',
      })
    },
  )

  await dbTest('validateVersionstamp rejects non-string values', async (db) => {
    await assert.rejects(
      async () =>
        db
          .atomic()
          // @ts-expect-error - Intentionally passing an invalid versionstamp type for testing
          .check({ key: ['a'], versionstamp: 123 })
          .commit(),
      {
        name: 'TypeError',
        message: 'Versionstamp must be a string or null',
      },
    )

    await assert.rejects(
      async () =>
        db
          .atomic()
          // @ts-expect-error - Intentionally passing an invalid versionstamp type for testing
          .check({ key: ['a'], versionstamp: true })
          .commit(),
      {
        name: 'TypeError',
        message: 'Versionstamp must be a string or null',
      },
    )

    await assert.rejects(
      async () =>
        db
          .atomic()
          // @ts-expect-error - Intentionally passing an invalid versionstamp type for testing
          .check({ key: ['a'], versionstamp: {} })
          .commit(),
      {
        name: 'TypeError',
        message: 'Versionstamp must be a string or null',
      },
    )
  })

  // Tests for from() and fromAsync() factory methods
  await test('from() creates and populates database with array', async () => {
    const users = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
    ]

    const db = await Valkeyrie.from(users, {
      prefix: ['users'],
      keyProperty: 'id',
    })

    try {
      // Verify all items were inserted
      const alice = await db.get(['users', 1])
      assert.deepEqual(alice.value, { id: 1, name: 'Alice' })

      const bob = await db.get(['users', 2])
      assert.deepEqual(bob.value, { id: 2, name: 'Bob' })

      const charlie = await db.get(['users', 3])
      assert.deepEqual(charlie.value, { id: 3, name: 'Charlie' })

      // Verify list works
      const allUsers = []
      for await (const entry of db.list({ prefix: ['users'] })) {
        allUsers.push(entry.value)
      }
      assert.equal(allUsers.length, 3)
    } finally {
      await db.close()
    }
  })

  await test('from() with function keyProperty', async () => {
    const items = [
      { email: 'alice@example.com', data: 'A' },
      { email: 'bob@example.com', data: 'B' },
    ]

    const db = await Valkeyrie.from(items, {
      prefix: ['emails'],
      keyProperty: (item) => item.email,
    })

    try {
      const alice = await db.get(['emails', 'alice@example.com'])
      assert.deepEqual(alice.value, { email: 'alice@example.com', data: 'A' })
    } finally {
      await db.close()
    }
  })

  await test('from() with file path', async () => {
    const testPath = join(tmpdir(), `test-from-${randomUUID()}.db`)

    const items = [{ id: 1, value: 'test' }]
    const db = await Valkeyrie.from(items, {
      prefix: ['items'],
      keyProperty: 'id',
      path: testPath,
    })

    try {
      // Verify file exists
      await access(testPath)

      // Verify data
      const item = await db.get(['items', 1])
      assert.deepEqual(item.value, { id: 1, value: 'test' })
    } finally {
      await db.close()
      await unlink(testPath)
    }
  })

  await test('from() with expireIn option', async () => {
    const items = [{ id: 1, value: 'expires' }]

    const db = await Valkeyrie.from(items, {
      prefix: ['temp'],
      keyProperty: 'id',
      expireIn: 100, // 100ms
    })

    try {
      // Should exist immediately
      const result1 = await db.get(['temp', 1])
      assert.deepEqual(result1.value, { id: 1, value: 'expires' })

      // Wait for expiration and cleanup
      await setTimeout(150)
      await db.cleanup()

      // Should be gone
      const result2 = await db.get(['temp', 1])
      assert.equal(result2.value, null)
    } finally {
      await db.close()
    }
  })

  await test('from() with progress callback', async () => {
    const items = Array.from({ length: 50 }, (_, i) => ({ id: i, value: i }))
    const progressUpdates: number[] = []

    const db = await Valkeyrie.from(items, {
      prefix: ['progress'],
      keyProperty: 'id',
      onProgress: (processed, total) => {
        progressUpdates.push(processed)
        if (total !== undefined) {
          assert.equal(total, 50)
        }
      },
    })

    try {
      // Should have received progress updates
      assert.ok(progressUpdates.length > 0)
      assert.equal(progressUpdates[progressUpdates.length - 1], 50)

      // Verify data
      const item = await db.get(['progress', 25])
      assert.deepEqual(item.value, { id: 25, value: 25 })
    } finally {
      await db.close()
    }
  })

  await test('from() with large dataset (chunking)', async () => {
    // Create 2500 items to test chunking (should use 3 atomic operations)
    const items = Array.from({ length: 2500 }, (_, i) => ({
      id: i,
      value: `item-${i}`,
    }))

    const db = await Valkeyrie.from(items, {
      prefix: ['large'],
      keyProperty: 'id',
    })

    try {
      // Verify first item
      const first = await db.get(['large', 0])
      assert.deepEqual(first.value, { id: 0, value: 'item-0' })

      // Verify middle item
      const middle = await db.get(['large', 1250])
      assert.deepEqual(middle.value, { id: 1250, value: 'item-1250' })

      // Verify last item
      const last = await db.get(['large', 2499])
      assert.deepEqual(last.value, { id: 2499, value: 'item-2499' })

      // Count all items
      let count = 0
      for await (const _ of db.list({ prefix: ['large'] })) {
        count++
      }
      assert.equal(count, 2500)
    } finally {
      await db.close()
    }
  })

  await test('from() with error handling - stop on error', async () => {
    const items = [
      { id: 1, value: 'valid' },
      { value: 'invalid' }, // Missing id
      { id: 3, value: 'also valid' },
    ]

    await assert.rejects(
      async () => {
        await Valkeyrie.from(items as { id: number; value: string }[], {
          prefix: ['test'],
          keyProperty: 'id',
          onError: 'stop',
        })
      },
      {
        name: 'TypeError',
        message:
          "Key property 'id' must be a valid KeyPart (Uint8Array, string, number, bigint, boolean, or symbol)",
      },
    )
  })

  await test('from() with error handling - continue on error', async () => {
    const items = [
      { id: 1, value: 'valid' },
      { value: 'invalid' }, // Missing id
      { id: 3, value: 'also valid' },
    ]
    const errors: Error[] = []

    const db = await Valkeyrie.from(items as { id: number; value: string }[], {
      prefix: ['test'],
      keyProperty: 'id',
      onError: 'continue',
      onErrorCallback: (error) => {
        errors.push(error)
      },
    })

    try {
      // Should have one error
      assert.equal(errors.length, 1)
      assert.ok(errors[0]?.message.includes('Key property'))

      // Valid items should be inserted
      const item1 = await db.get(['test', 1])
      assert.deepEqual(item1.value, { id: 1, value: 'valid' })

      const item3 = await db.get(['test', 3])
      assert.deepEqual(item3.value, { id: 3, value: 'also valid' })
    } finally {
      await db.close()
    }
  })

  await test('fromAsync() with async generator', async () => {
    async function* generateItems() {
      for (let i = 0; i < 5; i++) {
        await setTimeout(1) // Simulate async operation
        yield { id: i, value: `async-${i}` }
      }
    }

    const db = await Valkeyrie.fromAsync(generateItems(), {
      prefix: ['async'],
      keyProperty: 'id',
    })

    try {
      // Verify all items
      for (let i = 0; i < 5; i++) {
        const item = await db.get(['async', i])
        assert.deepEqual(item.value, { id: i, value: `async-${i}` })
      }
    } finally {
      await db.close()
    }
  })

  await test('fromAsync() with progress callback', async () => {
    async function* generateItems() {
      for (let i = 0; i < 10; i++) {
        yield { id: i, value: i }
      }
    }

    const progressUpdates: number[] = []
    const db = await Valkeyrie.fromAsync(generateItems(), {
      prefix: ['async-progress'],
      keyProperty: 'id',
      onProgress: (processed) => {
        progressUpdates.push(processed)
        // For async iterables, total is undefined until the end
      },
    })

    try {
      assert.ok(progressUpdates.length > 0)
      assert.equal(progressUpdates[progressUpdates.length - 1], 10)
    } finally {
      await db.close()
    }
  })

  await test('fromAsync() with large dataset (chunking)', async () => {
    async function* generateLargeDataset() {
      for (let i = 0; i < 1500; i++) {
        yield { id: i, data: `large-${i}` }
      }
    }

    const db = await Valkeyrie.fromAsync(generateLargeDataset(), {
      prefix: ['async-large'],
      keyProperty: 'id',
    })

    try {
      // Verify some items
      const first = await db.get(['async-large', 0])
      assert.deepEqual(first.value, { id: 0, data: 'large-0' })

      const last = await db.get(['async-large', 1499])
      assert.deepEqual(last.value, { id: 1499, data: 'large-1499' })

      // Count items
      let count = 0
      for await (const _ of db.list({ prefix: ['async-large'] })) {
        count++
      }
      assert.equal(count, 1500)
    } finally {
      await db.close()
    }
  })

  await test('from() with different key types', async () => {
    // Test with string keys
    const stringItems = [{ key: 'string-key', value: 1 }]
    const db1 = await Valkeyrie.from(stringItems, {
      prefix: ['keys'],
      keyProperty: 'key',
    })
    try {
      const item = await db1.get(['keys', 'string-key'])
      assert.deepEqual(item.value, { key: 'string-key', value: 1 })
    } finally {
      await db1.close()
    }

    // Test with number keys
    const numberItems = [{ key: 42, value: 2 }]
    const db2 = await Valkeyrie.from(numberItems, {
      prefix: ['keys'],
      keyProperty: 'key',
    })
    try {
      const item = await db2.get(['keys', 42])
      assert.deepEqual(item.value, { key: 42, value: 2 })
    } finally {
      await db2.close()
    }

    // Test with boolean keys
    const booleanItems = [{ key: true, value: 3 }]
    const db3 = await Valkeyrie.from(booleanItems, {
      prefix: ['keys'],
      keyProperty: 'key',
    })
    try {
      const item = await db3.get(['keys', true])
      assert.deepEqual(item.value, { key: true, value: 3 })
    } finally {
      await db3.close()
    }

    // Test with bigint keys
    const bigintItems = [{ key: 100n, value: 4 }]
    const db4 = await Valkeyrie.from(bigintItems, {
      prefix: ['keys'],
      keyProperty: 'key',
    })
    try {
      const item = await db4.get(['keys', 100n])
      assert.deepEqual(item.value, { key: 100n, value: 4 })
    } finally {
      await db4.close()
    }
  })

  await test('from() with empty prefix', async () => {
    const items = [{ id: 1, value: 'test' }]

    const db = await Valkeyrie.from(items, {
      prefix: [],
      keyProperty: 'id',
    })

    try {
      const item = await db.get([1])
      assert.deepEqual(item.value, { id: 1, value: 'test' })
    } finally {
      await db.close()
    }
  })

  await test('from() with nested prefix', async () => {
    const items = [{ id: 1, value: 'nested' }]

    const db = await Valkeyrie.from(items, {
      prefix: ['app', 'data', 'users'],
      keyProperty: 'id',
    })

    try {
      const item = await db.get(['app', 'data', 'users', 1])
      assert.deepEqual(item.value, { id: 1, value: 'nested' })
    } finally {
      await db.close()
    }
  })

  // Mock schema helper for testing
  const createMockSchema = (name: string): StandardSchemaV1 =>
    ({
      '~standard': {
        version: 1,
        vendor: 'test',
        validate: (value: unknown) => ({
          value,
          issues: undefined,
        }),
      },
      _name: name,
    }) as unknown as StandardSchemaV1

  await test('withSchema() returns ValkeyrieBuilder', async () => {
    const schema = createMockSchema('user')

    const builder = Valkeyrie.withSchema(['users', '*'], schema)

    assert.ok(builder instanceof ValkeyrieBuilder)
  })

  await test('withSchema() can be chained with open()', async () => {
    const schema = createMockSchema('user')

    const db = await Valkeyrie.withSchema(['users', '*'], schema).open()

    try {
      assert.ok(db instanceof Valkeyrie)
    } finally {
      await db.close()
    }
  })

  await test('withSchema() can be chained with from()', async () => {
    const schema = createMockSchema('user')
    const users = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]

    const db = await Valkeyrie.withSchema(['users', '*'], schema).from(users, {
      prefix: ['users'],
      keyProperty: 'id',
    })

    try {
      assert.ok(db instanceof Valkeyrie)
      const alice = await db.get(['users', 1])
      assert.deepEqual(alice.value, { id: 1, name: 'Alice' })
    } finally {
      await db.close()
    }
  })

  await test('withSchema() can be chained with fromAsync()', async () => {
    const schema = createMockSchema('user')

    async function* generateUsers() {
      yield { id: 1, name: 'Alice' }
      yield { id: 2, name: 'Bob' }
    }

    const db = await Valkeyrie.withSchema(['users', '*'], schema).fromAsync(
      generateUsers(),
      {
        prefix: ['users'],
        keyProperty: 'id',
      },
    )

    try {
      assert.ok(db instanceof Valkeyrie)
      const bob = await db.get(['users', 2])
      assert.deepEqual(bob.value, { id: 2, name: 'Bob' })
    } finally {
      await db.close()
    }
  })

  await test('withSchema() supports multiple schema registrations', async () => {
    const userSchema = createMockSchema('user')
    const postSchema = createMockSchema('post')

    const db = await Valkeyrie.withSchema(['users', '*'], userSchema)
      .withSchema(['posts', '*'], postSchema)
      .open()

    try {
      assert.ok(db instanceof Valkeyrie)
      // Just verify the database was created successfully
      // Schema validation testing is in valkeyrie-builder tests
    } finally {
      await db.close()
    }
  })

  await test('from() with invalid key extraction throws error', async () => {
    const items = [{ name: 'Alice' }] // no 'id' property

    await assert.rejects(
      async () => {
        await Valkeyrie.from(items, {
          prefix: ['users'],
          keyProperty: 'id' as keyof { name: string },
        })
      },
      {
        name: 'TypeError',
        message: /Key property 'id' must be a valid KeyPart/,
      },
    )
  })

  await test('from() with invalid key type throws error', async () => {
    const items = [{ id: { nested: 'object' } }] // object is not a valid KeyPart

    await assert.rejects(
      async () => {
        await Valkeyrie.from(items, {
          prefix: ['users'],
          keyProperty: 'id',
        })
      },
      {
        name: 'TypeError',
        message: /Key property 'id' must be a valid KeyPart/,
      },
    )
  })

  await test('fromAsync() with error handling - stop on error', async () => {
    async function* generate() {
      yield { id: 1, value: 'ok' }
      yield { id: { bad: 'object' }, value: 'bad' } // Invalid key type
      yield { id: 3, value: 'never' }
    }

    await assert.rejects(
      async () => {
        await Valkeyrie.fromAsync(generate(), {
          prefix: ['items'],
          keyProperty: 'id',
          onError: 'stop', // This is the default
        })
      },
      {
        name: 'TypeError',
      },
    )
  })

  await test('fromAsync() with error handling - continue on error', async () => {
    const errors: Array<{ error: Error; item: unknown }> = []

    async function* generate() {
      yield { id: 1, value: 'ok' }
      yield { id: { bad: 'object' }, value: 'bad' } // Invalid key type
      yield { id: 3, value: 'also ok' }
    }

    const db = await Valkeyrie.fromAsync(generate(), {
      prefix: ['items'],
      keyProperty: 'id',
      onError: 'continue',
      onErrorCallback: (error, item) => {
        errors.push({ error, item })
      },
    })

    try {
      // First and third items should be inserted
      const item1 = await db.get(['items', 1])
      assert.deepEqual(item1.value, { id: 1, value: 'ok' })

      const item3 = await db.get(['items', 3])
      assert.deepEqual(item3.value, { id: 3, value: 'also ok' })

      // Error callback should have been called once for the invalid item
      assert.strictEqual(errors.length, 1)
      assert.ok(errors[0]?.error instanceof TypeError)
      assert.deepEqual(errors[0]?.item, { id: { bad: 'object' }, value: 'bad' })
    } finally {
      await db.close()
    }
  })

  await test('fromAsync() with expireIn option', async () => {
    async function* generate() {
      yield { id: 1, value: 'expires soon' }
    }

    const db = await Valkeyrie.fromAsync(generate(), {
      prefix: ['temp'],
      keyProperty: 'id',
      expireIn: 100, // 100ms TTL
    })

    try {
      // Item should exist immediately
      const item1 = await db.get(['temp', 1])
      assert.deepEqual(item1.value, { id: 1, value: 'expires soon' })

      // Wait for expiration
      await setTimeout(150)

      // Trigger cleanup
      await db.cleanup()

      // Item should be gone
      const item2 = await db.get(['temp', 1])
      assert.strictEqual(item2.value, null)
    } finally {
      await db.close()
    }
  })

  await test('from() with destroyOnClose option', async () => {
    const items = [{ id: 1, value: 'test' }]
    const dbPath = join(tmpdir(), `valkeyrie-test-${randomUUID()}.db`)

    const db = await Valkeyrie.from(items, {
      prefix: ['items'],
      keyProperty: 'id',
      path: dbPath,
      destroyOnClose: true,
    })

    // Verify file exists
    await access(dbPath)

    await db.close()

    // File should be deleted after close
    await assert.rejects(
      async () => {
        await access(dbPath)
      },
      {
        code: 'ENOENT',
      },
    )
  })

  await test('fromAsync() with empty async iterable', async () => {
    async function* generate() {
      // Empty generator
    }

    const db = await Valkeyrie.fromAsync(generate(), {
      prefix: ['empty'],
      keyProperty: 'id',
    })

    try {
      assert.ok(db instanceof Valkeyrie)
      // Database should be empty
      const items = []
      for await (const item of db.list({ prefix: ['empty'] })) {
        items.push(item)
      }
      assert.strictEqual(items.length, 0)
    } finally {
      await db.close()
    }
  })

  await test('openWithDriver() opens a working database', async () => {
    const spy: DriverSpy = {
      created: 0,
      serializerSeen: false,
      setCalls: 0,
      getCalls: 0,
    }

    const db = await Valkeyrie.openWithDriver(createSpyDriverFn(spy))

    try {
      await db.set(['greeting'], 'hello')
      const entry = await db.get(['greeting'])
      assert.strictEqual(entry.value, 'hello')
      // The provided driver function was invoked and actually used
      assert.strictEqual(spy.created, 1)
      assert.ok(spy.setCalls > 0)
      assert.ok(spy.getCalls > 0)
    } finally {
      await db.close()
    }
  })

  await test('openWithDriver() passes the serializer to the driver function', async () => {
    const spy: DriverSpy = {
      created: 0,
      serializerSeen: false,
      setCalls: 0,
      getCalls: 0,
    }

    const db = await Valkeyrie.openWithDriver(createSpyDriverFn(spy), {
      serializer: jsonSerializer,
    })

    try {
      assert.strictEqual(spy.serializerSeen, true)
      await db.set(['k'], { a: 1 })
      const entry = await db.get(['k'])
      assert.deepEqual(entry.value, { a: 1 })
    } finally {
      await db.close()
    }
  })

  await test('from() uses driverFn when provided', async () => {
    const spy: DriverSpy = {
      created: 0,
      serializerSeen: false,
      setCalls: 0,
      getCalls: 0,
    }

    const items = [{ id: 1, value: 'a' }]
    const db = await Valkeyrie.from(items, {
      prefix: ['items'],
      keyProperty: 'id',
      driverFn: createSpyDriverFn(spy),
    })

    try {
      assert.strictEqual(spy.created, 1)
      const item = await db.get(['items', 1])
      assert.deepEqual(item.value, { id: 1, value: 'a' })
    } finally {
      await db.close()
    }
  })

  await test('from() with both path and driverFn uses driverFn and ignores path', async () => {
    const spy: DriverSpy = {
      created: 0,
      serializerSeen: false,
      setCalls: 0,
      getCalls: 0,
    }
    const testPath = join(tmpdir(), `test-driverfn-${randomUUID()}.db`)

    const items = [{ id: 1, value: 'a' }]
    const db = await Valkeyrie.from(items, {
      prefix: ['items'],
      keyProperty: 'id',
      path: testPath,
      driverFn: createSpyDriverFn(spy),
    })

    try {
      assert.strictEqual(spy.created, 1)
      const item = await db.get(['items', 1])
      assert.deepEqual(item.value, { id: 1, value: 'a' })
      // driverFn takes precedence: the file at `path` is never created
      await assert.rejects(access(testPath))
    } finally {
      await db.close()
    }
  })

  await test('fromAsync() with both path and driverFn uses driverFn and ignores path', async () => {
    const spy: DriverSpy = {
      created: 0,
      serializerSeen: false,
      setCalls: 0,
      getCalls: 0,
    }
    const testPath = join(tmpdir(), `test-driverfn-async-${randomUUID()}.db`)

    async function* generate() {
      yield { id: 1, value: 'a' }
    }

    const db = await Valkeyrie.fromAsync(generate(), {
      prefix: ['items'],
      keyProperty: 'id',
      path: testPath,
      driverFn: createSpyDriverFn(spy),
    })

    try {
      assert.strictEqual(spy.created, 1)
      const item = await db.get(['items', 1])
      assert.deepEqual(item.value, { id: 1, value: 'a' })
      await assert.rejects(access(testPath))
    } finally {
      await db.close()
    }
  })
})

// Conditionally import explicit resource management tests (requires Node.js 24+)
const nodeVersion = Number.parseInt(process.versions.node.split('.')[0] ?? '0')
if (nodeVersion >= 24) {
  await import('./node-24/explicit-resource-management.ts')
}
