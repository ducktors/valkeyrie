import assert from 'node:assert'
import { after, describe, test } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import { Redis } from 'ioredis'
import type { Driver } from '../src/driver.ts'
import { KvU64 } from '../src/kv-u64.ts'
import { jsonSerializer } from '../src/serializers/json.ts'
import type { Serializer } from '../src/serializers/serializer.ts'
import { v8Serializer } from '../src/serializers/v8.ts'
import { sqliteDriver } from '../src/sqlite-driver.ts'
import { valkeyDriver } from '../src/valkey-driver.ts'
import { type DriverFactory, type Key, Valkeyrie } from '../src/valkeyrie.ts'
import { removeNamespace, testNamespace } from './drivers/valkey.ts'

const redisUrl = process.env.VALKEYRIE_TEST_REDIS_URL

describe(
  'valkeyDriver',
  { skip: redisUrl === undefined && 'set VALKEYRIE_TEST_REDIS_URL to run' },
  async () => {
    const clients: Redis[] = []
    const namespaces: string[] = []

    function connect(): Redis {
      const client = new Redis(redisUrl as string)
      clients.push(client)
      return client
    }

    function namespace(): string {
      const name = testNamespace()
      namespaces.push(name)
      return name
    }

    // Records the key hashes Valkeyrie passes to the driver's `set`.
    function recordKeyHashes(
      factory: DriverFactory,
      keyHashes: string[],
    ): DriverFactory {
      return async (serializer) => {
        const driver = await factory(serializer)
        return {
          ...driver,
          set: (keyHash, ...rest) => {
            keyHashes.push(keyHash)
            return driver.set(keyHash, ...rest)
          },
        }
      }
    }

    // Runs `interfere` right after the first read of a commit attempt, so
    // another client can change the data between that read and the write.
    // It does so for the first `times` attempts.
    function interfereAfterRead(
      factory: DriverFactory,
      interfere: () => Promise<unknown>,
      times = 1,
    ): DriverFactory {
      return async (serializer) => {
        const driver = await factory(serializer)
        let armed = false
        let remaining = times
        return {
          ...driver,
          withTransaction: <T>(callback: () => Promise<T>) =>
            driver.withTransaction(async () => {
              armed = remaining > 0
              return await callback()
            }),
          get: async (keyHash, now) => {
            const result = await driver.get(keyHash, now)
            if (armed) {
              armed = false
              remaining--
              await interfere()
            }
            return result
          },
        }
      }
    }

    const admin = connect()

    after(async () => {
      for (const name of namespaces) {
        await removeNamespace(admin, name)
      }
      await Promise.all(clients.map((client) => client.quit()))
    })

    await test('two clients share the data of one namespace', async () => {
      const name = namespace()
      const a = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      const b = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      try {
        const { versionstamp } = await a.set(['k'], 'from a')
        assert.deepStrictEqual(await b.get(['k']), {
          key: ['k'],
          value: 'from a',
          versionstamp,
        })

        await b.delete(['k'])
        assert.strictEqual((await a.get(['k'])).versionstamp, null)
      } finally {
        await a.close()
        await b.close()
      }
    })

    await test('two namespaces never see each other’s keys', async () => {
      const client = connect()
      const one = await Valkeyrie.open(
        valkeyDriver({ client, namespace: namespace() }),
      )
      const two = await Valkeyrie.open(
        valkeyDriver({ client, namespace: namespace() }),
      )
      try {
        await one.set(['k'], 1)
        assert.strictEqual((await two.get(['k'])).value, null)

        await two.set(['k'], 2)
        assert.strictEqual((await one.get(['k'])).value, 1)
        assert.strictEqual((await two.get(['k'])).value, 2)

        await one.delete(['k'])
        assert.strictEqual((await one.get(['k'])).value, null)
        assert.strictEqual((await two.get(['k'])).value, 2)
      } finally {
        await one.close()
        await two.close()
      }
    })

    await test('versionstamps come from one counter per namespace', async () => {
      const name = namespace()
      const a = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      const b = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      try {
        const first = (await a.set(['k'], 1)).versionstamp
        const second = (await b.set(['k'], 2)).versionstamp
        assert.match(first, /^[0-9a-f]{20}$/)
        assert.match(second, /^[0-9a-f]{20}$/)
        // The low 20 bits hold the shared sequence.
        const sequence = (versionstamp: string) =>
          BigInt(`0x${versionstamp}`) & 0xfffffn
        assert.strictEqual(sequence(second), sequence(first) + 1n)
      } finally {
        await a.close()
        await b.close()
      }
    })

    await test('closing the database leaves the caller’s client open', async () => {
      const client = connect()
      const db = await Valkeyrie.open(
        valkeyDriver({ client, namespace: namespace() }),
      )
      await db.close()
      assert.strictEqual(await client.ping(), 'PONG')
    })

    await test('stores each entry under its raw key bytes and indexes it', async () => {
      const name = namespace()
      const keyHashes: string[] = []
      const db = await Valkeyrie.open(
        recordKeyHashes(
          valkeyDriver({ client: connect(), namespace: name }),
          keyHashes,
        ),
      )
      try {
        await db.set(['users', 42], 'alice')
        const keyBytes = Buffer.from(keyHashes[0] as string, 'hex')
        const entryKey = Buffer.concat([Buffer.from(`{${name}}:e:`), keyBytes])

        assert.strictEqual(await admin.exists(entryKey), 1)
        assert.deepStrictEqual(
          await admin.zrangeBuffer(`{${name}}:idx`, 0, -1),
          [keyBytes],
        )

        await db.delete(['users', 42])
        assert.strictEqual(await admin.exists(entryKey), 0)
        assert.strictEqual(await admin.zcard(`{${name}}:idx`), 0)
      } finally {
        await db.close()
      }
    })

    await test('indexed keys read back as the exact key hex', async () => {
      const keys: Key[] = [
        ['a'],
        [''],
        ['ünïcödé', '🦆'],
        [1],
        [-0.5],
        [Number.MAX_VALUE],
        [10n],
        [true, false],
        [new Uint8Array([0, 255, 0])],
        ['users', 42, 7n, true, new Uint8Array([1, 2])],
      ]
      const name = namespace()
      const keyHashes: string[] = []
      const db = await Valkeyrie.open(
        recordKeyHashes(
          valkeyDriver({ client: connect(), namespace: name }),
          keyHashes,
        ),
      )
      try {
        for (const key of keys) {
          await db.set(key, 'v')
        }

        const members = (await admin.zrangeBuffer(`{${name}}:idx`, 0, -1)).map(
          (member) => member.toString('hex'),
        )
        assert.deepStrictEqual(members.toSorted(), keyHashes.toSorted())

        const decoded = members.map((member) =>
          // @ts-expect-error Accessing private method for testing
          db.decodeKeyHash(member),
        )
        for (const key of keys) {
          assert.ok(
            decoded.some((candidate) => {
              try {
                assert.deepStrictEqual(candidate, key)
                return true
              } catch {
                return false
              }
            }),
            `key ${String(key)} did not round-trip`,
          )
          assert.deepStrictEqual((await db.get(key)).key, key)
        }
      } finally {
        await db.close()
      }
    })

    await test('uses the serializer it is given', async () => {
      const calls = { serialize: 0, deserialize: 0 }
      const spy = (): Serializer => {
        const inner = jsonSerializer()
        return {
          serialize: (value) => {
            calls.serialize++
            return inner.serialize(value)
          },
          deserialize: (value) => {
            calls.deserialize++
            return inner.deserialize(value)
          },
        }
      }
      const db = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: namespace() }),
        { serializer: spy },
      )
      try {
        await db.set(['k'], { a: 1 })
        assert.deepStrictEqual((await db.get(['k'])).value, { a: 1 })
        assert.deepStrictEqual(calls, { serialize: 1, deserialize: 1 })
      } finally {
        await db.close()
      }
    })

    await test('an entry whose expiry has passed reads as missing', async () => {
      const name = namespace()
      const keyHashes: string[] = []
      const db = await Valkeyrie.open(
        recordKeyHashes(
          valkeyDriver({ client: connect(), namespace: name }),
          keyHashes,
        ),
      )
      try {
        await db.set(['k'], 'v')
        const entryKey = Buffer.concat([
          Buffer.from(`{${name}}:e:`),
          Buffer.from(keyHashes[0] as string, 'hex'),
        ])
        // Rewrite the entry with an expiry in the past.
        const entry = (await admin.getBuffer(entryKey)) as Buffer
        entry.writeBigUInt64BE(BigInt(Date.now() - 1000), 10)
        await admin.set(entryKey, entry)

        assert.strictEqual((await db.get(['k'])).value, null)
      } finally {
        await db.close()
      }
    })

    await test('a commit writes nothing when another client changed a checked key', async () => {
      const name = namespace()
      const other = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      const db = await Valkeyrie.open(
        interfereAfterRead(
          valkeyDriver({ client: connect(), namespace: name }),
          () => other.set(['k'], 'other'),
        ),
      )
      try {
        const { versionstamp } = await db.set(['k'], 'mine')
        const result = await db
          .atomic()
          .check({ key: ['k'], versionstamp })
          .set(['k'], 'commit')
          .set(['side'], 'commit')
          .commit()

        assert.deepStrictEqual(result, { ok: false })
        assert.strictEqual((await db.get(['k'])).value, 'other')
        assert.strictEqual((await db.get(['side'])).versionstamp, null)
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('a create-if-absent check fails when another client created the key first', async () => {
      const name = namespace()
      const other = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      const db = await Valkeyrie.open(
        interfereAfterRead(
          valkeyDriver({ client: connect(), namespace: name }),
          () => other.set(['k'], 'other'),
        ),
      )
      try {
        const result = await db
          .atomic()
          .check({ key: ['k'], versionstamp: null })
          .set(['k'], 'mine')
          .commit()

        assert.deepStrictEqual(result, { ok: false })
        assert.strictEqual((await db.get(['k'])).value, 'other')
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('a commit retries on fresh data when another client changed a key it read', async () => {
      const name = namespace()
      const other = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      const db = await Valkeyrie.open(
        interfereAfterRead(
          valkeyDriver({ client: connect(), namespace: name }),
          () => other.set(['n'], new KvU64(10n)),
        ),
      )
      try {
        assert.ok((await db.atomic().sum(['n'], 1n).commit()).ok)
        assert.deepStrictEqual((await db.get(['n'])).value, new KvU64(11n))
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('concurrent sums from several clients all land', async () => {
      const name = namespace()
      const dbs = await Promise.all(
        Array.from({ length: 4 }, () =>
          Valkeyrie.open(valkeyDriver({ client: connect(), namespace: name })),
        ),
      )
      try {
        const results = await Promise.all(
          dbs.flatMap((db) =>
            Array.from({ length: 10 }, () =>
              db.atomic().sum(['n'], 1n).commit(),
            ),
          ),
        )
        assert.ok(results.every((result) => result.ok))
        assert.deepStrictEqual(
          (await (dbs[0] as Valkeyrie).get(['n'])).value,
          new KvU64(40n),
        )
      } finally {
        await Promise.all(dbs.map((db) => db.close()))
      }
    })

    await test('a commit gives up after repeated conflicts', async () => {
      const name = namespace()
      const other = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      let interference = 0
      const db = await Valkeyrie.open(
        interfereAfterRead(
          valkeyDriver({ client: connect(), namespace: name }),
          () => other.set(['n'], new KvU64(BigInt(++interference))),
          Number.POSITIVE_INFINITY,
        ),
      )
      try {
        assert.deepStrictEqual(await db.atomic().sum(['n'], 100n).commit(), {
          ok: false,
        })
        assert.ok(interference > 1, 'the commit was retried')
        assert.deepStrictEqual(
          (await db.get(['n'])).value,
          new KvU64(BigInt(interference)),
        )
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('reads inside a commit see its earlier writes', async () => {
      const db = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: namespace() }),
      )
      try {
        assert.ok((await db.atomic().sum(['n'], 1n).sum(['n'], 2n).commit()).ok)
        assert.deepStrictEqual((await db.get(['n'])).value, new KvU64(3n))

        await db.set(['m'], new KvU64(5n))
        assert.ok((await db.atomic().delete(['m']).sum(['m'], 1n).commit()).ok)
        assert.deepStrictEqual((await db.get(['m'])).value, new KvU64(1n))
      } finally {
        await db.close()
      }
    })

    await test('concurrent commits on one instance run one at a time', async () => {
      const client = connect()
      const driver = await valkeyDriver({ client, namespace: namespace() })(
        v8Serializer,
      )
      const db = await Valkeyrie.open(
        valkeyDriver({ client, namespace: namespace() }),
      )
      try {
        const events: string[] = []
        const run = (id: number) =>
          driver.withTransaction(async () => {
            events.push(`start ${id}`)
            await setTimeout(20)
            events.push(`end ${id}`)
          })
        await Promise.all([run(1), run(2)])
        assert.deepStrictEqual(events, ['start 1', 'end 1', 'start 2', 'end 2'])

        const results = await Promise.all([
          db
            .atomic()
            .check({ key: ['a'], versionstamp: null })
            .set(['a'], 1)
            .commit(),
          db
            .atomic()
            .check({ key: ['b'], versionstamp: null })
            .set(['b'], 2)
            .commit(),
          ...Array.from({ length: 20 }, () =>
            db.atomic().sum(['n'], 1n).commit(),
          ),
        ])
        assert.ok(results.every((result) => result.ok))
        assert.strictEqual((await db.get(['a'])).value, 1)
        assert.strictEqual((await db.get(['b'])).value, 2)
        assert.deepStrictEqual((await db.get(['n'])).value, new KvU64(20n))
      } finally {
        await driver.close()
        await db.close()
      }
    })

    await test('commits release the keys they watched', async () => {
      const name = namespace()
      const other = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      const db = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: name }),
      )
      try {
        // A commit that throws after reading 's'.
        await db.set(['s'], 'not a number')
        await assert.rejects(db.atomic().sum(['s'], 1n).commit(), TypeError)
        // A commit whose check on 'c' fails, so it has nothing to write.
        await db.set(['c'], 1)
        assert.deepStrictEqual(
          await db
            .atomic()
            .check({ key: ['c'], versionstamp: '00000000000000000001' })
            .set(['c'], 2)
            .commit(),
          { ok: false },
        )

        await other.set(['s'], 'changed')
        await other.set(['c'], 'changed')
        assert.ok((await db.atomic().set(['x'], 1).commit()).ok)
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('commits share one extra connection, closed with the database', async () => {
      const client = connect()
      const duplicates: Redis[] = []
      const duplicate = client.duplicate.bind(client)
      client.duplicate = (...args: Parameters<Redis['duplicate']>) => {
        const connection = duplicate(...args)
        duplicates.push(connection)
        return connection
      }
      const db = await Valkeyrie.open(
        valkeyDriver({ client, namespace: namespace() }),
      )
      await db.set(['k'], 1)
      assert.strictEqual(duplicates.length, 0)

      await db.atomic().sum(['n'], 1n).commit()
      await db.atomic().sum(['n'], 1n).commit()
      assert.strictEqual(duplicates.length, 1)

      await db.close()
      await assert.rejects((duplicates[0] as Redis).ping())
      assert.strictEqual(await client.ping(), 'PONG')
    })

    // Raw key hashes, in key order. KEY_A_NUL_B is ['a\0b'] as encoded once
    // #306 escapes NUL bytes: its bytes start with those of ['a'], then 0xff.
    const hex = (...bytes: number[]) => Buffer.from(bytes).toString('hex')
    const KEY_A = hex(0x02, 0x61, 0x00)
    const KEY_A_B = hex(0x02, 0x61, 0x00, 0x02, 0x62, 0x00)
    const KEY_A_C = hex(0x02, 0x61, 0x00, 0x02, 0x63, 0x00)
    const KEY_A_NUL_B = hex(0x02, 0x61, 0x00, 0xff, 0x62, 0x00)
    const KEY_B = hex(0x02, 0x62, 0x00)
    const ALL_KEYS = [KEY_A, KEY_A_B, KEY_A_C, KEY_A_NUL_B, KEY_B]

    const BOUND_CASES: {
      name: string
      start: string
      end: string
      prefix: string
      expected: string[]
    }[] = [
      {
        name: 'the whole key space',
        start: '',
        end: '￿',
        prefix: '',
        expected: ALL_KEYS,
      },
      {
        name: 'an inclusive start',
        start: KEY_A_B,
        end: '￿',
        prefix: '',
        expected: [KEY_A_B, KEY_A_C, KEY_A_NUL_B, KEY_B],
      },
      {
        name: 'a start after a key',
        start: `${KEY_A_B}\0`,
        end: '￿',
        prefix: '',
        expected: [KEY_A_C, KEY_A_NUL_B, KEY_B],
      },
      {
        name: 'the end of a prefix (0.8.4 form)',
        start: KEY_A,
        end: `${KEY_A}\xff`,
        prefix: KEY_A,
        expected: [KEY_A_B, KEY_A_C, KEY_A_NUL_B],
      },
      {
        name: 'the end of a prefix (form after #306)',
        start: KEY_A,
        end: `${KEY_A}ff`,
        prefix: KEY_A,
        expected: [KEY_A_B, KEY_A_C],
      },
      {
        name: 'an exclusive end',
        start: '',
        end: KEY_A_C,
        prefix: '',
        expected: [KEY_A, KEY_A_B],
      },
    ]

    await test('list bounds select the same keys as on SQLite', async () => {
      const valkey = await valkeyDriver({
        client: connect(),
        namespace: namespace(),
      })(v8Serializer)
      const sqlite = await sqliteDriver(':memory:', v8Serializer)
      try {
        for (const keyHash of ALL_KEYS) {
          await valkey.set(keyHash, keyHash, '0'.repeat(20))
          await sqlite.set(keyHash, keyHash, '0'.repeat(20))
        }
        const now = Date.now()
        for (const { name, start, end, prefix, expected } of BOUND_CASES) {
          for (const reverse of [false, true]) {
            const order = reverse ? expected.toReversed() : expected
            const label = `${name}${reverse ? ', reversed' : ''}`
            const fromSqlite = await sqlite.list(
              start,
              end,
              prefix,
              now,
              10,
              reverse,
            )
            const fromValkey = await valkey.list(
              start,
              end,
              prefix,
              now,
              10,
              reverse,
            )
            assert.deepStrictEqual(
              fromSqlite.map((e) => e.keyHash),
              order,
              `SQLite: ${label}`,
            )
            assert.deepStrictEqual(fromValkey, fromSqlite, `Valkey: ${label}`)
            assert.deepStrictEqual(
              await valkey.list(start, end, prefix, now, 1, reverse),
              fromSqlite.slice(0, 1),
              `Valkey, limit 1: ${label}`,
            )
          }
        }
      } finally {
        await valkey.close()
        await sqlite.close()
      }
    })

    await test('list skips index members whose entry is gone or expired', async () => {
      const name = namespace()
      const driver = await valkeyDriver({ client: connect(), namespace: name })(
        v8Serializer,
      )
      const entryKey = (keyHash: string) =>
        Buffer.concat([
          Buffer.from(`{${name}}:e:`),
          Buffer.from(keyHash, 'hex'),
        ])
      try {
        const keyHashes = Array.from({ length: 10 }, (_, i) =>
          hex(0x04, i, 0x00),
        )
        for (const keyHash of keyHashes) {
          await driver.set(keyHash, keyHash, '0'.repeat(20))
        }
        // Entries 0-3 disappear behind the driver's back, as on eviction.
        await admin.unlink(...keyHashes.slice(0, 4).map(entryKey))
        // Entries 4-6 have expired.
        for (const keyHash of keyHashes.slice(4, 7)) {
          const entry = (await admin.getBuffer(entryKey(keyHash))) as Buffer
          entry.writeBigUInt64BE(BigInt(Date.now() - 1000), 10)
          await admin.set(entryKey(keyHash), entry)
        }
        const now = Date.now()

        // The first index read finds only missing or expired entries, so
        // filling the limit takes more reads.
        const forward = await driver.list('', '￿', '', now, 2)
        assert.deepStrictEqual(
          forward.map((e) => e.keyHash),
          keyHashes.slice(7, 9),
        )
        assert.deepStrictEqual(
          forward.map((e) => e.value),
          keyHashes.slice(7, 9),
        )
        const reverse = await driver.list('', '￿', '', now, 10, true)
        assert.deepStrictEqual(
          reverse.map((e) => e.keyHash),
          keyHashes.slice(7).toReversed(),
        )
      } finally {
        await driver.close()
      }
    })

    // A driver and the Redis keys of its namespace.
    async function openDriver(client = connect()) {
      const name = namespace()
      const driver = await valkeyDriver({ client, namespace: name })(
        v8Serializer,
      )
      const keys = {
        entry: (keyHash: string) =>
          Buffer.concat([
            Buffer.from(`{${name}}:e:`),
            Buffer.from(keyHash, 'hex'),
          ]),
        index: `{${name}}:idx`,
        expiry: `{${name}}:exp`,
        sequence: `{${name}}:seq`,
      }
      const members = async (key: string) =>
        (await admin.zrangeBuffer(key, 0, -1)).map((member) =>
          member.toString('hex'),
        )
      return { driver, name, keys, members }
    }
    const VERSIONSTAMP = '0'.repeat(20)

    await test('a set with an expiry gets a TTL, and a plain set clears it', async () => {
      const { driver, keys, members } = await openDriver()
      const keyHash = hex(0x02, 0x6b, 0x00)
      try {
        const expiresAt = Date.now() + 60_000
        await driver.set(keyHash, 1, VERSIONSTAMP, expiresAt)
        const ttl = await admin.pttl(keys.entry(keyHash))
        assert.ok(ttl > 0 && ttl <= 60_000, `TTL ${ttl}`)
        assert.strictEqual(
          await admin.zscore(keys.expiry, Buffer.from(keyHash, 'hex')),
          String(expiresAt),
        )
        assert.strictEqual((await driver.get(keyHash, Date.now()))?.value, 1)
        assert.strictEqual(await driver.get(keyHash, expiresAt), undefined)

        await driver.set(keyHash, 2, VERSIONSTAMP)
        assert.strictEqual(await admin.pttl(keys.entry(keyHash)), -1)
        assert.deepStrictEqual(await members(keys.expiry), [])
        assert.strictEqual((await driver.get(keyHash, expiresAt))?.value, 2)

        // A fractional expiry time, from a fractional `expireIn`, rounds up.
        await driver.set(keyHash, 3, VERSIONSTAMP, expiresAt + 0.5)
        assert.strictEqual(
          await admin.zscore(keys.expiry, Buffer.from(keyHash, 'hex')),
          String(expiresAt + 1),
        )
        await driver.delete(keyHash)
        assert.deepStrictEqual(await members(keys.index), [])
        assert.deepStrictEqual(await members(keys.expiry), [])
      } finally {
        await driver.close()
      }
    })

    await test('cleanup removes expired entries and their index members', async () => {
      const { driver, keys, members } = await openDriver()
      const [expired1, expired2, evicted, live, later] = [1, 2, 3, 4, 5].map(
        (i) => hex(0x04, i, 0x00),
      )
      try {
        const soon = Date.now() + 50
        await driver.set(expired1 as string, 'x', VERSIONSTAMP, soon)
        await driver.set(expired2 as string, 'x', VERSIONSTAMP, soon)
        await driver.set(evicted as string, 'x', VERSIONSTAMP, soon)
        await driver.set(live as string, 'x', VERSIONSTAMP)
        await driver.set(later as string, 'x', VERSIONSTAMP, soon + 3_600_000)
        // Redis evicts an entry before it expires.
        await admin.unlink(keys.entry(evicted as string))
        await setTimeout(100)

        await driver.cleanup(Date.now())
        assert.deepStrictEqual(await members(keys.index), [live, later])
        assert.deepStrictEqual(await members(keys.expiry), [later])
        assert.strictEqual(
          await admin.exists(keys.entry(expired1 as string)),
          0,
        )
      } finally {
        await driver.close()
      }
    })

    await test('cleanup keeps a key that another client sets again meanwhile', async () => {
      const client = connect()
      const { driver, name, keys, members } = await openDriver(client)
      const other = await valkeyDriver({ client: connect(), namespace: name })(
        v8Serializer,
      )
      const keyHash = hex(0x02, 0x6b, 0x00)
      // Right after cleanup reads the expiry index, the other client sets
      // the key again, without an expiry.
      const read = client.zrangebyscoreBuffer.bind(client)
      let interfered = false
      client.zrangebyscoreBuffer = (async (
        ...args: Parameters<typeof read>
      ) => {
        const result = await read(...args)
        if (!interfered) {
          interfered = true
          await other.set(keyHash, 'again', VERSIONSTAMP)
        }
        return result
      }) as typeof read
      try {
        await driver.set(keyHash, 'expiring', VERSIONSTAMP, Date.now() + 50)
        await setTimeout(100)

        await driver.cleanup(Date.now())
        assert.ok(interfered)
        assert.strictEqual(
          (await driver.get(keyHash, Date.now()))?.value,
          'again',
        )
        assert.deepStrictEqual(await members(keys.index), [keyHash])
        assert.deepStrictEqual(await members(keys.expiry), [])
      } finally {
        await driver.close()
        await other.close()
      }
    })

    await test('a cleanup running when the driver closes opens no connection', async () => {
      const client = connect()
      const duplicates: Redis[] = []
      const duplicate = client.duplicate.bind(client)
      client.duplicate = (...args: Parameters<Redis['duplicate']>) => {
        const connection = duplicate(...args)
        duplicates.push(connection)
        return connection
      }
      const { driver } = await openDriver(client)
      try {
        await driver.set(
          hex(0x02, 0x6b, 0x00),
          'x',
          VERSIONSTAMP,
          Date.now() + 10,
        )
        await setTimeout(50)
        // The driver closes right after cleanup reads the expiry index.
        const read = client.zrangebyscoreBuffer.bind(client)
        client.zrangebyscoreBuffer = (async (
          ...args: Parameters<typeof read>
        ) => {
          const result = await read(...args)
          await driver.close()
          return result
        }) as typeof read

        await driver.cleanup(Date.now())
        for (const connection of duplicates) {
          await assert.rejects(connection.ping())
        }
      } finally {
        for (const connection of duplicates) {
          connection.disconnect()
        }
      }
    })

    await test('two processes can run cleanup together', async () => {
      const { driver: one, name, keys, members } = await openDriver()
      const two = await valkeyDriver({ client: connect(), namespace: name })(
        v8Serializer,
      )
      try {
        const expired = Array.from({ length: 1500 }, (_, i) =>
          hex(0x04, i >> 8, i & 0xff, 0x00),
        )
        const soon = Date.now() + 50
        for (const keyHash of expired) {
          await one.set(keyHash, 'x', VERSIONSTAMP, soon)
        }
        const live = hex(0x02, 0x6c, 0x00)
        await one.set(live, 'x', VERSIONSTAMP)
        await setTimeout(100)

        const now = Date.now()
        await Promise.all([one.cleanup(now), two.cleanup(now)])
        // A run that lost a race to the other leaves the rest to the next run.
        await one.cleanup(now)
        assert.deepStrictEqual(await members(keys.index), [live])
        assert.deepStrictEqual(await members(keys.expiry), [])
      } finally {
        await one.close()
        await two.close()
      }
    })

    await test('watch emits current values, then after each change through the same instance', async () => {
      const db = await Valkeyrie.open(
        valkeyDriver({ client: connect(), namespace: namespace() }),
      )
      const reader = db.watch([['a'], ['b']]).getReader()
      const next = async () => {
        const { value } = await reader.read()
        return value?.map((entry) => entry.value)
      }
      try {
        assert.deepStrictEqual(await next(), [null, null])
        await db.set(['a'], 1)
        assert.deepStrictEqual(await next(), [1, null])
        await db.set(['unwatched'], 'no event')
        await db.atomic().set(['b'], 2).commit()
        assert.deepStrictEqual(await next(), [1, 2])
        await db.delete(['a'])
        assert.deepStrictEqual(await next(), [null, 2])
        await db.clear()
        assert.deepStrictEqual(await next(), [null, null])
      } finally {
        await reader.cancel()
        await db.close()
      }
    })

    await test('clear deletes entries and indexes but keeps the sequence', async () => {
      const { driver, name, keys } = await openDriver()
      try {
        await driver.set(
          hex(0x02, 0x61, 0x00),
          1,
          await driver.generateVersionstamp(),
        )
        const before = await driver.generateVersionstamp()
        await driver.set(hex(0x02, 0x62, 0x00), 2, before, Date.now() + 60_000)

        await driver.clear()
        assert.deepStrictEqual(
          await driver.list('', '￿', '', Date.now(), 10),
          [],
        )
        const left = await admin.keys(`{${name}}:*`)
        assert.deepStrictEqual(left, [keys.sequence])
        const after = await driver.generateVersionstamp()
        assert.ok(
          (BigInt(`0x${after}`) & 0xfffffn) >
            (BigInt(`0x${before}`) & 0xfffffn),
        )
      } finally {
        await driver.close()
      }
    })

    await test('destroy deletes every key of the namespace', async () => {
      const { driver, name } = await openDriver()
      try {
        await driver.set(
          hex(0x02, 0x61, 0x00),
          1,
          await driver.generateVersionstamp(),
        )
        await driver.set(
          hex(0x02, 0x62, 0x00),
          2,
          VERSIONSTAMP,
          Date.now() + 60_000,
        )
        await driver.destroy()
        assert.deepStrictEqual(await admin.keys(`{${name}}:*`), [])
      } finally {
        await driver.close()
      }
    })

    await test('uses only the commands listed in the design', async () => {
      const allowed = new Set([
        'get',
        'mget',
        'set',
        'unlink',
        'incr',
        'zadd',
        'zrem',
        'zrangebylex',
        'zrevrangebylex',
        'zrangebyscore',
        'watch',
        'multi',
        'exec',
        'unwatch',
      ])
      // Sent by ioredis itself while it connects.
      const connectionSetup = new Set([
        'info',
        'client',
        'select',
        'auth',
        'hello',
      ])
      const used = new Set<string>()
      const record = (redis: Redis): Redis => {
        const send = redis.sendCommand.bind(redis)
        redis.sendCommand = (...args: Parameters<Redis['sendCommand']>) => {
          used.add(args[0].name.toLowerCase())
          return send(...args)
        }
        const duplicate = redis.duplicate.bind(redis)
        redis.duplicate = (...args: Parameters<Redis['duplicate']>) =>
          record(duplicate(...args))
        return redis
      }

      let driver: Driver | undefined
      const factory = valkeyDriver({
        client: record(connect()),
        namespace: namespace(),
      })
      const db = await Valkeyrie.open(async (serializer) => {
        driver = await factory(serializer)
        return driver
      })
      try {
        const reader = db.watch([['a']]).getReader()
        await reader.read()
        await db.set(['a'], 1)
        await reader.read()
        await reader.cancel()
        await db.set(['b'], 2, { expireIn: 50 })
        await db.getMany([['a'], ['b']])
        await Array.fromAsync(db.list({ prefix: [] }))
        await Array.fromAsync(db.list({ prefix: [] }, { reverse: true }))
        await db
          .atomic()
          .check({
            key: ['a'],
            versionstamp: (await db.get(['a'])).versionstamp,
          })
          .set(['a'], 3)
          .sum(['n'], 1n)
          .commit()
        await db.delete(['a'])
        await setTimeout(100)
        await (driver as Driver).cleanup(Date.now())
        await db.clear()
        await db.destroy()
      } finally {
        await db.close()
      }

      const sent = [...used].filter((name) => !connectionSetup.has(name))
      assert.deepStrictEqual(
        sent.filter((name) => !allowed.has(name)),
        [],
      )
      for (const name of ['watch', 'exec', 'zrangebylex', 'zrangebyscore']) {
        assert.ok(used.has(name), `${name} was not recorded`)
      }
    })

    await test('rejects a namespace that breaks the hash tag', () => {
      const client = connect()
      for (const name of ['', 'a}b', '{a']) {
        assert.throws(() => valkeyDriver({ client, namespace: name }), {
          name: 'TypeError',
        })
      }
    })
  },
)
