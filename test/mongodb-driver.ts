import assert from 'node:assert'
import { type TestContext, after, describe, test } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import {
  Binary,
  type ClientSession,
  Collection,
  MongoClient,
  ObjectId,
} from 'mongodb'
import { KEY_FORMAT_KEY_HASH } from '../src/key-format.ts'
import { KvU64 } from '../src/kv-u64.ts'
import { mongodbDriver } from '../src/mongodb-driver.ts'
import { jsonSerializer } from '../src/serializers/json.ts'
import type { Serializer } from '../src/serializers/serializer.ts'
import { v8Serializer } from '../src/serializers/v8.ts'
import { sqliteDriver } from '../src/sqlite-driver.ts'
import { type DriverFactory, type Key, Valkeyrie } from '../src/valkeyrie.ts'
import {
  removeNamespace,
  testDbName,
  testNamespace,
} from './drivers/mongodb.ts'

const mongodbUrl = process.env.VALKEYRIE_TEST_MONGODB_URL

// 0xFF, then "commit-lock" in ASCII.
const COMMIT_LOCK_KEY_HASH = 'ff636f6d6d69742d6c6f636b'

describe(
  'mongodbDriver',
  { skip: mongodbUrl === undefined && 'set VALKEYRIE_TEST_MONGODB_URL to run' },
  async () => {
    const clients: MongoClient[] = []
    const namespaces: { name: unknown; field: string }[] = []

    function connect(): MongoClient {
      const client = new MongoClient(mongodbUrl as string)
      clients.push(client)
      return client
    }

    function namespace(name: unknown = testNamespace(), field = 'namespace') {
      namespaces.push({ name, field })
      return name
    }

    function driverFor(
      name: unknown,
      client: MongoClient = connect(),
    ): DriverFactory {
      return mongodbDriver({
        client,
        namespace: name as string,
        dbName: testDbName,
      })
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
            if (keyHash !== KEY_FORMAT_KEY_HASH) {
              keyHashes.push(keyHash)
            }
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
            if (armed && keyHash !== KEY_FORMAT_KEY_HASH) {
              armed = false
              remaining--
              await interfere()
            }
            return result
          },
        }
      }
    }

    // Counts the transactions that `client` starts: one per attempt of a
    // commit, also an attempt that conflicts on the commit lock row.
    function countAttempts(t: TestContext, client: MongoClient) {
      const attempts = { count: 0 }
      const startSession = client.startSession.bind(client)
      t.mock.method(client, 'startSession', (...args: unknown[]) => {
        const session: ClientSession = Reflect.apply(startSession, client, args)
        const startTransaction = session.startTransaction.bind(session)
        session.startTransaction = (...options) => {
          attempts.count++
          return startTransaction(...options)
        }
        return session
      })
      return attempts
    }

    // Rejects if a watch read does not arrive promptly, so a missing event
    // fails the test instead of hanging it.
    async function readWithin<T>(
      reader: ReadableStreamDefaultReader<T>,
      ms = 2000,
    ): Promise<T | undefined> {
      const timeout = AbortSignal.timeout(ms)
      const { value } = await Promise.race([
        reader.read(),
        new Promise<never>((_, reject) =>
          timeout.addEventListener('abort', () =>
            reject(new Error(`no watch event within ${ms} ms`)),
          ),
        ),
      ])
      return value
    }

    const admin = connect()
    const entries = admin.db(testDbName).collection('valkeyrie')
    const sequences = admin.db(testDbName).collection('valkeyrie_sequences')

    after(async () => {
      for (const { name, field } of namespaces) {
        await removeNamespace(admin, name, field)
      }
      await Promise.all(clients.map((client) => client.close()))
    })

    await test('two clients share the data of one namespace', async () => {
      const name = namespace()
      const a = await Valkeyrie.open(driverFor(name))
      const b = await Valkeyrie.open(driverFor(name))
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
      const oneName = namespace()
      const one = await Valkeyrie.open(driverFor(oneName, client))
      const two = await Valkeyrie.open(driverFor(namespace(), client))
      try {
        await one.set(['items', 1], 'one')
        await one.set(['expiring'], 'one', { expireIn: 50 })
        assert.strictEqual((await two.get(['items', 1])).value, null)

        await two.set(['items', 1], 'two')
        await two.set(['items', 2], 'two')
        assert.deepStrictEqual(
          (await Array.fromAsync(one.list({ prefix: ['items'] }))).map(
            (entry) => entry.value,
          ),
          ['one'],
        )

        await setTimeout(100)
        await two.cleanup()
        await two.clear()
        assert.strictEqual((await one.get(['items', 1])).value, 'one')
        assert.strictEqual(
          await entries.countDocuments({
            namespace: oneName,
            expiresAt: { $exists: true },
          }),
          1,
          'the other namespace keeps its expired entry until it cleans up',
        )
      } finally {
        await one.close()
        await two.close()
      }
    })

    await test('versionstamps come from one counter per namespace', async () => {
      const name = namespace()
      const a = await Valkeyrie.open(driverFor(name))
      const b = await Valkeyrie.open(driverFor(name))
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
      const db = await Valkeyrie.open(driverFor(namespace(), client))
      await db.close()
      assert.strictEqual((await client.db('admin').command({ ping: 1 })).ok, 1)
    })

    await test('stores one document per entry, with the namespace as given', async () => {
      // One namespace field per collection: the unique index covers it.
      const name = new ObjectId()
      const collectionName = `valkeyrie-workspaces-${name}`
      const sequenceCollectionName = `${collectionName}-sequences`
      const entries = admin.db(testDbName).collection(collectionName)
      const sequences = admin.db(testDbName).collection(sequenceCollectionName)
      const keyHashes: string[] = []
      const db = await Valkeyrie.open(
        recordKeyHashes(
          mongodbDriver({
            client: connect(),
            namespace: name,
            namespaceField: 'workspaceId',
            dbName: testDbName,
            collectionName,
            sequenceCollectionName,
          }),
          keyHashes,
        ),
      )
      try {
        await db.set(['users', 42], 'alice', { expireIn: 60_000 })
        await db.set(['users', 43], 'bob')

        const docs = await entries
          .find({ workspaceId: name }, { sort: { keyHash: 1 } })
          .toArray()
        assert.deepStrictEqual(
          docs.map((doc) => doc.keyHash),
          // Every commit, the key format check of open() too, writes the
          // commit lock row.
          [...keyHashes, COMMIT_LOCK_KEY_HASH, KEY_FORMAT_KEY_HASH],
        )
        const [alice, bob] = docs
        assert.ok(alice?.workspaceId instanceof ObjectId)
        assert.ok(alice.workspaceId.equals(name))
        assert.ok(alice.value instanceof Binary)
        assert.match(alice.versionstamp, /^[0-9a-f]{20}$/)
        assert.strictEqual(typeof alice.expiresAt, 'number')
        assert.strictEqual(bob?.expiresAt, undefined)
        assert.strictEqual(alice.namespace, undefined)
        assert.strictEqual(
          await sequences.countDocuments({ workspaceId: name }),
          1,
        )

        const indexes = (await entries.indexes()).map((index) => index.key)
        assert.ok(
          indexes.some(
            (key) =>
              JSON.stringify(key) ===
              JSON.stringify({ workspaceId: 1, keyHash: 1 }),
          ),
        )
      } finally {
        await db.close()
        await entries.drop()
        await sequences.drop()
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
      const db = await Valkeyrie.open(driverFor(namespace()), {
        serializer: spy,
      })
      try {
        await db.set(['k'], { a: 1 })
        assert.deepStrictEqual((await db.get(['k'])).value, { a: 1 })
        assert.ok(calls.serialize > 0 && calls.deserialize > 0)
      } finally {
        await db.close()
      }
    })

    await test('works whatever BSON options the caller’s client sets', async () => {
      // promoteBuffers reads values as Buffers, promoteValues: false reads
      // the sequence as an Int32.
      const client = new MongoClient(mongodbUrl as string, {
        promoteBuffers: true,
        promoteValues: false,
      })
      clients.push(client)
      const db = await Valkeyrie.open(driverFor(namespace(), client))
      const { versionstamp } = await db.set(['a'], { n: 1 })
      // The first event of a watch reads the value.
      const reader = db.watch([['a']]).getReader()
      try {
        assert.deepStrictEqual(await db.get(['a']), {
          key: ['a'],
          value: { n: 1 },
          versionstamp,
        })
        assert.deepStrictEqual(
          (await Array.fromAsync(db.list({ prefix: [] }))).map(
            (entry) => entry.value,
          ),
          [{ n: 1 }],
        )
        assert.deepStrictEqual((await readWithin(reader))?.[0]?.value, {
          n: 1,
        })
      } finally {
        await reader.cancel()
        await db.close()
      }
    })

    await test('a plain write from another client waits for a commit that checked its key', async () => {
      // The commit checks 'k' but writes only 'side', so only the commit lock
      // row can make the other client's write conflict with it.
      const name = namespace()
      const other = await Valkeyrie.open(driverFor(name))
      const events: string[] = []
      let otherWrite: Promise<unknown> | undefined
      const db = await Valkeyrie.open(
        interfereAfterRead(driverFor(name), async () => {
          otherWrite = other
            .set(['k'], 'other')
            .then(() => events.push('other set'))
          await setTimeout(20)
        }),
      )
      try {
        const { versionstamp } = await db.set(['k'], 'mine')
        const result = await db
          .atomic()
          .check({ key: ['k'], versionstamp })
          .set(['side'], 'commit')
          .commit()
        events.push('commit')
        await otherWrite

        assert.ok(result.ok)
        assert.deepStrictEqual(events, ['commit', 'other set'])
        assert.strictEqual((await db.get(['k'])).value, 'other')
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('of two create-if-absent commits from different clients, only the first succeeds', async () => {
      const name = namespace()
      const other = await Valkeyrie.open(driverFor(name))
      let otherCommit: Promise<{ ok: boolean }> | undefined
      const db = await Valkeyrie.open(
        interfereAfterRead(driverFor(name), async () => {
          otherCommit = other
            .atomic()
            .check({ key: ['k'], versionstamp: null })
            .set(['k'], 'other')
            .commit()
          await setTimeout(20)
        }),
      )
      try {
        const result = await db
          .atomic()
          .check({ key: ['k'], versionstamp: null })
          .set(['k'], 'mine')
          .commit()

        assert.ok(result.ok)
        assert.deepStrictEqual(await otherCommit, { ok: false })
        assert.strictEqual((await db.get(['k'])).value, 'mine')
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('a commit that finds the lock row taken runs again on fresh data', async (t) => {
      const name = namespace()
      const client = connect()
      const db = await Valkeyrie.open(driverFor(name, client))
      const attempts = countAttempts(t, client)
      let dbCommit: Promise<{ ok: boolean }> | undefined
      // The other client's commit holds the lock row while db commits.
      const other = await Valkeyrie.open(
        interfereAfterRead(driverFor(name), async () => {
          dbCommit = db.atomic().sum(['n'], 1n).commit()
          await setTimeout(20)
        }),
      )
      try {
        assert.ok((await other.atomic().sum(['n'], 10n).commit()).ok)
        assert.ok((await dbCommit)?.ok)
        assert.ok(attempts.count > 1, 'the commit ran again')
        assert.deepStrictEqual((await db.get(['n'])).value, new KvU64(11n))
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('two commits that each check what the other writes cannot both succeed', async () => {
      // Snapshot isolation alone lets both commit: neither writes a document
      // that the other writes. The commit lock row makes them conflict.
      const name = namespace()
      const other = await Valkeyrie.open(driverFor(name))
      const x = await other.set(['x'], 0)
      const y = await other.set(['y'], 0)
      let otherResult: { ok: boolean } | undefined
      const db = await Valkeyrie.open(
        interfereAfterRead(driverFor(name), async () => {
          otherResult = await other
            .atomic()
            .check({ key: ['y'], versionstamp: y.versionstamp })
            .set(['x'], 'other')
            .commit()
        }),
      )
      try {
        const result = await db
          .atomic()
          .check({ key: ['x'], versionstamp: x.versionstamp })
          .set(['y'], 'db')
          .commit()

        assert.ok(otherResult)
        assert.ok(
          !(result.ok && otherResult.ok),
          'both commits succeeded on checks that never held together',
        )
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('concurrent sums from several clients all land', async () => {
      const name = namespace()
      const dbs = await Promise.all(
        Array.from({ length: 4 }, () => Valkeyrie.open(driverFor(name))),
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

    await test('a commit or a plain write gives up after repeated conflicts', async (t) => {
      const name = namespace()
      const client = connect()
      const db = await Valkeyrie.open(driverFor(name, client))
      const attempts = countAttempts(t, client)
      let dbCommit: { ok: boolean } | undefined
      let commitAttempts = 0
      let dbSet: unknown
      // The other client's commit holds the lock row until db has given up.
      const other = await Valkeyrie.open(
        interfereAfterRead(driverFor(name), async () => {
          dbCommit = await db.atomic().sum(['n'], 100n).commit()
          commitAttempts = attempts.count
          dbSet = await db.set(['x'], 1).catch((error: unknown) => error)
        }),
      )
      try {
        assert.ok((await other.atomic().sum(['n'], 1n).commit()).ok)
        assert.deepStrictEqual(dbCommit, { ok: false })
        assert.strictEqual(commitAttempts, 10)
        assert.ok(dbSet instanceof Error, 'the plain write rejects')
        assert.strictEqual(attempts.count, 20)
        assert.deepStrictEqual((await db.get(['n'])).value, new KvU64(1n))
        assert.strictEqual((await db.get(['x'])).versionstamp, null)
      } finally {
        await db.close()
        await other.close()
      }
    })

    await test('a retried commit publishes only what it committed', async () => {
      const name = namespace()
      // Valkeyrie writes cannot reach 'k' while the commit runs, so a write
      // that bypasses the driver, and its lock row, makes the conflict.
      let checked: string | undefined
      const db = await Valkeyrie.open(
        interfereAfterRead(driverFor(name), () =>
          entries.updateOne(
            { namespace: name, versionstamp: checked },
            { $set: { versionstamp: '0'.repeat(20) } },
          ),
        ),
      )
      const reader = db.watch([['side']]).getReader()
      try {
        assert.strictEqual((await readWithin(reader))?.[0]?.value, null)
        checked = (await db.set(['k'], 'mine')).versionstamp
        // The first attempt writes 'side', then conflicts on 'k'. The retry
        // fails its check and writes nothing.
        const result = await db
          .atomic()
          .check({ key: ['k'], versionstamp: checked })
          .set(['side'], 'never committed')
          .set(['k'], 'commit')
          .commit()
        assert.deepStrictEqual(result, { ok: false })

        await db.set(['side'], 'later')
        assert.strictEqual((await readWithin(reader))?.[0]?.value, 'later')
      } finally {
        await reader.cancel()
        await db.close()
      }
    })

    await test('reads inside a commit see its earlier writes', async () => {
      const db = await Valkeyrie.open(driverFor(namespace()))
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

    await test('a commit that throws part-way writes nothing', async () => {
      const db = await Valkeyrie.open(driverFor(namespace()))
      try {
        await db.set(['plain'], 'not a u64')
        await assert.rejects(
          db.atomic().set(['canary'], 'written').sum(['plain'], 1n).commit(),
          TypeError,
        )
        assert.strictEqual((await db.get(['canary'])).versionstamp, null)
      } finally {
        await db.close()
      }
    })

    await test('concurrent commits on one instance run one at a time', async () => {
      const client = connect()
      const driver = await driverFor(namespace(), client)(v8Serializer)
      const db = await Valkeyrie.open(driverFor(namespace(), client))
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

    // Raw key hashes, in key order. KEY_A_NUL_B is ['a\0b']: its bytes start
    // with those of ['a'], then 0xff.
    const hex = (...bytes: number[]) => Buffer.from(bytes).toString('hex')
    const KEY_A = hex(0x02, 0x61, 0x00)
    const KEY_A_B = hex(0x02, 0x61, 0x00, 0x02, 0x62, 0x00)
    const KEY_A_C = hex(0x02, 0x61, 0x00, 0x02, 0x63, 0x00)
    const KEY_A_NUL_B = hex(0x02, 0x61, 0x00, 0xff, 0x62, 0x00)
    const KEY_B = hex(0x02, 0x62, 0x00)
    const ALL_KEYS = [KEY_A, KEY_A_B, KEY_A_C, KEY_A_NUL_B, KEY_B]
    const VERSIONSTAMP = '0'.repeat(20)

    await test('list bounds select the same keys as on SQLite', async () => {
      const cases = [
        { start: '', end: '￿', prefix: '' },
        { start: KEY_A_B, end: '￿', prefix: '' },
        { start: `${KEY_A_B}\0`, end: '￿', prefix: '' },
        { start: KEY_A, end: `${KEY_A}\xff`, prefix: KEY_A },
        { start: KEY_A, end: `${KEY_A}ff`, prefix: KEY_A },
        { start: '', end: KEY_A_C, prefix: '' },
        { start: '', end: 'ff', prefix: '' },
      ]
      const mongo = await driverFor(namespace())(v8Serializer)
      const sqlite = await sqliteDriver(':memory:', v8Serializer)
      try {
        for (const keyHash of ALL_KEYS) {
          await mongo.set(keyHash, keyHash, VERSIONSTAMP)
          await sqlite.set(keyHash, keyHash, VERSIONSTAMP)
        }
        const now = Date.now()
        for (const { start, end, prefix } of cases) {
          for (const reverse of [false, true]) {
            for (const limit of [10, 1]) {
              const label = JSON.stringify({ start, end, reverse, limit })
              assert.deepStrictEqual(
                await mongo.list(start, end, prefix, now, limit, reverse),
                await sqlite.list(start, end, prefix, now, limit, reverse),
                label,
              )
            }
          }
        }
      } finally {
        await mongo.close()
        await sqlite.close()
      }
    })

    await test('a set with an expiry stores it, and a plain set removes it', async () => {
      const name = namespace()
      const driver = await driverFor(name)(v8Serializer)
      try {
        const expiresAt = Date.now() + 60_000
        await driver.set(KEY_A, 1, VERSIONSTAMP, expiresAt)
        assert.strictEqual(
          (await entries.findOne({ namespace: name, keyHash: KEY_A }))
            ?.expiresAt,
          expiresAt,
        )
        assert.strictEqual((await driver.get(KEY_A, Date.now()))?.value, 1)
        assert.strictEqual(await driver.get(KEY_A, expiresAt), undefined)

        await driver.set(KEY_A, 2, VERSIONSTAMP)
        assert.strictEqual(
          (await entries.findOne({ namespace: name, keyHash: KEY_A }))
            ?.expiresAt,
          undefined,
        )
        assert.strictEqual((await driver.get(KEY_A, expiresAt))?.value, 2)
      } finally {
        await driver.close()
      }
    })

    await test('watch emits current values, then after each change through the same instance', async () => {
      const db = await Valkeyrie.open(driverFor(namespace()))
      const reader = db.watch([['a'], ['b']]).getReader()
      const next = async () =>
        (await readWithin(reader))?.map((entry) => entry.value)
      try {
        assert.deepStrictEqual(await next(), [null, null])
        await db.set(['a'], 1)
        assert.deepStrictEqual(await next(), [1, null])
        await db.set(['unwatched'], 'no event')
        await db.atomic().set(['b'], 2).commit()
        assert.deepStrictEqual(await next(), [1, 2])
        await db.delete(['a'])
        assert.deepStrictEqual(await next(), [null, 2])
        // Valkeyrie clears inside a commit; the deletions still reach watchers.
        await db.clear()
        assert.deepStrictEqual(await next(), [null, null])
      } finally {
        await reader.cancel()
        await db.close()
      }
    })

    await test('cleanup notifies the watchers of the keys it deleted, and only them', async () => {
      const db = await Valkeyrie.open(driverFor(namespace()))
      await db.set(['live'], 'v1')
      await db.set(['dying'], 'soon gone', { expireIn: 50 })
      const both = db.watch([['live'], ['dying']]).getReader()
      const liveOnly = db.watch([['live']]).getReader()
      try {
        assert.deepStrictEqual(
          (await readWithin(both))?.map((entry) => entry.value),
          ['v1', 'soon gone'],
        )
        assert.strictEqual((await readWithin(liveOnly))?.[0]?.value, 'v1')

        await setTimeout(100)
        await db.cleanup()
        assert.deepStrictEqual(
          (await readWithin(both))?.map((entry) => entry.versionstamp),
          [(await db.get(['live'])).versionstamp, null],
        )

        // The next event on the live key is a real change, not a cleanup echo.
        await db.set(['live'], 'v2')
        assert.strictEqual((await readWithin(liveOnly))?.[0]?.value, 'v2')
      } finally {
        await both.cancel()
        await liveOnly.cancel()
        await db.close()
      }
    })

    await test('cleanup keeps a key that another client sets again meanwhile', async (t) => {
      const name = namespace()
      const driver = await driverFor(name)(v8Serializer)
      const other = await driverFor(name)(v8Serializer)
      await driver.set(KEY_A, 'expiring', VERSIONSTAMP, Date.now() + 50)
      await setTimeout(100)
      const reader = driver.watch([KEY_A]).getReader()
      // Right before cleanup deletes, the other client sets the key again,
      // without an expiry.
      const deleteMany = Collection.prototype.deleteMany
      let interfered = false
      t.mock.method(
        Collection.prototype,
        'deleteMany',
        async function (this: Collection, ...args: unknown[]) {
          if (!interfered) {
            interfered = true
            await other.set(KEY_A, 'again', VERSIONSTAMP)
          }
          return Reflect.apply(deleteMany, this, args)
        },
      )
      try {
        assert.strictEqual((await readWithin(reader))?.[0]?.value, null)
        await driver.cleanup(Date.now())
        t.mock.restoreAll()
        assert.ok(interfered)
        assert.strictEqual(
          (await driver.get(KEY_A, Date.now()))?.value,
          'again',
        )
        // Cleanup deleted nothing, so the next event is the next write.
        await driver.set(KEY_A, 'final', VERSIONSTAMP)
        assert.strictEqual((await readWithin(reader))?.[0]?.value, 'final')
      } finally {
        await reader.cancel()
        await driver.close()
        await other.close()
      }
    })

    await test('a watcher that cannot read its values fails, but the write stands', async (t) => {
      const db = await Valkeyrie.open(driverFor(namespace()))
      const reader = db.watch([['a'], ['b']]).getReader()
      try {
        await readWithin(reader)
        // Publishing a change to 'a' reads 'b', which fails.
        t.mock.method(Collection.prototype, 'findOne', async () => {
          throw new Error('read failed')
        })
        await db.set(['a'], 1)
        t.mock.restoreAll()
        await assert.rejects(reader.read(), /read failed/)
        assert.strictEqual((await db.get(['a'])).value, 1)
      } finally {
        await db.close()
      }
    })

    await test('watchers always see the committed value under concurrent commits', async () => {
      const chain = async (index: number) => {
        const db = await Valkeyrie.open(driverFor(namespace()))
        const key: Key = ['stress']
        try {
          for (let i = 0; i < 25; i++) {
            await db.set(key, `c${index}-i${i}-initial`)
            const reader = db.watch([key]).getReader()
            try {
              await readWithin(reader)
              const committed = `c${index}-i${i}-committed`
              assert.ok((await db.atomic().set(key, committed).commit()).ok)
              assert.strictEqual(
                (await readWithin(reader))?.[0]?.value,
                committed,
              )
            } finally {
              await reader.cancel()
            }
          }
        } finally {
          await db.close()
        }
      }
      await Promise.all(Array.from({ length: 8 }, (_, index) => chain(index)))
    })

    await test('clear deletes the entries but keeps the sequence', async () => {
      const name = namespace()
      const driver = await driverFor(name)(v8Serializer)
      try {
        await driver.set(KEY_A, 1, await driver.generateVersionstamp())
        const before = await driver.generateVersionstamp()
        await driver.set(KEY_B, 2, before, Date.now() + 60_000)

        await driver.clear()
        assert.strictEqual(await entries.countDocuments({ namespace: name }), 0)
        assert.strictEqual(
          await sequences.countDocuments({ namespace: name }),
          1,
        )
        const after = await driver.generateVersionstamp()
        assert.ok(
          (BigInt(`0x${after}`) & 0xfffffn) >
            (BigInt(`0x${before}`) & 0xfffffn),
        )
      } finally {
        await driver.close()
      }
    })

    await test('destroy deletes every document of the namespace', async () => {
      const name = namespace()
      const db = await Valkeyrie.open(driverFor(name))
      await db.set(['a'], 1)
      await db.atomic().set(['b'], 2).commit()
      await db.destroy()
      await db.close()
      assert.strictEqual(await entries.countDocuments({ namespace: name }), 0)
      assert.strictEqual(await sequences.countDocuments({ namespace: name }), 0)
    })

    await test('rejects a namespace that is not a plain value', () => {
      const client = connect()
      for (const name of ['', Number.NaN, null, undefined, {}, { $ne: null }]) {
        assert.throws(
          () => mongodbDriver({ client, namespace: name as string }),
          { name: 'TypeError' },
          String(name),
        )
      }
      for (const name of ['app', 0, new ObjectId()]) {
        mongodbDriver({ client, namespace: name })
      }
    })

    await test('rejects a namespace field that is not a plain field name', () => {
      const client = connect()
      for (const field of ['', '$ns', 'a.b', '_id', 'keyHash', 'value']) {
        assert.throws(
          () =>
            mongodbDriver({ client, namespace: 'app', namespaceField: field }),
          { name: 'TypeError' },
          field,
        )
      }
    })

    await test('refuses a collection whose default collation is not simple', async () => {
      const collectionName = `valkeyrie-collation-${new ObjectId()}`
      const db = admin.db(testDbName)
      await db.createCollection(collectionName, {
        collation: { locale: 'en', strength: 2 },
      })
      try {
        await assert.rejects(
          mongodbDriver({
            client: connect(),
            namespace: 'app',
            dbName: testDbName,
            collectionName,
          })(),
          /needs the simple collation/,
        )
      } finally {
        await db.dropCollection(collectionName)
      }
    })
  },
)
