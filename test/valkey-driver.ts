import assert from 'node:assert'
import { after, describe, test } from 'node:test'
import { Redis } from 'ioredis'
import { jsonSerializer } from '../src/serializers/json.ts'
import type { Serializer } from '../src/serializers/serializer.ts'
import { v8Serializer } from '../src/serializers/v8.ts'
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

    await test('methods of later tickets throw a clear error', async () => {
      const driver = await valkeyDriver({
        client: connect(),
        namespace: namespace(),
      })(v8Serializer)
      const notImplemented = /not implemented yet/
      const keyHash = Buffer.from([0x02, 0x6b, 0x00]).toString('hex')
      try {
        await assert.rejects(
          driver.list('', '￿', '', Date.now(), 10),
          notImplemented,
        )
        await assert.rejects(
          driver.withTransaction(async () => {}),
          notImplemented,
        )
        await assert.rejects(
          driver.set(keyHash, 'v', '0'.repeat(20), Date.now() + 1000),
          notImplemented,
        )
        assert.throws(() => driver.watch([keyHash]), notImplemented)
        await assert.rejects(driver.clear(), notImplemented)
        await assert.rejects(driver.destroy(), notImplemented)
      } finally {
        await driver.close()
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
