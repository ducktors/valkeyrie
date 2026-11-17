import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { test } from 'node:test'
import { Cluster, Redis } from 'ioredis'
import { KvU64 } from '../src/kv-u64.ts'
import { redisDriver } from '../src/redis-driver.ts'
import { Valkeyrie } from '../src/valkeyrie.ts'

const clusterUrl = process.env.VALKEYRIE_TEST_REDIS_CLUSTER_URL

test(
  'atomic reads use the primary when the caller reads a lagging replica',
  {
    skip:
      clusterUrl === undefined && 'set VALKEYRIE_TEST_REDIS_CLUSTER_URL to run',
    timeout: 15_000,
  },
  async (context) => {
    const url = new URL(clusterUrl as string)
    const client = new Cluster(
      [{ host: url.hostname, port: Number(url.port) }],
      {
        lazyConnect: true,
        scaleReads: 'slave',
      },
    )
    context.after(() => client.disconnect())
    await client.connect()
    const primaryNode = client.nodes('master')[0]
    const replicaNode = client.nodes('slave')[0]
    assert.ok(primaryNode)
    assert.ok(replicaNode)
    const primary = new Redis(primaryNode.options)
    const replica = new Redis(replicaNode.options)
    context.after(() => {
      primary.disconnect()
      replica.disconnect()
    })
    const namespace = `redis-cluster-test:${randomUUID()}`
    const writer = await Valkeyrie.open(
      redisDriver({ client: primary, namespace }),
    )
    const db = await Valkeyrie.open(redisDriver({ client, namespace }))
    try {
      await writer.set(['n'], new KvU64(1n))
      assert.strictEqual(await primary.wait(1, 5_000), 1)
      assert.deepStrictEqual((await db.get(['n'])).value, new KvU64(1n))
      await replica.client('PAUSE', 10_000, 'WRITE')
      await writer.set(['n'], new KvU64(10n))
      assert.deepStrictEqual((await db.get(['n'])).value, new KvU64(1n))
      assert.ok((await db.atomic().sum(['n'], 1n).commit()).ok)
      assert.deepStrictEqual((await writer.get(['n'])).value, new KvU64(11n))
      assert.strictEqual(client.options.scaleReads, 'slave')
      const current = await writer.get(['n'])
      assert.ok(
        (
          await db
            .atomic()
            .check({ key: ['n'], versionstamp: current.versionstamp })
            .commit()
        ).ok,
      )
    } finally {
      await replica.client('UNPAUSE')
      await db.close()
      await writer.destroy()
      await writer.close()
      client.disconnect()
      primary.disconnect()
      replica.disconnect()
    }
  },
)
