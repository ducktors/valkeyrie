import { randomUUID } from 'node:crypto'
import { after } from 'node:test'
import { Redis } from 'ioredis'
import { valkeyDriver } from '../../src/valkey-driver.ts'
import type { TestDriver } from './test-driver.ts'

/** The Redis or Valkey server the tests use. */
export const redisUrl =
  process.env.VALKEYRIE_TEST_REDIS_URL ?? 'redis://localhost:6379'

// Strict client options: commands fail instead of waiting for a connection,
// and commands sent in the same tick share one round trip.
let client: Promise<Redis> | undefined
function getClient(): Promise<Redis> {
  client ??= (async () => {
    const redis = new Redis(redisUrl, {
      lazyConnect: true,
      enableAutoPipelining: true,
      enableOfflineQueue: false,
      maxRetriesPerRequest: 1,
    })
    await redis.connect()
    return redis
  })()
  return client
}
after(async () => {
  const redis = await client?.catch(() => undefined)
  await redis?.quit()
})

/** Returns a namespace no other test uses. */
export function testNamespace(): string {
  return `valkeyrie-test:${randomUUID()}`
}

/** Deletes every key of a namespace, whatever the driver left behind. */
export async function removeNamespace(
  redis: Redis,
  namespace: string,
): Promise<void> {
  let cursor = '0'
  do {
    const [next, keys] = await redis.scanBuffer(
      cursor,
      'MATCH',
      `{${namespace}}:*`,
      'COUNT',
      1000,
    )
    cursor = next.toString()
    if (keys.length > 0) {
      await redis.unlink(...keys)
    }
  } while (cursor !== '0')
}

const valkey: TestDriver = {
  name: 'valkey',
  // Each database gets its own namespace, deleted when the database closes.
  open: async (serializer) => {
    const redis = await getClient()
    const namespace = testNamespace()
    const driver = await valkeyDriver({ client: redis, namespace })(serializer)
    return {
      ...driver,
      close: async () => {
        await driver.close()
        await removeNamespace(redis, namespace)
      },
    }
  },
  createPersistentDatabase: () => {
    const namespace = testNamespace()
    return {
      open: async (serializer) =>
        valkeyDriver({ client: await getClient(), namespace })(serializer),
      remove: async () => removeNamespace(await getClient(), namespace),
    }
  },
}

export default valkey
