import { randomUUID } from 'node:crypto'
import { after } from 'node:test'
import { Redis } from 'ioredis'
import { valkeyDriver } from '../../src/valkey-driver.ts'
import type { TestDriver } from './test-driver.ts'

/** The Redis or Valkey server the tests use. */
export const redisUrl =
  process.env.VALKEYRIE_TEST_REDIS_URL ?? 'redis://localhost:6379'

let client: Redis | undefined
function getClient(): Redis {
  client ??= new Redis(redisUrl)
  return client
}
after(async () => {
  await client?.quit()
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
    const namespace = testNamespace()
    const driver = await valkeyDriver({ client: getClient(), namespace })(
      serializer,
    )
    return {
      ...driver,
      close: async () => {
        await driver.close()
        await removeNamespace(getClient(), namespace)
      },
    }
  },
  createPersistentDatabase: () => {
    const namespace = testNamespace()
    return {
      open: (serializer) =>
        valkeyDriver({ client: getClient(), namespace })(serializer),
      remove: () => removeNamespace(getClient(), namespace),
    }
  },
}

export default valkey
