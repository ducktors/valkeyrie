import { randomUUID } from 'node:crypto'
import { after } from 'node:test'
import { MongoClient } from 'mongodb'
import { mongodbDriver } from '../../src/mongodb-driver.ts'
import type { TestDriver } from './test-driver.ts'

/** The MongoDB replica set the tests use. */
export const mongodbUrl =
  process.env.VALKEYRIE_TEST_MONGODB_URL ??
  'mongodb://localhost:27017/?directConnection=true'

/** The database the tests use. */
export const testDbName = 'valkeyrie-test'

let client: Promise<MongoClient> | undefined
function getClient(): Promise<MongoClient> {
  client ??= MongoClient.connect(mongodbUrl)
  return client
}
after(async () => {
  const mongo = await client?.catch(() => undefined)
  await mongo?.close()
})

/** Returns a namespace no other test uses. */
export function testNamespace(): string {
  return `valkeyrie-test:${randomUUID()}`
}

/** Deletes every document of a namespace, whatever the driver left behind. */
export async function removeNamespace(
  mongo: MongoClient,
  namespace: unknown,
  namespaceField = 'namespace',
): Promise<void> {
  const db = mongo.db(testDbName)
  await db.collection('valkeyrie').deleteMany({ [namespaceField]: namespace })
  await db
    .collection('valkeyrie_sequences')
    .deleteMany({ [namespaceField]: namespace })
}

const mongodb: TestDriver = {
  name: 'mongodb',
  // Each database gets its own namespace, deleted when the database closes.
  open: async (serializer) => {
    const mongo = await getClient()
    const namespace = testNamespace()
    const driver = await mongodbDriver({
      client: mongo,
      namespace,
      dbName: testDbName,
    })(serializer)
    return {
      ...driver,
      close: async () => {
        await driver.close()
        await removeNamespace(mongo, namespace)
      },
    }
  },
  createPersistentDatabase: () => {
    const namespace = testNamespace()
    return {
      open: async (serializer) =>
        mongodbDriver({
          client: await getClient(),
          namespace,
          dbName: testDbName,
        })(serializer),
      remove: async () => removeNamespace(await getClient(), namespace),
    }
  },
}

export default mongodb
