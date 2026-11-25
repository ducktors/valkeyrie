import { AsyncLocalStorage } from 'node:async_hooks'
import { setTimeout } from 'node:timers/promises'
import type { ClientSession, Db, Document, MongoClient } from 'mongodb'
import type { Driver, DriverValue } from './driver.ts'
import { KEY_HASH_END } from './key-codec.ts'
import { v8Serializer } from './serializers/v8.ts'
import type { DriverFactory } from './valkeyrie.ts'

/**
 * An instance of a BSON class, such as an `ObjectId`, a `UUID` or a `Long`.
 */
export interface BsonValue {
  readonly _bsontype: string
}

/** A value that names a database: a string, a number or a BSON value. */
export type MongodbNamespace = string | number | BsonValue

/** Options for {@link mongodbDriver}. */
export interface MongodbDriverOptions {
  /**
   * A connected MongoDB client. The caller owns it: the driver never closes
   * it. The server must be a replica set or a sharded cluster, because atomic
   * commits use transactions.
   */
  client: MongoClient
  /**
   * Names the database. Every document carries it, so one namespace is one
   * Valkeyrie database, and several namespaces can share the collections.
   * The driver stores it as given: an `ObjectId` stays an `ObjectId`.
   */
  namespace: MongodbNamespace
  /** The field that holds the namespace. Default: `namespace`. */
  namespaceField?: string
  /** The database. Default: the database of the client's connection string. */
  dbName?: string
  /** The collection of the entries. Default: `valkeyrie`. */
  collectionName?: string
  /** The collection of the versionstamp counters. Default: `valkeyrie_sequences`. */
  sequenceCollectionName?: string
}

type WatchEvent = (
  | DriverValue
  | { keyHash: string; value: null; versionstamp: null }
)[]

interface Watcher {
  keyHashes: string[]
  controller: ReadableStreamDefaultController<WatchEvent>
}

// A value written by this driver, or null for a deletion. Watchers receive it
// as written: reading it back can race the visibility of the commit.
type Change = { value: unknown; versionstamp: string } | null

// A commit in progress: its session, and the changes to publish after it
// commits.
interface Transaction {
  session: ClientSession
  changes: Map<string, Change>
}

// The binary value of a stored entry. bson 4 and later all have these fields.
interface StoredBinary {
  buffer: Uint8Array
  position: number
}

interface EntryDocument {
  keyHash: string
  value: StoredBinary
  versionstamp: string
  expiresAt?: number | null
}

// Every commit writes this row first. Two commits in one namespace then write
// the same document, so MongoDB lets only one of them commit, and a value
// that a commit read cannot change before it commits. The hash starts with
// KEY_HASH_END, so no key has it and no list returns it.
const COMMIT_LOCK_KEY_HASH = `${KEY_HASH_END}${Buffer.from('commit-lock').toString('hex')}`

// How often a commit runs again after it conflicted with another commit.
const COMMIT_ATTEMPTS = 10

// How often the driver retries a commit whose result is unknown.
const COMMIT_RESULT_ATTEMPTS = 5

// The most expired entries one cleanup step deletes.
const PAGE_SIZE = 1000

// The fields of the documents, which the namespace field must not replace.
const RESERVED_FIELDS = new Set([
  '_id',
  'keyHash',
  'value',
  'versionstamp',
  'expiresAt',
  'sequence',
  'commits',
])

// Checks the error label with the error's own method, so that the errors of
// any version of the MongoDB driver work.
function hasErrorLabel(error: unknown, label: string): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'hasErrorLabel' in error &&
    typeof error.hasErrorLabel === 'function' &&
    error.hasErrorLabel(label) === true
  )
}

// Runs tasks one at a time, in call order.
function createLock(): <T>(task: () => Promise<T>) => Promise<T> {
  let tail: Promise<unknown> = Promise.resolve()
  return (task) => {
    const result = tail.then(task)
    tail = result.catch(() => {})
    return result
  }
}

// A namespace goes into every query as a value. A plain object would be read
// as a query, such as `{ $ne: null }`, and match other namespaces.
function validateNamespace(namespace: unknown): void {
  if (typeof namespace === 'string') {
    if (namespace === '') {
      throw new TypeError('mongodbDriver: the namespace must not be empty')
    }
    return
  }
  if (typeof namespace === 'number') {
    if (!Number.isFinite(namespace)) {
      throw new TypeError('mongodbDriver: the namespace must be finite')
    }
    return
  }
  if (
    typeof namespace === 'object' &&
    namespace !== null &&
    typeof (namespace as Partial<BsonValue>)._bsontype === 'string'
  ) {
    return
  }
  throw new TypeError(
    'mongodbDriver: the namespace must be a string, a number or a BSON value such as an ObjectId',
  )
}

function validateNamespaceField(field: string): void {
  if (
    field === '' ||
    field.startsWith('$') ||
    field.includes('.') ||
    field.includes('\0') ||
    RESERVED_FIELDS.has(field)
  ) {
    throw new TypeError(`mongodbDriver: invalid namespace field "${field}"`)
  }
}

// The collection of the entries must compare strings by their bytes, as
// SQLite does: list bounds and key order depend on it.
async function checkCollation(db: Db, collectionName: string): Promise<void> {
  const [info] = await db.listCollections({ name: collectionName }).toArray()
  const locale = (info as { options?: { collation?: { locale?: string } } })
    ?.options?.collation?.locale
  if (locale !== undefined && locale !== 'simple') {
    throw new Error(
      `mongodbDriver: the collection "${collectionName}" has the default collation "${locale}"; it needs the simple collation`,
    )
  }
}

/**
 * Creates a driver that stores a Valkeyrie database in MongoDB, so several
 * processes can share it. The server must be a replica set or a sharded
 * cluster.
 *
 * @example
 * ```typescript
 * import { MongoClient } from 'mongodb'
 * import { Valkeyrie } from 'valkeyrie'
 * import { mongodbDriver } from 'valkeyrie/drivers/mongodb'
 *
 * const client = await MongoClient.connect('mongodb://localhost:27017/app')
 * const db = await Valkeyrie.open(mongodbDriver({ client, namespace: 'my-app' }))
 * ```
 */
export function mongodbDriver({
  client,
  namespace,
  namespaceField = 'namespace',
  dbName,
  collectionName = 'valkeyrie',
  sequenceCollectionName = 'valkeyrie_sequences',
}: MongodbDriverOptions): DriverFactory {
  validateNamespace(namespace)
  validateNamespaceField(namespaceField)
  const scope = { [namespaceField]: namespace }
  const live = (now: number) => ({
    $or: [{ expiresAt: null }, { expiresAt: { $gt: now } }],
  })

  return async (serializerInit) => {
    const serializer = (serializerInit ?? v8Serializer)()
    const db = client.db(dbName)
    const entries = db.collection(collectionName)
    const sequences = db.collection(sequenceCollectionName)

    await checkCollation(db, collectionName)
    await entries.createIndex(
      { [namespaceField]: 1, keyHash: 1 },
      { unique: true },
    )
    await entries.createIndex({ [namespaceField]: 1, expiresAt: 1 })
    await sequences.createIndex({ [namespaceField]: 1 }, { unique: true })

    const transactions = new AsyncLocalStorage<Transaction>()
    const lock = createLock()
    // The session of the running commit, as operation options.
    const inTransaction = (): { session?: ClientSession } => {
      const transaction = transactions.getStore()
      return transaction === undefined ? {} : { session: transaction.session }
    }

    const decode = (doc: Document): DriverValue => {
      const entry = doc as EntryDocument
      return {
        keyHash: entry.keyHash,
        value: serializer.deserialize(
          entry.value.buffer.subarray(0, entry.value.position),
        ),
        versionstamp: entry.versionstamp,
      }
    }

    // Watchers see writes made through this driver instance only.
    const watchers = new Set<Watcher>()

    // Sends a watcher its values. A key in `known` gets the value written to
    // it; the other keys are read. A watcher whose values cannot be read gets
    // the error and stops, but the write that changed them stands.
    const emit = async (watcher: Watcher, known: Map<string, Change>) => {
      try {
        const now = Date.now()
        const event: WatchEvent = await Promise.all(
          watcher.keyHashes.map(async (keyHash) => {
            let change: Change | undefined = known.get(keyHash)
            if (change === undefined && !known.has(keyHash)) {
              const doc = await entries.findOne({
                ...scope,
                keyHash,
                ...live(now),
              })
              change = doc === null ? null : decode(doc)
            }
            return change === null
              ? { keyHash, value: null, versionstamp: null }
              : { keyHash, ...change }
          }),
        )
        if (watchers.has(watcher)) {
          watcher.controller.enqueue(event)
        }
      } catch (error) {
        if (watchers.delete(watcher)) {
          watcher.controller.error(error)
        }
      }
    }

    // Sends the new values to the watchers of the changed keys.
    const notify = async (changes: Map<string, Change>) => {
      await Promise.all(
        [...watchers]
          .filter((watcher) =>
            watcher.keyHashes.some((keyHash) => changes.has(keyHash)),
          )
          .map((watcher) => emit(watcher, changes)),
      )
    }

    // Inside a commit, changes wait until it commits.
    const publish = async (changes: Map<string, Change>) => {
      const transaction = transactions.getStore()
      if (transaction === undefined) {
        await notify(changes)
        return
      }
      for (const [keyHash, change] of changes) {
        transaction.changes.set(keyHash, change)
      }
    }

    // A deletion of every watched key, for `clear` and `destroy`.
    const everyWatchedKey = () => {
      const changes = new Map<string, Change>()
      for (const watcher of watchers) {
        for (const keyHash of watcher.keyHashes) {
          changes.set(keyHash, null)
        }
      }
      return changes
    }

    // Runs `callback` in a MongoDB transaction. The MongoDB driver's own
    // withTransaction would run the operations of a callback that runs them
    // concurrently (with Promise.all) on one session at a time, so the driver
    // uses startTransaction and retries by itself.
    const runTransaction = async <T>(
      callback: () => Promise<T>,
    ): Promise<{ result: T; changes: Map<string, Change> }> => {
      const mongoSession = client.startSession()
      try {
        for (let attempt = 1; ; attempt++) {
          const transaction: Transaction = {
            session: mongoSession,
            changes: new Map(),
          }
          mongoSession.startTransaction()
          try {
            await entries.updateOne(
              { ...scope, keyHash: COMMIT_LOCK_KEY_HASH },
              { $inc: { commits: 1 } },
              { upsert: true, session: mongoSession },
            )
            const result = await transactions.run(transaction, callback)
            for (let commit = 1; ; commit++) {
              try {
                await mongoSession.commitTransaction()
                break
              } catch (error) {
                if (
                  commit === COMMIT_RESULT_ATTEMPTS ||
                  !hasErrorLabel(error, 'UnknownTransactionCommitResult')
                ) {
                  throw error
                }
              }
            }
            return { result, changes: transaction.changes }
          } catch (error) {
            if (mongoSession.inTransaction()) {
              await mongoSession.abortTransaction().catch(() => {})
            }
            if (
              attempt === COMMIT_ATTEMPTS ||
              !hasErrorLabel(error, 'TransientTransactionError')
            ) {
              throw error
            }
          }
          // Another commit won. Back off 5–20 ms, then run on fresh data.
          await setTimeout(5 + Math.random() * 15)
        }
      } finally {
        await mongoSession.endSession()
      }
    }

    const driver: Driver = {
      get: async (keyHash, now) => {
        const doc = await entries.findOne(
          { ...scope, keyHash, ...live(now) },
          inTransaction(),
        )
        return doc === null ? undefined : decode(doc)
      },
      set: async (keyHash, value, versionstamp, expiresAt) => {
        const serialized = serializer.serialize(value)
        await entries.updateOne(
          { ...scope, keyHash },
          expiresAt
            ? { $set: { value: serialized, versionstamp, expiresAt } }
            : {
                $set: { value: serialized, versionstamp },
                $unset: { expiresAt: '' },
              },
          { upsert: true, ...inTransaction() },
        )
        // Watchers get the value as a later get() returns it.
        await publish(
          new Map([
            [
              keyHash,
              { value: serializer.deserialize(serialized), versionstamp },
            ],
          ]),
        )
      },
      delete: async (keyHash) => {
        await entries.deleteOne({ ...scope, keyHash }, inTransaction())
        await publish(new Map([[keyHash, null]]))
      },
      generateVersionstamp: async () => {
        // includeResultMetadata returns the same result shape in every
        // version of the MongoDB driver.
        const result = await sequences.findOneAndUpdate(
          scope,
          { $inc: { sequence: 1 } },
          {
            upsert: true,
            returnDocument: 'after',
            includeResultMetadata: true,
            ...inTransaction(),
          },
        )
        const sequence = (result.value as { sequence?: number } | null)
          ?.sequence
        if (typeof sequence !== 'number') {
          throw new Error('mongodbDriver: the versionstamp sequence is missing')
        }
        const timestamp = BigInt(Date.now() * 1000) // microseconds
        // Same format as the SQLite driver: the timestamp shifted left by 20
        // bits, OR the low 20 bits of the sequence, as 20 hex characters.
        return ((timestamp << 20n) | (BigInt(sequence) & 0xfffffn))
          .toString(16)
          .padStart(20, '0')
      },
      // Commits of one driver instance run one at a time. Commits of other
      // instances conflict on the commit lock row, and the one that loses
      // runs again on fresh data, so a check that no longer holds returns
      // `{ ok: false }`. After COMMIT_ATTEMPTS the driver throws, and
      // Valkeyrie reports the commit as `{ ok: false }`.
      withTransaction: async <T>(callback: () => Promise<T>): Promise<T> => {
        if (transactions.getStore() !== undefined) {
          return await callback()
        }
        const { result, changes } = await lock(() => runTransaction(callback))
        await notify(changes)
        return result
      },
      list: async (
        startHash,
        endHash,
        prefixHash,
        now,
        limit,
        reverse = false,
      ) => {
        const docs = await entries
          .find(
            {
              ...scope,
              keyHash: { $gte: startHash, $lt: endHash, $ne: prefixHash },
              ...live(now),
            },
            inTransaction(),
          )
          .sort({ keyHash: reverse ? -1 : 1 })
          .limit(limit)
          .toArray()
        return docs.map(decode)
      },
      // Deletes the expired entries a page at a time, and tells the watchers
      // which keys it deleted. The delete checks the expiry again, so it
      // keeps a key that was set again in the meantime.
      cleanup: async (now) => {
        const expired = { ...scope, expiresAt: { $lte: now } }
        while (true) {
          const candidates = (
            await entries
              .find(expired, { projection: { keyHash: 1 } })
              .limit(PAGE_SIZE)
              .toArray()
          ).map((doc) => doc.keyHash as string)
          if (candidates.length === 0) {
            return
          }
          const { deletedCount } = await entries.deleteMany({
            ...expired,
            keyHash: { $in: candidates },
          })
          let deleted = candidates
          if (deletedCount !== candidates.length) {
            const kept = new Set(
              (
                await entries
                  .find(
                    { ...scope, keyHash: { $in: candidates } },
                    { projection: { keyHash: 1 } },
                  )
                  .toArray()
              ).map((doc) => doc.keyHash as string),
            )
            deleted = candidates.filter((keyHash) => !kept.has(keyHash))
          }
          await publish(new Map(deleted.map((keyHash) => [keyHash, null])))
          if (candidates.length < PAGE_SIZE) {
            return
          }
        }
      },
      // Emits the current values, then the new values each time a write
      // through this driver instance changes a watched key.
      watch: (keyHashes) => {
        let watcher: Watcher | undefined
        return new ReadableStream<WatchEvent>({
          start: (controller) => {
            watcher = { keyHashes, controller }
            watchers.add(watcher)
            void emit(watcher, new Map())
          },
          cancel: () => {
            if (watcher) {
              watchers.delete(watcher)
            }
          },
        })
      },
      // Keeps the versionstamp sequence, like the SQLite driver.
      clear: async () => {
        await entries.deleteMany(scope, inTransaction())
        await publish(everyWatchedKey())
      },
      destroy: async () => {
        await entries.deleteMany(scope, inTransaction())
        await sequences.deleteMany(scope, inTransaction())
        await publish(everyWatchedKey())
      },
      // Ends the watches and waits for a running commit. The caller's client
      // stays open.
      close: async () => {
        for (const watcher of watchers) {
          try {
            watcher.controller.close()
          } catch {
            // Already closed or cancelled.
          }
        }
        watchers.clear()
        await lock(async () => {})
      },
    }
    return driver
  }
}
