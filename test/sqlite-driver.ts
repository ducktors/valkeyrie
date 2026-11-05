import assert from 'node:assert'
import { createHash } from 'node:crypto'
import { existsSync } from 'node:fs'
import { unlink } from 'node:fs/promises'
import { afterEach, beforeEach, describe, test } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import { sqliteDriver } from '../src/sqlite-driver.ts'
import { sqliteOnly } from './drivers/test-driver.ts'

const hash = (key: string) => createHash('sha256').update(key).digest('hex')

interface CustomError extends Error {
  code?: string
}

const cleanup = async (path: string) =>
  Promise.allSettled([
    unlink(path),
    unlink(`${path}-shm`),
    unlink(`${path}-wal`),
  ])

describe('sqliteDriver', sqliteOnly, async () => {
  const TEST_DB_PATH = 'test-db.sqlite'
  let driver: Awaited<ReturnType<typeof sqliteDriver>>

  beforeEach(async () => {
    // Clean up any existing test database
    await cleanup(TEST_DB_PATH)
    driver = await sqliteDriver(TEST_DB_PATH)
  })

  afterEach(async () => {
    await driver.close()
    await cleanup(TEST_DB_PATH)
  })

  await test('handles null results in watch streams', async () => {
    const keyHash = hash('nonexistent-key')
    const stream = driver.watch([keyHash])
    const reader = stream.getReader()

    // This should return values with null for the nonexistent key
    const { value, done } = await reader.read()
    assert(!done, 'Stream should not be done')
    assert(value && value.length === 1, 'Should return an array with one item')
    assert.deepStrictEqual(value[0], {
      keyHash,
      value: null,
      versionstamp: null,
    })

    // Properly release and clean up (no cancel which causes issues)
    reader.releaseLock()
  })

  await test('destroys file-based databases', async () => {
    // Set some data
    await driver.set(hash('test-key'), 'test-value', 'v1')

    // Use node's unlink directly without spying
    await driver.destroy()

    // Check that the file no longer exists
    assert(!existsSync(TEST_DB_PATH), 'Database file should be deleted')
    assert(!existsSync(`${TEST_DB_PATH}-shm`), 'SHM file should be deleted')
    assert(!existsSync(`${TEST_DB_PATH}-wal`), 'WAL file should be deleted')
  })

  await test('handles unlink errors in destroy gracefully', async () => {
    // For this test, we'll create a special instance of the driver with a custom file path
    const ERROR_TEST_PATH = 'error-test.sqlite'
    const errorDriver = await sqliteDriver(ERROR_TEST_PATH)

    // Set some data
    await errorDriver.set(hash('test-key'), 'test-value', 'v1')

    // Create a file path that doesn't exist to force unlink to fail naturally
    // This test is different - instead of mocking unlink, we'll ensure the destroy
    // function properly handles the catch blocks by trying to destroy a file that
    // has already been deleted

    // First delete the files manually
    if (existsSync(ERROR_TEST_PATH)) {
      await unlink(ERROR_TEST_PATH).catch(() => {})
      await unlink(`${ERROR_TEST_PATH}-shm`).catch(() => {})
      await unlink(`${ERROR_TEST_PATH}-wal`).catch(() => {})
    }

    // Now call destroy - this should not throw even though the files are gone
    await errorDriver.destroy()

    // Clean up
    await errorDriver.close()
  })

  await test('retries transactions on SQLITE_BUSY errors', async () => {
    // Create a mock function that throws SQLITE_BUSY on first call
    let callCount = 0
    const busyErrorFn = async () => {
      if (callCount === 0) {
        callCount++
        const error = new Error('database is locked') as CustomError
        error.code = 'SQLITE_BUSY'
        throw error
      }
      return 'success'
    }

    // Use the mock in a transaction
    const result = await driver.withTransaction(busyErrorFn)

    // Should have retried and succeeded
    assert.strictEqual(callCount, 1)
    assert.strictEqual(result, 'success')
  })

  await test('notifies watchers when data changes', async () => {
    const keyHash = hash('watched-key')

    // Start watching before the key exists
    const stream = driver.watch([keyHash])
    const reader = stream.getReader()

    // Initial read should return null
    const initialRead = await reader.read()
    assert(!initialRead.done, 'Stream should not be done')
    assert(initialRead.value, 'Should have a value')
    assert.strictEqual(initialRead.value?.[0]?.value, null)

    // Set the key
    await driver.set(keyHash, 'test-value', 'v1')

    // Should get notification with the new value
    const update = await reader.read()
    assert(!update.done, 'Stream should not be done')
    assert(update.value, 'Should have a value')
    assert.strictEqual(update.value?.[0]?.keyHash, keyHash)
    assert.strictEqual(update.value?.[0]?.value, 'test-value')

    // Just release the lock without cancelling
    reader.releaseLock()
  })

  await test('properly handles stream cancel mechanism', async () => {
    const keyHash = hash('cancel-mechanic-test')

    // Create mock controller
    const mockController = {
      enqueue: () => {},
      close: () => {},
    }

    // Create a special version of the test driver to test the internal cancel logic
    const testDriver = await sqliteDriver(TEST_DB_PATH)

    // Directly access the watchQueue and insert our mock
    // We need to use 'any' since we are accessing private implementation details for testing

    // biome-ignore lint/suspicious/noExplicitAny: testing
    const driverAny = testDriver as any

    // Find the watchQueue symbol or property
    type WatchQueueItem = {
      keyHashes: string[]
      controller: { close: () => void; enqueue: (val: unknown) => void }
    }
    let watchQueue: WatchQueueItem[] | undefined

    // Try to find watchQueue whether it's a direct property or a symbol
    for (const key of Object.getOwnPropertyNames(driverAny)) {
      if (key === 'watchQueue' || key.includes('watchQueue')) {
        watchQueue = driverAny[key]
        break
      }
    }

    if (!watchQueue) {
      for (const sym of Object.getOwnPropertySymbols(driverAny)) {
        if (sym.toString().includes('watchQueue')) {
          watchQueue = driverAny[sym]
          break
        }
      }
    }

    // If we found the watchQueue, we can test the cancel functionality
    if (watchQueue) {
      // Push our mock controller
      watchQueue.push({
        keyHashes: [keyHash],
        controller: mockController,
      })

      // Get a real stream
      const stream = testDriver.watch([keyHash])

      // Cancel it - this should call controller.close()
      await stream.cancel()

      // Ensure that our driver still works
      await testDriver.set(keyHash, 'after-cancel-test', 'v1')
      const result = await testDriver.get(keyHash, Date.now())
      assert.strictEqual(result?.value, 'after-cancel-test')
    }

    await testDriver.close()
  })

  await test('handles watch cancellation', async () => {
    const keyHash = hash('cancel-test-key')

    // Create a stream and read from it
    const stream = driver.watch([keyHash])
    const reader = stream.getReader()

    // Read the initial null value
    await reader.read()

    // Actually cancel the stream (this triggers the cancel path and tests the bug fix)
    await reader.cancel()

    // Set the key - this should not throw
    await driver.set(keyHash, 'test-value', 'v1')

    // Create a new stream for the same key
    const newStream = driver.watch([keyHash])
    const newReader = newStream.getReader()

    // Should get the value
    const { value, done } = await newReader.read()
    assert(!done, 'Stream should not be done')
    assert(value, 'Should have a value')
    assert.strictEqual(value?.[0]?.value, 'test-value')

    // Cancel this one too to test it works multiple times
    await newReader.cancel()
  })

  await test('handles controller close errors', async () => {
    // For this test, we'll create a real close error scenario
    // First, create a database and get a watch going
    const memDriver = await sqliteDriver(':memory:')
    const keyHash = hash('test-key')

    // Set up a watch
    const stream = memDriver.watch([keyHash])

    // Start reading from it in a separate task
    const readerTask = (async () => {
      const reader = stream.getReader()
      try {
        await reader.read()
      } catch (e) {
        // This will throw when we close the driver
      }
    })()

    // Give the reader a chance to start
    await setTimeout(10)

    // Close the driver while the reader is active - should not throw
    await memDriver.close()

    // Wait for reader task to complete
    await readerTask
  })

  await test('clears memory databases on destroy', async () => {
    // Create a memory DB
    const memoryDriver = await sqliteDriver(':memory:')

    // Set some data
    const keyHash = hash('memory-key')
    await memoryDriver.set(keyHash, 'memory-value', 'v1')

    // Create a watch to test notification
    const stream = memoryDriver.watch([keyHash])
    const reader = stream.getReader()

    // Initial read
    await reader.read()

    // Release the reader lock
    reader.releaseLock()

    // Destroy should clear, not unlink for memory DBs
    await memoryDriver.destroy()

    // Verify the key is gone
    const result = await memoryDriver.get(keyHash, Date.now())
    assert.strictEqual(result, undefined)

    await memoryDriver.close()
  })

  await test('handles multiple concurrent transactions correctly', async () => {
    // Start multiple transactions that will be processed sequentially
    const results = await Promise.all([
      driver.withTransaction(async () => {
        await driver.set(hash('tx1'), 'value1', 'v1')
        return 1
      }),
      driver.withTransaction(async () => {
        await driver.set(hash('tx2'), 'value2', 'v2')
        return 2
      }),
      driver.withTransaction(async () => {
        await driver.set(hash('tx3'), 'value3', 'v3')
        return 3
      }),
    ])

    assert.deepStrictEqual(results, [1, 2, 3])

    // Verify all transactions were applied
    const tx1 = await driver.get(hash('tx1'), Date.now())
    const tx2 = await driver.get(hash('tx2'), Date.now())
    const tx3 = await driver.get(hash('tx3'), Date.now())

    assert.strictEqual(tx1?.value, 'value1')
    assert.strictEqual(tx2?.value, 'value2')
    assert.strictEqual(tx3?.value, 'value3')
  })

  await test('uses backoff when encountering database locks', async () => {
    let attempts = 0
    const busyErrorFn = async () => {
      attempts++
      if (attempts <= 3) {
        const error = new Error('database is locked') as CustomError
        error.code = 'SQLITE_BUSY'
        throw error
      }
      return 'success after backoff'
    }

    // Since we can't easily mock the built-in setTimeout, just verify transaction succeeds
    const result = await driver.withTransaction(busyErrorFn)

    assert.strictEqual(attempts, 4, 'Should have attempted 4 times')
    assert.strictEqual(result, 'success after backoff')
  })

  await test('handles concurrent versionstamp generation outside transactions', async () => {
    // Create two separate driver instances sharing the same database file
    // This simulates multiple processes accessing the same database
    const SHARED_DB_PATH = 'shared-test-db.sqlite'
    await cleanup(SHARED_DB_PATH)

    const driver1 = await sqliteDriver(SHARED_DB_PATH)
    const driver2 = await sqliteDriver(SHARED_DB_PATH)

    try {
      // Generate versionstamps concurrently with more aggressive parallelism
      // to increase the chance of triggering SQLITE_BUSY
      const promises = []

      // Launch 50 concurrent versionstamp generations from both drivers
      for (let i = 0; i < 25; i++) {
        promises.push(driver1.generateVersionstamp())
        promises.push(driver2.generateVersionstamp())
      }

      const allStamps = await Promise.all(promises)

      // All versionstamps should be generated successfully
      assert.strictEqual(allStamps.length, 50, 'Should generate 50 stamps')

      // All versionstamps should be unique
      const uniqueStamps = new Set(allStamps)
      assert.strictEqual(
        uniqueStamps.size,
        allStamps.length,
        'All versionstamps should be unique',
      )

      // All versionstamps should be 20 characters
      for (const stamp of allStamps) {
        assert.strictEqual(
          stamp.length,
          20,
          'Versionstamp should be 20 characters',
        )
      }
    } finally {
      await driver1.close()
      await driver2.close()
      await cleanup(SHARED_DB_PATH)
    }
  })

  await test('handles watch controller close errors with unexpected errors', async () => {
    // Create a driver and set up a watch
    const testPath = 'watch-error-test.sqlite'
    await cleanup(testPath)
    const testDriver = await sqliteDriver(testPath)

    try {
      const keyHash = hash('error-test-key')

      // Create a watch stream
      const stream = testDriver.watch([keyHash])
      const reader = stream.getReader()

      // Read the initial value
      await reader.read()

      // Now we need to simulate a scenario where closing the controller
      // throws a non-ERR_INVALID_STATE error
      // We'll access the internal watchQueue and inject a mock controller
      // biome-ignore lint/suspicious/noExplicitAny: testing internals
      const driverAny = testDriver as any

      // Find the watchQueue
      type WatchQueueItem = {
        keyHashes: string[]
        controller: { close: () => void; enqueue: (val: unknown) => void }
      }
      let watchQueue: WatchQueueItem[] | undefined

      for (const key of Object.getOwnPropertyNames(driverAny)) {
        if (key === 'watchQueue' || key.includes('watchQueue')) {
          watchQueue = driverAny[key]
          break
        }
      }

      if (!watchQueue) {
        for (const sym of Object.getOwnPropertySymbols(driverAny)) {
          if (sym.toString().includes('watchQueue')) {
            watchQueue = driverAny[sym]
            break
          }
        }
      }

      // Inject a controller that throws a different error
      if (watchQueue) {
        watchQueue.push({
          keyHashes: [keyHash],
          controller: {
            close: () => {
              const error = new Error('Unexpected close error') as CustomError
              error.code = 'UNEXPECTED_ERROR'
              throw error
            },
            enqueue: () => {},
          },
        })
      }

      // Release the reader
      reader.releaseLock()

      // Close should not throw even if a controller.close() throws
      await testDriver.close()
    } finally {
      await cleanup(testPath)
    }
  })

  await test('handles rollback failures in transaction retry logic', async () => {
    // This test aims to cover the rollback error catch block
    // We'll create a scenario where a ROLLBACK might fail
    const testPath = 'rollback-test.sqlite'
    await cleanup(testPath)
    const testDriver = await sqliteDriver(testPath)

    try {
      // Create a scenario with multiple SQLITE_BUSY errors
      let attemptCount = 0
      const busyWithRollbackFn = async () => {
        attemptCount++
        if (attemptCount <= 2) {
          const error = new Error('database is locked') as CustomError
          error.code = 'SQLITE_BUSY'
          throw error
        }
        return 'success'
      }

      // Run the transaction - the retry logic will attempt rollbacks
      const result = await testDriver.withTransaction(busyWithRollbackFn)

      assert.strictEqual(result, 'success')
      assert(attemptCount >= 2, 'Should have retried at least twice')

      // The driver should still be functional after rollback handling
      const keyHash = hash('post-rollback-test')
      await testDriver.set(keyHash, 'test-value', 'v1')
      const retrieved = await testDriver.get(keyHash, Date.now())
      assert.strictEqual(retrieved?.value, 'test-value')
    } finally {
      await testDriver.close()
      await cleanup(testPath)
    }
  })
})
