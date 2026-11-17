import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { DriverFactory } from '../../src/valkeyrie.ts'

/**
 * A storage driver that the shared test suite can run against.
 *
 * A module that default-exports one can be selected with the
 * `VALKEYRIE_TEST_DRIVER` environment variable (see {@link loadTestDriver}).
 */
export interface TestDriver {
  /** Name shown in skip messages. SQLite-only tests run only when it is `sqlite`. */
  readonly name: string
  /** Opens a new, empty database that is discarded when it is closed. */
  readonly open: DriverFactory
  /** Creates a new, empty database that outlives `close`, for tests that close and reopen it. */
  readonly createPersistentDatabase: () => PersistentDatabase
}

export interface PersistentDatabase {
  /** Opens the database. Every call opens the same database. */
  readonly open: DriverFactory
  /** Deletes the database and everything it stored. */
  readonly remove: () => Promise<void>
}

/**
 * Loads the test driver named by `selection`: a driver name from this folder
 * (`sqlite`, the default when `selection` is empty) or a path, relative to the
 * working directory, to a module that default-exports a {@link TestDriver}.
 */
export async function loadTestDriver(
  selection: string | undefined,
): Promise<TestDriver> {
  const name = selection || 'sqlite'
  const url = /^[\w-]+$/.test(name)
    ? new URL(`./${name}.ts`, import.meta.url)
    : pathToFileURL(resolve(name))

  let driver: Partial<TestDriver> | undefined
  try {
    driver = (await import(url.href)).default
  } catch (error) {
    throw new Error(`Cannot load test driver "${name}"`, { cause: error })
  }
  if (
    typeof driver?.name !== 'string' ||
    typeof driver.open !== 'function' ||
    typeof driver.createPersistentDatabase !== 'function'
  ) {
    throw new TypeError(
      `Test driver module "${name}" does not default-export a test driver`,
    )
  }
  return driver as TestDriver
}

/** The driver selected for this test run. */
export const testDriver = await loadTestDriver(
  process.env.VALKEYRIE_TEST_DRIVER,
)

/** `node:test` options that skip a test unless it runs on the SQLite driver. */
export const sqliteOnly =
  testDriver.name === 'sqlite'
    ? {}
    : { skip: `SQLite-only test, running on ${testDriver.name}` }
