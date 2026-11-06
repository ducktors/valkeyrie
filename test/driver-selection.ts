import assert from 'node:assert'
import { randomUUID } from 'node:crypto'
import { rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, test } from 'node:test'
import { loadTestDriver } from './drivers/test-driver.ts'

describe('test driver selection', async () => {
  async function withModule(
    source: string,
    fn: (path: string) => Promise<void>,
  ) {
    const path = join(tmpdir(), `${randomUUID()}.mjs`)
    await writeFile(path, source)
    try {
      await fn(path)
    } finally {
      await rm(path, { force: true })
    }
  }

  await test('defaults to the SQLite driver', async () => {
    assert.strictEqual((await loadTestDriver(undefined)).name, 'sqlite')
    assert.strictEqual((await loadTestDriver('')).name, 'sqlite')
  })

  await test('selecting sqlite by name gives the default driver', async () => {
    assert.strictEqual(
      await loadTestDriver('sqlite'),
      await loadTestDriver(undefined),
    )
  })

  await test('selects the driver exported by a module path', async () => {
    await withModule(
      `export default {
        name: 'other',
        open: async () => { throw new Error('not used') },
        createPersistentDatabase: () => { throw new Error('not used') },
      }`,
      async (path) => {
        const driver = await loadTestDriver(path)
        assert.strictEqual(driver.name, 'other')
      },
    )
  })

  await test('rejects an unknown driver name', async () => {
    await assert.rejects(() => loadTestDriver('no-such-driver'), {
      message: /Cannot load test driver "no-such-driver"/,
    })
  })

  await test('rejects a module without a test driver', async () => {
    await withModule('export default 42', async (path) => {
      await assert.rejects(() => loadTestDriver(path), {
        message: /does not default-export a test driver/,
      })
    })
  })
})
