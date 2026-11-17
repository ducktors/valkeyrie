import { randomUUID } from 'node:crypto'
import { rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { sqliteDriver } from '../../src/sqlite-driver.ts'
import type { TestDriver } from './test-driver.ts'

const sqlite: TestDriver = {
  name: 'sqlite',
  // In memory, as Valkeyrie.open() does without a path.
  open: (serializer) => sqliteDriver(undefined, serializer),
  createPersistentDatabase: () => {
    const path = join(tmpdir(), randomUUID())
    return {
      open: (serializer) => sqliteDriver(path, serializer),
      remove: async () => {
        await Promise.all(
          [path, `${path}-shm`, `${path}-wal`].map((file) =>
            rm(file, { force: true }),
          ),
        )
      },
    }
  },
}

export default sqlite
