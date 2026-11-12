import assert from 'node:assert'
import { randomUUID } from 'node:crypto'
import { existsSync } from 'node:fs'
import { unlink } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, mock, test } from 'node:test'
import { Valkeyrie as LegacyValkeyrie } from 'valkeyrie-legacy'
import { KvU64 as LegacyKvU64 } from 'valkeyrie-legacy/KvU64'
import { jsonSerializer as legacyJsonSerializer } from 'valkeyrie-legacy/serializers/json'
import { z } from 'zod'
import { KEY_FORMAT_KEY_HASH } from '../src/key-format.ts'
import { KvU64 } from '../src/kv-u64.ts'
import { migrateKeyHash, migrateSqliteKeyFormat } from '../src/migrate.ts'
import { jsonSerializer } from '../src/serializers/json.ts'
import { sqliteDriver } from '../src/sqlite-driver.ts'
import { KeyFormatError, Valkeyrie } from '../src/valkeyrie.ts'

const tempPath = () => join(tmpdir(), `valkeyrie-key-format-${randomUUID()}.db`)

const removeStore = async (path: string) => {
  await Promise.allSettled([
    unlink(path),
    unlink(`${path}-shm`),
    unlink(`${path}-wal`),
  ])
}

/**
 * Runs a test with the paths of a source store and a target store, and
 * deletes both stores after the test.
 */
const withStores = async (
  fn: (source: string, target: string) => Promise<void>,
) => {
  const source = tempPath()
  const target = tempPath()
  try {
    await fn(source, target)
  } finally {
    await removeStore(source)
    await removeStore(target)
  }
}

describe('key format', async () => {
  await test('open() rejects a store that Valkeyrie 0.8 wrote, and does not change it', async () => {
    await withStores(async (source) => {
      const legacy = await LegacyValkeyrie.open(source)
      const { versionstamp } = await legacy.set(
        [new Uint8Array([0, 255])],
        'legacy value',
      )
      await legacy.close()

      await assert.rejects(
        Valkeyrie.open(source, { destroyOnClose: true }),
        new KeyFormatError(1, 2),
      )

      const reopened = await LegacyValkeyrie.open(source)
      try {
        assert.deepStrictEqual(
          await Array.fromAsync(reopened.list({ prefix: [] })),
          [
            {
              key: [new Uint8Array([0, 255])],
              value: 'legacy value',
              versionstamp,
            },
          ],
        )
      } finally {
        await reopened.close()
      }
    })
  })

  await test('from() and a builder open() reject a store that Valkeyrie 0.8 wrote', async () => {
    await withStores(async (source) => {
      const legacy = await LegacyValkeyrie.open(source)
      await legacy.set(['users', 'alice'], { name: 'Alice' })
      await legacy.close()

      await assert.rejects(
        Valkeyrie.from([{ id: 'bob' }], {
          path: source,
          prefix: ['users'],
          keyProperty: 'id',
        }),
        new KeyFormatError(1, 2),
      )
      await assert.rejects(
        Valkeyrie.withSchema(
          ['users', '*'],
          z.object({ name: z.string() }),
        ).open(source),
        new KeyFormatError(1, 2),
      )
    })
  })

  await test('open() rejects a store with a key format row of another version', async () => {
    await withStores(async (source) => {
      const driver = await sqliteDriver(source)
      await driver.set(
        KEY_FORMAT_KEY_HASH,
        { keyFormat: 3 },
        await driver.generateVersionstamp(),
      )
      await driver.close()

      await assert.rejects(Valkeyrie.open(source), new KeyFormatError(3, 2))
    })
  })

  await test('open() opens a store again after it wrote entries to it', async () => {
    await withStores(async (source) => {
      const db = await Valkeyrie.open(source)
      const { versionstamp } = await db.set(['a'], 'value')
      await db.close()

      const reopened = await Valkeyrie.open(source)
      try {
        assert.deepStrictEqual(await reopened.get(['a']), {
          key: ['a'],
          value: 'value',
          versionstamp,
        })
      } finally {
        await reopened.close()
      }
    })
  })

  await test('open() opens a store again after clear() and new entries', async () => {
    await withStores(async (source) => {
      const db = await Valkeyrie.open(source)
      await db.set(['a'], 'cleared')
      await db.clear()
      const { versionstamp } = await db.set(['b'], 'value')
      await db.close()

      const reopened = await Valkeyrie.open(source)
      try {
        assert.deepStrictEqual(
          await Array.fromAsync(reopened.list({ prefix: [] })),
          [{ key: ['b'], value: 'value', versionstamp }],
        )
      } finally {
        await reopened.close()
      }
    })
  })

  await test('list() over all keys does not return the key format row', async () => {
    const db = await Valkeyrie.open()
    try {
      const first = await db.set([new Uint8Array([0])], 'first')
      const last = await db.set([true], 'last')
      const firstEntry = {
        key: [new Uint8Array([0])],
        value: 'first',
        versionstamp: first.versionstamp,
      }
      const lastEntry = {
        key: [true],
        value: 'last',
        versionstamp: last.versionstamp,
      }

      assert.deepStrictEqual(await Array.fromAsync(db.list({ prefix: [] })), [
        firstEntry,
        lastEntry,
      ])
      assert.deepStrictEqual(
        await Array.fromAsync(db.list({ prefix: [] }, { reverse: true })),
        [lastEntry, firstEntry],
      )

      const page = db.list({ prefix: [] }, { limit: 1 })
      assert.deepStrictEqual(await Array.fromAsync(page), [firstEntry])
      assert.deepStrictEqual(
        await Array.fromAsync(db.list({ prefix: [] }, { cursor: page.cursor })),
        [lastEntry],
      )
    } finally {
      await db.close()
    }
  })

  await test('migrateKeyHash() should return the hash of the same key in the current key format', () => {
    assert.deepStrictEqual(
      ['0100ff00', '010000', '026100050100'].map(migrateKeyHash),
      ['0100ffff00', '0100ff00', '026100050100'],
    )
  })

  await test('migrateSqliteKeyFormat() keeps a legacy entry apart from a new key with the same legacy bytes', async () => {
    await withStores(async (source, target) => {
      const legacyKey = [new Uint8Array([0, 255])]
      const newKey = [new Uint8Array([0])]
      const legacy = await LegacyValkeyrie.open(source)
      const { versionstamp } = await legacy.set(legacyKey, 'legacy value')
      await legacy.close()

      assert.deepStrictEqual(await migrateSqliteKeyFormat(source, target), {
        migrated: 1,
        rekeyed: 1,
        skipped: [],
      })

      const legacyEntry = {
        key: legacyKey,
        value: 'legacy value',
        versionstamp,
      }
      const db = await Valkeyrie.open(target)
      try {
        assert.deepStrictEqual(await db.get(newKey), {
          key: newKey,
          value: null,
          versionstamp: null,
        })
        assert.deepStrictEqual(await db.get(legacyKey), legacyEntry)

        const written = await db.set(newKey, 'new value')
        assert.deepStrictEqual(await db.getMany([newKey, legacyKey]), [
          {
            key: newKey,
            value: 'new value',
            versionstamp: written.versionstamp,
          },
          legacyEntry,
        ])

        await db.delete(newKey)
        assert.deepStrictEqual(await Array.fromAsync(db.list({ prefix: [] })), [
          legacyEntry,
        ])
      } finally {
        await db.close()
      }
    })
  })

  await test('migrateSqliteKeyFormat() keeps two legacy keys apart when the new hash of one is the legacy hash of the other', async () => {
    await withStores(async (source, target) => {
      const legacy = await LegacyValkeyrie.open(source)
      const zero = await legacy.set([new Uint8Array([0])], 'zero')
      const zeroFF = await legacy.set([new Uint8Array([0, 255])], 'zero ff')
      await legacy.close()

      assert.deepStrictEqual(await migrateSqliteKeyFormat(source, target), {
        migrated: 2,
        rekeyed: 2,
        skipped: [],
      })

      const db = await Valkeyrie.open(target)
      try {
        assert.deepStrictEqual(await Array.fromAsync(db.list({ prefix: [] })), [
          {
            key: [new Uint8Array([0])],
            value: 'zero',
            versionstamp: zero.versionstamp,
          },
          {
            key: [new Uint8Array([0, 255])],
            value: 'zero ff',
            versionstamp: zeroFF.versionstamp,
          },
        ])
      } finally {
        await db.close()
      }
    })
  })

  await test('migrateSqliteKeyFormat() keeps key types, value types, versionstamps and expiry times', async () => {
    mock.timers.enable({ apis: ['Date'], now: 1_000_000 })
    try {
      await withStores(async (source, target) => {
        const legacy = await LegacyValkeyrie.open(source)
        const bigint = await legacy.set(['types', 10n], new Map([['a', 1]]))
        const boolean = await legacy.set(['types', true], 10n)
        const bytes = await legacy.set(
          ['types', new Uint8Array([1, 2])],
          new Uint8Array([3, 4]),
        )
        const number = await legacy.set(
          ['types', 1.5],
          new Date('2026-01-01T00:00:00.000Z'),
        )
        const string = await legacy.set(['types', 'text'], {
          nested: [1, 'two'],
        })
        const u64 = await legacy.set(['u64'], new LegacyKvU64(5n))
        await legacy.set(['expiring'], 'soon gone', { expireIn: 1000 })
        await legacy.close()

        assert.deepStrictEqual(await migrateSqliteKeyFormat(source, target), {
          migrated: 7,
          rekeyed: 0,
          skipped: [],
        })

        mock.timers.tick(999)
        const db = await Valkeyrie.open(target)
        try {
          assert.deepStrictEqual(
            (await db.get(['expiring'])).value,
            'soon gone',
          )
          mock.timers.tick(1)
          assert.deepStrictEqual(
            await Array.fromAsync(db.list({ prefix: [] })),
            [
              {
                key: ['types', new Uint8Array([1, 2])],
                value: new Uint8Array([3, 4]),
                versionstamp: bytes.versionstamp,
              },
              {
                key: ['types', 'text'],
                value: { nested: [1, 'two'] },
                versionstamp: string.versionstamp,
              },
              {
                key: ['types', 10n],
                value: new Map([['a', 1]]),
                versionstamp: bigint.versionstamp,
              },
              {
                key: ['types', 1.5],
                value: new Date('2026-01-01T00:00:00.000Z'),
                versionstamp: number.versionstamp,
              },
              {
                key: ['types', true],
                value: 10n,
                versionstamp: boolean.versionstamp,
              },
              {
                key: ['u64'],
                value: new KvU64(5n),
                versionstamp: u64.versionstamp,
              },
            ],
          )
        } finally {
          await db.close()
        }
      })
    } finally {
      mock.timers.reset()
    }
  })

  await test('migrateSqliteKeyFormat() writes the key format row with the serializer of the store', async () => {
    await withStores(async (source, target) => {
      const legacy = await LegacyValkeyrie.open(source, {
        serializer: legacyJsonSerializer,
      })
      const { versionstamp } = await legacy.set(['a'], { b: 1 })
      await legacy.close()

      await migrateSqliteKeyFormat(source, target, {
        serializer: jsonSerializer,
      })

      const db = await Valkeyrie.open(target, { serializer: jsonSerializer })
      try {
        assert.deepStrictEqual(await db.get(['a']), {
          key: ['a'],
          value: { b: 1 },
          versionstamp,
        })
      } finally {
        await db.close()
      }
    })
  })

  await test('migrateSqliteKeyFormat() should throw when Valkeyrie 0.8 cannot decode a row, and creates no target', async () => {
    await withStores(async (source, target) => {
      const legacy = await LegacyValkeyrie.open(source)
      await legacy.set(['a\0b'], 'unreadable')
      await legacy.set(['ok'], 'value')
      await legacy.close()

      await assert.rejects(
        migrateSqliteKeyFormat(source, target),
        new Error(
          'The migration cannot copy these rows: 0261006200 (Invalid key hash: unknown type marker 0x62). Set skipUnmigratable to copy the other rows.',
        ),
      )
      assert.strictEqual(existsSync(target), false)
    })
  })

  await test('migrateSqliteKeyFormat() should throw when it cannot create the target store, and leaves no target files', async () => {
    await withStores(async (source, target) => {
      const legacy = await LegacyValkeyrie.open(source)
      await legacy.set(['a'], 'value')
      await legacy.close()

      await assert.rejects(
        migrateSqliteKeyFormat(source, target, {
          serializer: () => {
            throw new Error('serializer failed')
          },
        }),
        new Error('serializer failed'),
      )
      assert.deepStrictEqual(
        [target, `${target}-shm`, `${target}-wal`].filter((path) =>
          existsSync(path),
        ),
        [],
      )
      assert.deepStrictEqual(await migrateSqliteKeyFormat(source, target), {
        migrated: 1,
        rekeyed: 0,
        skipped: [],
      })
    })
  })

  await test('migrateSqliteKeyFormat() with skipUnmigratable copies the other rows and reports the skipped rows', async () => {
    await withStores(async (source, target) => {
      const legacy = await LegacyValkeyrie.open(source)
      await legacy.set(['a\0b'], 'unreadable')
      const { versionstamp } = await legacy.set(['ok'], 'value')
      await legacy.close()

      assert.deepStrictEqual(
        await migrateSqliteKeyFormat(source, target, {
          skipUnmigratable: true,
        }),
        {
          migrated: 1,
          rekeyed: 0,
          skipped: [
            {
              keyHash: '0261006200',
              reason: 'Invalid key hash: unknown type marker 0x62',
            },
          ],
        },
      )

      const db = await Valkeyrie.open(target)
      try {
        assert.deepStrictEqual(await Array.fromAsync(db.list({ prefix: [] })), [
          { key: ['ok'], value: 'value', versionstamp },
        ])
      } finally {
        await db.close()
      }
    })
  })

  await test('migrateSqliteKeyFormat() should throw when the target exists', async () => {
    await withStores(async (source, target) => {
      const legacy = await LegacyValkeyrie.open(source)
      await legacy.set(['a'], 'value')
      await legacy.close()
      const existing = await Valkeyrie.open(target)
      await existing.close()

      await assert.rejects(
        migrateSqliteKeyFormat(source, target),
        new Error(`The target ${target} exists. Give the path of a new file.`),
      )
    })
  })

  await test('migrateSqliteKeyFormat() should throw when the source has a key format row', async () => {
    await withStores(async (source, target) => {
      const current = await Valkeyrie.open(source)
      await current.set(['a'], 'value')
      await current.close()

      await assert.rejects(
        migrateSqliteKeyFormat(source, target),
        new Error(
          `The source ${source} is not in key format 1: it has a key format row.`,
        ),
      )
      assert.strictEqual(existsSync(target), false)
    })
  })
})
