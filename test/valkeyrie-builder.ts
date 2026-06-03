import assert from 'node:assert'
import { randomUUID } from 'node:crypto'
import { access } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, test } from 'node:test'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import type { Driver } from '../src/driver.ts'
import type { Serializer } from '../src/serializers/serializer.ts'
import { sqliteDriver } from '../src/sqlite-driver.ts'
import { ValkeyrieBuilder } from '../src/valkeyrie-builder.ts'
import { Valkeyrie } from '../src/valkeyrie.ts'

// Mock schema for testing
const createMockSchema = (name: string): StandardSchemaV1 =>
  ({
    '~standard': {
      version: 1,
      vendor: 'test',
      validate: (value: unknown) => ({
        value,
        issues: undefined,
      }),
    },
    _name: name,
  }) as unknown as StandardSchemaV1

// Spy wrapping the built-in in-memory driver, used to verify a custom
// driverFn is actually invoked through the builder.
type DriverSpy = { created: number }

const createSpyDriverFn =
  (spy: DriverSpy) =>
  async (serializer?: () => Serializer): Promise<Driver> => {
    spy.created += 1
    return sqliteDriver(':memory:', serializer)
  }

describe('ValkeyrieBuilder', () => {
  describe('Builder Construction', () => {
    test('creates a new builder instance', () => {
      const builder = new ValkeyrieBuilder()
      assert.ok(builder instanceof ValkeyrieBuilder)
    })

    test('multiple builders are independent', () => {
      const builder1 = new ValkeyrieBuilder()
      const builder2 = new ValkeyrieBuilder()

      const schema1 = createMockSchema('schema1')
      const schema2 = createMockSchema('schema2')

      // Register different schemas to each builder
      builder1.withSchema(['users', '*'], schema1)
      builder2.withSchema(['posts', '*'], schema2)

      // If builders shared state, this would cause issues
      // The fact this doesn't throw proves independence
      assert.notStrictEqual(builder1, builder2)
    })
  })

  describe('Fluent Interface', () => {
    test('withSchema returns the builder instance for chaining', () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('user')

      const result = builder.withSchema(['users', '*'], schema)

      assert.strictEqual(result, builder)
    })

    test('can chain multiple withSchema calls', () => {
      const builder = new ValkeyrieBuilder()
      const userSchema = createMockSchema('user')
      const postSchema = createMockSchema('post')
      const commentSchema = createMockSchema('comment')

      const result = builder
        .withSchema(['users', '*'], userSchema)
        .withSchema(['posts', '*'], postSchema)
        .withSchema(['comments', '*'], commentSchema)

      assert.strictEqual(result, builder)
    })

    test('fluent pattern works end-to-end', async () => {
      const userSchema = createMockSchema('user')
      const postSchema = createMockSchema('post')

      const db = await new ValkeyrieBuilder()
        .withSchema(['users', '*'], userSchema)
        .withSchema(['posts', '*'], postSchema)
        .open()

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })
  })

  describe('open() Method', () => {
    test('returns a Valkeyrie instance', async () => {
      const builder = new ValkeyrieBuilder()

      const db = await builder.open()

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('works without any schemas registered', async () => {
      const builder = new ValkeyrieBuilder()

      const db = await builder.open()

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('works with schemas registered', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('user')

      builder.withSchema(['users', '*'], schema)

      const db = await builder.open()

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('accepts path parameter', async () => {
      const builder = new ValkeyrieBuilder()

      const db = await builder.open(':memory:')

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('accepts options parameter', async () => {
      const builder = new ValkeyrieBuilder()

      const db = await builder.open(undefined, {
        destroyOnClose: true,
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('accepts both path and options', async () => {
      const builder = new ValkeyrieBuilder()

      const db = await builder.open(':memory:', {
        destroyOnClose: true,
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })
  })

  describe('openWithDriver() Method', () => {
    test('returns a working Valkeyrie instance from a custom driver', async () => {
      const builder = new ValkeyrieBuilder()
      const spy: DriverSpy = { created: 0 }

      const db = await builder.openWithDriver(createSpyDriverFn(spy))

      assert.ok(db instanceof Valkeyrie)
      assert.strictEqual(spy.created, 1)
      await db.set(['k'], 'v')
      const entry = await db.get(['k'])
      assert.strictEqual(entry.value, 'v')
      await db.close()
    })

    test('applies registered schemas through the custom driver', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('user')
      builder.withSchema(['users', '*'], schema)
      const spy: DriverSpy = { created: 0 }

      const db = await builder.openWithDriver(createSpyDriverFn(spy))

      assert.ok(db instanceof Valkeyrie)
      assert.strictEqual(spy.created, 1)
      await db.close()
    })
  })

  describe('from() Method', () => {
    test('returns a Valkeyrie instance', async () => {
      const builder = new ValkeyrieBuilder()
      const data = [{ id: 1 }]

      const db = await builder.from(data, {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('works with empty iterable', async () => {
      const builder = new ValkeyrieBuilder()
      const data: Array<{ id: number }> = []

      const db = await builder.from(data, {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('works with schemas registered', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('item')

      builder.withSchema(['items', '*'], schema)

      const data = [{ id: 1 }]
      const db = await builder.from(data, {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('accepts keyProperty as function', async () => {
      const builder = new ValkeyrieBuilder()
      const data = [{ userId: 1 }]

      const db = await builder.from(data, {
        prefix: ['users'],
        keyProperty: (item) => item.userId,
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('forwards options to Valkeyrie', async () => {
      const builder = new ValkeyrieBuilder()
      const data = [{ id: 1 }]

      const db = await builder.from(data, {
        prefix: ['items'],
        keyProperty: 'id',
        path: ':memory:',
        destroyOnClose: true,
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('uses driverFn over path when both are provided', async () => {
      const builder = new ValkeyrieBuilder()
      const spy: DriverSpy = { created: 0 }
      const testPath = join(tmpdir(), `builder-from-${randomUUID()}.db`)

      const db = await builder.from([{ id: 1, value: 'a' }], {
        prefix: ['items'],
        keyProperty: 'id',
        path: testPath,
        driverFn: createSpyDriverFn(spy),
      })

      assert.ok(db instanceof Valkeyrie)
      assert.strictEqual(spy.created, 1)
      const item = await db.get(['items', 1])
      assert.deepEqual(item.value, { id: 1, value: 'a' })
      await assert.rejects(access(testPath))
      await db.close()
    })
  })

  describe('fromAsync() Method', () => {
    test('returns a Valkeyrie instance', async () => {
      const builder = new ValkeyrieBuilder()

      async function* generate() {
        yield { id: 1 }
      }

      const db = await builder.fromAsync(generate(), {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('works with empty async iterable', async () => {
      const builder = new ValkeyrieBuilder()

      async function* generate() {
        // Empty generator
      }

      const db = await builder.fromAsync(generate(), {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('works with schemas registered', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('item')

      builder.withSchema(['items', '*'], schema)

      async function* generate() {
        yield { id: 1 }
      }

      const db = await builder.fromAsync(generate(), {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('accepts keyProperty as function', async () => {
      const builder = new ValkeyrieBuilder()

      async function* generate() {
        yield { userId: 1 }
      }

      const db = await builder.fromAsync(generate(), {
        prefix: ['users'],
        keyProperty: (item) => item.userId,
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('forwards options to Valkeyrie', async () => {
      const builder = new ValkeyrieBuilder()

      async function* generate() {
        yield { id: 1 }
      }

      const db = await builder.fromAsync(generate(), {
        prefix: ['items'],
        keyProperty: 'id',
        path: ':memory:',
        destroyOnClose: true,
      })

      assert.ok(db instanceof Valkeyrie)
      await db.close()
    })

    test('uses driverFn over path when both are provided', async () => {
      const builder = new ValkeyrieBuilder()
      const spy: DriverSpy = { created: 0 }
      const testPath = join(tmpdir(), `builder-fromasync-${randomUUID()}.db`)

      async function* generate() {
        yield { id: 1, value: 'a' }
      }

      const db = await builder.fromAsync(generate(), {
        prefix: ['items'],
        keyProperty: 'id',
        path: testPath,
        driverFn: createSpyDriverFn(spy),
      })

      assert.ok(db instanceof Valkeyrie)
      assert.strictEqual(spy.created, 1)
      const item = await db.get(['items', 1])
      assert.deepEqual(item.value, { id: 1, value: 'a' })
      await assert.rejects(access(testPath))
      await db.close()
    })
  })

  describe('Builder Reusability', () => {
    test('can call open() multiple times', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('user')

      builder.withSchema(['users', '*'], schema)

      const db1 = await builder.open()
      const db2 = await builder.open()

      assert.ok(db1 instanceof Valkeyrie)
      assert.ok(db2 instanceof Valkeyrie)
      assert.notStrictEqual(db1, db2)

      await db1.close()
      await db2.close()
    })

    test('can call from() multiple times', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('item')

      builder.withSchema(['items', '*'], schema)

      const db1 = await builder.from([{ id: 1 }], {
        prefix: ['items'],
        keyProperty: 'id',
      })

      const db2 = await builder.from([{ id: 2 }], {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db1 instanceof Valkeyrie)
      assert.ok(db2 instanceof Valkeyrie)
      assert.notStrictEqual(db1, db2)

      await db1.close()
      await db2.close()
    })

    test('can call fromAsync() multiple times', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('item')

      builder.withSchema(['items', '*'], schema)

      async function* generate1() {
        yield { id: 1 }
      }
      async function* generate2() {
        yield { id: 2 }
      }

      const db1 = await builder.fromAsync(generate1(), {
        prefix: ['items'],
        keyProperty: 'id',
      })

      const db2 = await builder.fromAsync(generate2(), {
        prefix: ['items'],
        keyProperty: 'id',
      })

      assert.ok(db1 instanceof Valkeyrie)
      assert.ok(db2 instanceof Valkeyrie)
      assert.notStrictEqual(db1, db2)

      await db1.close()
      await db2.close()
    })

    test('can mix open(), from(), and fromAsync() calls', async () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('data')

      builder.withSchema(['data', '*'], schema)

      const db1 = await builder.open()

      const db2 = await builder.from([{ id: 1 }], {
        prefix: ['data'],
        keyProperty: 'id',
      })

      async function* generate() {
        yield { id: 2 }
      }
      const db3 = await builder.fromAsync(generate(), {
        prefix: ['data'],
        keyProperty: 'id',
      })

      assert.ok(db1 instanceof Valkeyrie)
      assert.ok(db2 instanceof Valkeyrie)
      assert.ok(db3 instanceof Valkeyrie)

      await db1.close()
      await db2.close()
      await db3.close()
    })
  })

  describe('Schema Registration Patterns', () => {
    test('registers exact pattern', () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('exact')

      const result = builder.withSchema(['users', 'alice'], schema)

      assert.strictEqual(result, builder)
    })

    test('registers wildcard pattern', () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('wildcard')

      const result = builder.withSchema(['users', '*'], schema)

      assert.strictEqual(result, builder)
    })

    test('registers pattern with multiple wildcards', () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('multi')

      const result = builder.withSchema(['users', '*', 'posts', '*'], schema)

      assert.strictEqual(result, builder)
    })

    test('registers empty pattern', () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('root')

      const result = builder.withSchema([], schema)

      assert.strictEqual(result, builder)
    })

    test('registers patterns with different key part types', () => {
      const builder = new ValkeyrieBuilder()
      const schema = createMockSchema('mixed')

      builder
        .withSchema(['string'], schema)
        .withSchema(['number', 123], schema)
        .withSchema(['bigint', 456n], schema)
        .withSchema(['boolean', true], schema)
        .withSchema(['binary', new Uint8Array([1, 2, 3])], schema)

      // No assertion needed - if it doesn't throw, it works
      assert.ok(builder)
    })
  })
})
