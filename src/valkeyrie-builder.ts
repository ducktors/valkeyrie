import type { StandardSchemaV1 } from '@standard-schema/spec'
import { SchemaRegistry } from './schema-registry.ts'
import type { Serializer } from './serializers/serializer.ts'
import { sqliteDriver } from './sqlite-driver.ts'
import { kFrom, kFromAsync, kOpen } from './symbols.ts'
import type {
  SchemaRegistryEntry,
  SchemaRegistry as SchemaRegistryType,
} from './types/schema-registry-types.ts'
import type { DriverFactory, FromOptions, Key } from './valkeyrie.ts'
import { Valkeyrie } from './valkeyrie.ts'

/**
 * Builder for creating Valkeyrie instances with schema validation.
 * Schemas are registered before opening the database and become immutable after.
 *
 * @template TRegistry - Compile-time schema registry for type inference
 */
export class ValkeyrieBuilder<
  TRegistry extends SchemaRegistryType = readonly [],
> {
  private schemaRegistry: SchemaRegistry

  constructor() {
    this.schemaRegistry = new SchemaRegistry()
  }

  /**
   * Registers a schema pattern for validation.
   * Uses `const` type parameter to automatically infer literal types without `as const`.
   *
   * @param pattern Key pattern with optional '*' wildcards
   * @param schema Standard schema for validation
   * @returns A new builder with the updated schema registry type
   */
  withSchema<const TPattern extends Key, TSchema extends StandardSchemaV1>(
    pattern: TPattern,
    schema: TSchema,
  ): ValkeyrieBuilder<
    readonly [
      ...TRegistry,
      readonly [TPattern, TSchema] extends SchemaRegistryEntry
        ? readonly [TPattern, TSchema]
        : never,
    ]
  > {
    this.schemaRegistry.register(pattern, schema)
    // Type assertion needed because we're tracking types at compile-time while mutating at runtime
    return this as unknown as ValkeyrieBuilder<
      readonly [
        ...TRegistry,
        readonly [TPattern, TSchema] extends SchemaRegistryEntry
          ? readonly [TPattern, TSchema]
          : never,
      ]
    >
  }

  /**
   * Opens a new Valkeyrie database instance with registered schemas.
   *
   * Accepts either a file path for the built-in SQLite backend, or a
   * {@link DriverFactory} to supply a custom storage backend. Omit the argument
   * for an in-memory SQLite database.
   *
   * @param path Optional path to the database file (defaults to in-memory)
   * @param options Optional configuration options
   * @returns A new Valkeyrie instance with schema validation and type inference
   */
  async open(
    path?: string,
    options?: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    },
  ): Promise<Valkeyrie<TRegistry>>
  /**
   * Opens a new Valkeyrie database instance backed by a custom driver,
   * with registered schemas.
   *
   * @param driverFn Function that creates the driver, optionally using the serializer
   * @param options Optional configuration options
   * @returns A new Valkeyrie instance with schema validation and type inference
   */
  async open(
    driverFn: DriverFactory,
    options?: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    },
  ): Promise<Valkeyrie<TRegistry>>
  async open(
    pathOrDriver?: string | DriverFactory,
    options: {
      serializer?: () => Serializer
      destroyOnClose?: boolean
    } = {},
  ): Promise<Valkeyrie<TRegistry>> {
    const driverFn: DriverFactory =
      typeof pathOrDriver === 'function'
        ? pathOrDriver
        : (serializer) => sqliteDriver(pathOrDriver, serializer)
    return Valkeyrie[kOpen](
      driverFn,
      options,
      this.schemaRegistry,
    ) as unknown as Promise<Valkeyrie<TRegistry>>
  }

  /**
   * Creates and populates a Valkeyrie database from a synchronous iterable with schemas.
   * @param iterable The iterable to populate the database from
   * @param options Configuration options including prefix and key extraction
   * @returns A populated Valkeyrie instance with schema validation and type inference
   */
  async from<T>(
    iterable: Iterable<T>,
    options: FromOptions<T>,
  ): Promise<Valkeyrie<TRegistry>> {
    return Valkeyrie[kFrom](
      iterable,
      options,
      this.schemaRegistry,
    ) as unknown as Promise<Valkeyrie<TRegistry>>
  }

  /**
   * Creates and populates a Valkeyrie database from an asynchronous iterable with schemas.
   * @param iterable The async iterable to populate the database from
   * @param options Configuration options including prefix and key extraction
   * @returns A populated Valkeyrie instance with schema validation and type inference
   */
  async fromAsync<T>(
    iterable: AsyncIterable<T>,
    options: FromOptions<T>,
  ): Promise<Valkeyrie<TRegistry>> {
    return Valkeyrie[kFromAsync](
      iterable,
      options,
      this.schemaRegistry,
    ) as unknown as Promise<Valkeyrie<TRegistry>>
  }
}
