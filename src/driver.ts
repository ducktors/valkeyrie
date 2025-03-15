import type { Serializer } from './serializers/serializer.js'

export interface Driver {
  close: () => Promise<void>
  get: (keyHash: string, now: number) => Promise<DriverValue | undefined>
  set: (
    keyHash: string,
    value: unknown,
    versionstamp: string,
    expiresAt?: number,
  ) => Promise<void>
  delete: (keyHash: string) => Promise<void>
  list: (
    startHash: string,
    endHash: string,
    prefixHash: string,
    now: number,
    limit: number,
    reverse?: boolean,
  ) => Promise<DriverValue[]>
  cleanup: (now: number) => Promise<void>
  withTransaction: <T>(callback: () => Promise<T>) => Promise<T>
}

export function defineDriver(
  initDriver:
    | ((path?: string, serializerInit?: () => Serializer) => Promise<Driver>)
    | Driver,
): (path?: string, serializerInit?: () => Serializer) => Promise<Driver> {
  if (initDriver instanceof Function) {
    return initDriver
  }

  return async () => initDriver
}

export interface DriverValue<T = unknown> {
  keyHash: string
  value: T
  versionstamp: string
}
