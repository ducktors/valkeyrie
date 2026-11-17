import type { Driver } from './driver.ts'
import { KEY_HASH_END } from './key-codec.ts'
import { KeyFormatError } from './key-format-error.ts'

/**
 * The key format that this version reads and writes. Format 1 is the format of
 * Valkeyrie 0.8 and earlier, which does not escape a 0x00 byte inside a string
 * or Uint8Array key part.
 */
export const KEY_FORMAT = 2

/**
 * The hash of the row that records the key format of a store: 0xFF, then
 * "key-format" in ASCII. It starts at {@link KEY_HASH_END}, so no key has this
 * hash and no list returns the row.
 */
export const KEY_FORMAT_KEY_HASH = `${KEY_HASH_END}${Buffer.from('key-format').toString('hex')}`

/**
 * Reads the key format number from the value of a key format row.
 *
 * @param value - The value of the row.
 * @returns The key format, or null if the value has no valid number.
 */
function readKeyFormat(value: unknown): number | null {
  if (
    typeof value === 'object' &&
    value !== null &&
    'keyFormat' in value &&
    typeof value.keyFormat === 'number'
  ) {
    return value.keyFormat
  }
  return null
}

/**
 * Records the current key format in a store.
 *
 * @param driver - The driver of the store.
 */
export async function writeKeyFormat(driver: Driver): Promise<void> {
  await driver.set(
    KEY_FORMAT_KEY_HASH,
    { keyFormat: KEY_FORMAT },
    await driver.generateVersionstamp(),
  )
}

/**
 * Checks the key format of a store before Valkeyrie reads or writes it. An
 * empty store gets the current key format. A store with entries and no key
 * format row was written by Valkeyrie 0.8 or earlier. Its bytes cannot tell
 * the two formats apart, so a key could address another entry.
 *
 * @param driver - The driver of the store.
 * @throws {KeyFormatError} If the store has entries in another key format.
 */
export async function checkKeyFormat(driver: Driver): Promise<void> {
  await driver.withTransaction(async () => {
    // An entry without an expiry matches every `now`, so 0 finds the row
    const formatRow = await driver.get(KEY_FORMAT_KEY_HASH, 0)
    if (formatRow !== undefined) {
      const found = readKeyFormat(formatRow.value)
      if (found !== KEY_FORMAT) {
        throw new KeyFormatError(found, KEY_FORMAT)
      }
      return
    }
    // `now` 0 also finds the expired entries that cleanup did not remove yet
    const [entry] = await driver.list('', KEY_HASH_END, '', 0, 1)
    if (entry !== undefined) {
      throw new KeyFormatError(1, KEY_FORMAT)
    }
    await writeKeyFormat(driver)
  })
}
