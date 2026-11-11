/**
 * Error thrown when a store uses a key format that this version of Valkeyrie
 * does not read. The same bytes can address different keys in two formats, so
 * Valkeyrie does not open such a store: a key could read, overwrite or delete
 * another entry.
 */
export class KeyFormatError extends Error {
  /**
   * The key format of the store. 1 is a store with entries and no key format
   * row, written by Valkeyrie 0.8 or earlier. null is a key format row without
   * a valid number.
   */
  public readonly found: number | null
  /** The key format that this version of Valkeyrie reads and writes. */
  public readonly expected: number

  /**
   * @param found - The key format of the store, or null if it is not known.
   * @param expected - The key format that this version reads and writes.
   */
  constructor(found: number | null, expected: number) {
    super(
      found === 1
        ? `Valkeyrie cannot open this store: it has entries in key format 1 (Valkeyrie 0.8 or earlier), and this version uses key format ${expected}. Migrate the store first: https://ducktors.github.io/valkeyrie/guides/upgrading`
        : `Valkeyrie cannot open this store: it uses key format ${found ?? 'unknown'}, and this version uses key format ${expected}.`,
    )
    this.name = 'KeyFormatError'
    this.found = found
    this.expected = expected
  }
}
