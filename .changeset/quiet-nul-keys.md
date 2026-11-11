---
"valkeyrie": minor
---

fix: escape NUL bytes in string and Uint8Array key parts, so keys with a NUL byte read back and never alias another key. A stored key with a 0x00 byte in such a part gets new bytes, and the same bytes can be another key in the old format. So `open()` now throws `KeyFormatError` for a non-empty store that 0.8 or earlier wrote, instead of letting a new key read, overwrite or delete an old entry. Migrate a SQLite store with `migrateSqliteKeyFormat()` from `valkeyrie/migrate`; see the "Upgrading to 0.9" guide.
