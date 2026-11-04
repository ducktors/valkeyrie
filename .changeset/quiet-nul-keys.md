---
"valkeyrie": minor
---

fix: escape NUL bytes in string and Uint8Array key parts, so keys with a NUL byte read back and never alias another key. A stored Uint8Array key part that holds a 0x00 byte gets new bytes.
