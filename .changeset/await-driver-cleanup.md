---
"valkeyrie": patch
---

fix: await driver cleanup in `Valkeyrie.cleanup()` so `open()` waits for the expiry sweep and surfaces its errors
