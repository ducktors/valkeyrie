---
"valkeyrie": patch
---

fix: await driver cleanup in `Valkeyrie.cleanup()` so `open()` waits for the expiry sweep and surfaces its errors; a failed `open()`, `from()` or `fromAsync()` now closes the driver without destroying data, even with `destroyOnClose`
