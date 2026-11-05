---
"valkeyrie": minor
---

Add a Valkey and Redis driver, `valkeyDriver({ client, namespace })` from `valkeyrie/drivers/valkey`, so several processes can share one database. It takes an ioredis client (an optional peer dependency) and supports Redis 6.2+ and Valkey 7.2+.
