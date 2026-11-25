---
"valkeyrie": minor
---

Add a MongoDB driver, `mongodbDriver({ client, namespace })` from `valkeyrie/drivers/mongodb`, so several processes can share one database. It takes a `MongoClient` (`mongodb` is an optional peer dependency) and needs a replica set or a sharded cluster. The namespace can be any BSON value, such as an `ObjectId`, and the namespace field and collection names are options.
