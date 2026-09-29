---
"@lotiai/composer": patch
---

Asynchronous error handlers now receive a context. The `<workflow>__errorHandler` activity runs the context provider's `beforeStep("__errorHandler__")` and `afterStep` hooks around `onError`, as the synchronous path already did, instead of passing `ctx` as `undefined`.
