---
'@truto/ginger': patch
---

Fix `Maximum call stack size exceeded` when encrypting large secret values.

`encrypt` packed the ciphertext with `btoa(String.fromCharCode(...bytes))`, which
spreads every byte into an argument list. Any secret field large enough to exceed
the engine's argument limit (roughly 256KB on workerd) threw, surfacing as a 500
on the write path. Base64 conversion now runs in fixed-size chunks in both
directions, with no argument-count ceiling. The encoding is byte-for-byte
identical, so previously stored ciphertext decrypts unchanged.
