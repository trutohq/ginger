---
'@truto/ginger': patch
---

Fix two customer-triggerable 500s in base64 handling. Neither changes the
encoding of any data that encodes successfully today.

**`encrypt` overflowed on large secrets.** The ciphertext was packed with
`btoa(String.fromCharCode(...bytes))`, which spreads every byte into an
argument list. Any secret field past the engine's argument limit (roughly
256KB on workerd) threw `Maximum call stack size exceeded`. Base64 conversion
now runs in fixed-size chunks in both directions, with no argument-count
ceiling. The output is byte-for-byte identical, so previously stored ciphertext
decrypts unchanged.

**`encodeCursor` threw on non-latin1 values.** Keyset cursors carry the values
of the columns being sorted on, and `JSON.stringify` emits non-ASCII characters
raw, so paginating over a text column holding a CJK name or an emoji threw
`InvalidCharacterError` out of `btoa` — page 2 of the list 500'd. Code units
above U+00FF are now escaped as `\uXXXX` before encoding. That is exactly the
set `btoa` rejected, so every cursor that encodes today keeps its identical
byte representation, and `decodeCursor` is unchanged because `JSON.parse`
already reverses the escape. Deliberately not UTF-8, which would have re-encoded
the latin1 range (`José`) that works today.
