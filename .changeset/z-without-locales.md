---
'@truto/ginger': major
---

Keep zod's locale tables out of every bundle that uses `z`.

`export * as z from 'zod/v4'` re-exports a namespace as a value, so a bundler
has to keep every member of it. One of them is `locales`, which pulls all ~50
locale message tables (~200 KB of the ~400 KB zod costs in a bundle) onto the
module-evaluation path of any Worker that imports ginger, even one that only
calls `z.object(...)`. Cloudflare charges that to cold-start CPU (error 10021).

`z` now comes from `src/zod.ts`, which star-re-exports `zod/v4` and shadows the
two namespace-valued names that reach the locale tables: `z.locales` and
`z.z` (a namespace of zod itself), plus `z.core.locales`. Everything else —
schemas, `z.core`, `z.iso`, `z.coerce`, `z.toJSONSchema`, `z.config`, every type
— is unchanged, and a zod upgrade that adds an export flows through untouched.
English, zod's default message table, is unaffected.

**Breaking:** `z.locales`, `z.z` and `z.core.locales` are now `undefined`
(typed `never` and `@deprecated`, so accessing a member of one — `z.locales.fr()`
— is a compile error, though passing the value itself still type-checks), and
`z.default` is gone (`'default' in z` is false). `z.core` keeps every binding but
is no longer the same object as `zod/v4`'s `core`. To change the message
language, import the table directly: `import fr from 'zod/v4/locales/fr.js';
z.config(fr())`. No package in the workspace (elaichi, clove, envoy, saffron,
truto) used any of these.

Measured on a Worker bundle: ginger's contribution through zod drops from
~398 KB to ~190 KB of eagerly-evaluated source.
