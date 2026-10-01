---
'@truto/ginger': minor
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

**Behaviour change:** `z.locales`, `z.z` and `z.core.locales` are now
`undefined` (typed `never` and `@deprecated`, so using one is a compile error).
To change the message language, import the table directly:
`import fr from 'zod/v4/locales/fr.js'; z.config(fr())`. No package in the
workspace used any of the three.

Measured on a Worker bundle: ginger's contribution through zod drops from
~398 KB to ~190 KB of eagerly-evaluated source.
