---
'@truto/ginger': minor
---

Add the `@truto/ginger/zod` subpath: `import * as z from '@truto/ginger/zod'`.

It is the same `z` as `import { z } from '@truto/ginger'` (same zod copy, same
exports), but a bundler can see through the namespace import and
drop zod members a Worker never calls, which it cannot do for the index's
`export * as z`. Measured on a bundle that only calls `z.object(...)`: about
200 KB through the index, about 94 KB through the subpath. The index's `z` is
unchanged.

Bundler caveat: zod is `"sideEffects": false` and installs its English messages
with a top-level `config(en())`, which a bundler may drop. esbuild (wrangler)
keeps it for both spellings; Bun.build keeps it through the index but drops it
for the subpath (as it does for a raw `import * as z from 'zod/v4'`); Vite and
Rollup drop it for both. A dropped call leaves messages as "Invalid input" (not
"Invalid input: expected string, received number"). To restore them, add
`import en from 'zod/v4/locales/en.js'` and call `z.config(en())` once at
startup.
