---
'@truto/ginger': minor
---

Add the `@truto/ginger/zod` subpath: `import * as z from '@truto/ginger/zod'`.

It is the same `z` as `import { z } from '@truto/ginger'` (same zod copy, same
locale-free surface), but a bundler can see through the namespace import and
drop zod members a Worker never calls, which it cannot do for the index's
`export * as z`. Measured on a bundle that only calls `z.object(...)`: about
200 KB through the index, about 94 KB through the subpath. The index's `z` is
unchanged.
