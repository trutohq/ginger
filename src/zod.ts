/**
 * The `z` namespace ginger re-exports (`import { z } from '@truto/ginger'`),
 * also published as the `@truto/ginger/zod` subpath.
 *
 * This is zod v4's classic API, minus the locale tables. It is a module of its
 * own, rather than `export * as z from 'zod/v4'` in `index.ts`, for one reason:
 * bundle size on a cold path.
 *
 * `zod/v4` exposes `locales` as a namespace (`export * as locales`), and so
 * does `zod/v4/core`, and `zod/v4` also exports `z` — a namespace of itself.
 * A namespace re-exported as a value has to carry every member, because a
 * bundler cannot know which ones a consumer will touch — so `export * as z`
 * pins all ~50 locale files (~200 KB of message tables nobody asked for) into
 * every Worker that imports ginger, even one that only ever calls
 * `z.object(...)`. They are evaluated on cold start, which is where a Worker
 * pays for them (Cloudflare's startup CPU limit, error 10021).
 *
 * Shadowing the namespace-valued names with local exports stops `export *` from
 * re-exporting them, so nothing reaches `locales/index.js`. Everything else
 * is still a star re-export, so a zod upgrade that adds an export flows through
 * without an edit here. `src/zod.test.ts` pins both halves: what is kept
 * and what is shadowed.
 *
 * ENGLISH. zod installs its English messages with a top-level `config(en())`
 * in `classic/external.js`, and zod declares `"sideEffects": false`, so a
 * bundler is allowed to drop that call. Whether it does is the bundler's call,
 * not ours: esbuild (what wrangler uses) keeps it through both spellings here;
 * Bun.build keeps it through the index's `z` but drops it for the `./zod`
 * subpath (and for a raw `import * as z from 'zod/v4'` — upstream behaviour);
 * Vite/Rollup drop it for both. A dropped call degrades every message to
 * "Invalid input"; validation itself is unaffected. A consumer whose bundler
 * does this and who wants the full text installs it itself — `locales` is
 * shadowed here, so import the table from zod directly:
 * `import en from 'zod/v4/locales/en.js'` and `z.config(en())`.
 *
 * THE SUBPATH. Shadowing removes the locales, but `import { z } from
 * '@truto/ginger'` still hands the bundler a namespace *value* (`export * as z`
 * in the index), and a namespace used as a binding has to be materialized
 * whole: every schema class, `toJSONSchema` and the rest stay even when the
 * consumer only calls `z.object`. `import * as z from '@truto/ginger/zod'`
 * names this module directly instead, so the bundler sees `z.object` as a plain
 * member access and drops whatever is never touched. Same zod copy either way,
 * so `z.infer<>` and `instanceof` agree across both spellings. The index's
 * `z` stays for consumers that do not bundle for a cold path.
 */
export * from 'zod/v4'

// `z.core` stays (typed access such as `z.core.$ZodIssue` is in real use) but
// goes through a shadowed copy, because `zod/v4/core` has its own `locales`.
export * as core from './zod-core.js'

/**
 * @deprecated Not part of ginger's `z`: zod's locale tables are ~200 KB and
 * re-exporting them keeps all of them on every consumer's cold-start path.
 * Import the one you need directly — `import fr from 'zod/v4/locales/fr.js'`
 * and `z.config(fr())`.
 */
export const locales: never = undefined as never

/**
 * @deprecated Not part of ginger's `z`: zod's `z.z` is a namespace of the
 * whole of zod, locales included. Use `z` itself.
 */
export const z: never = undefined as never
