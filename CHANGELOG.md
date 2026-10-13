# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [3.0.0]

### Changed

- **Breaking: `z` no longer carries zod's locale tables.** `export * as z from 'zod/v4'` re-exported a namespace as a value, so a bundler had to keep every member, including `locales`: all ~50 message tables (~200 KB of the ~400 KB zod costs in a bundle), evaluated on the cold start of any Worker that imports ginger. `z` now comes from `src/zod.ts`, which star-re-exports `zod/v4` and shadows the names that reach the locale tables. `z.locales`, `z.z` and `z.core.locales` are now `undefined` (typed `never` and `@deprecated`, so `z.locales.fr()` is a compile error), `z.default` is gone (`'default' in z` is false), and `z.core` keeps every binding but is no longer the same object as `zod/v4`'s `core`. Everything else (schemas, `z.core`, `z.iso`, `z.coerce`, `z.toJSONSchema`, `z.config`, every type) is unchanged, and English messages are unaffected. To change the message language, import the table directly: `import fr from 'zod/v4/locales/fr.js'; z.config(fr())`.

### Added

- **`@truto/ginger/zod` subpath:** `import * as z from '@truto/ginger/zod'` is the same `z` (same zod copy, same exports) as `import { z } from '@truto/ginger'`, but a bundler can drop the zod members a Worker never calls: about 94 KB instead of 200 KB on a bundle that only calls `z.object(...)`. zod installs its English messages with a top-level `config(en())` and is `"sideEffects": false`, so a bundler may drop that call: esbuild (wrangler) keeps it, Bun.build drops it for the subpath, Vite and Rollup drop it for both spellings. Without it messages read "Invalid input"; call `z.config(en())` once at startup (`import en from 'zod/v4/locales/en.js'`) to restore them.

### Fixed

- **Cursor pagination around NULL values:** SQLite sorts NULL below every value, first on ASC and last on DESC, and `= NULL`, `> NULL` and `< NULL` are never true. With two or more order-by columns, a page ending on a row whose earlier sort column was NULL stopped the listing, because the `col = ?` equality prefix and `col > ?` both matched nothing, and on DESC the NULL rows were never reached. For a joined or exposed column, a table column a plain `z.object` row schema doesn't describe, a field that reads NULL as NULL, and any NULL cursor value, the conditions are now NULL-aware (`col IS ?`, `col IS NOT NULL`, `(col < ? OR col IS NULL)`). A field that rejects NULL, or reads it as another value (a `.transform`, a `.catch`, a `z.coerce` without `.nullable()`), keeps byte-identical SQL: its cursors carry that value, not NULL, so its NULL rows past a page boundary are still skipped, in either direction, as before. So does every table column of a row schema that isn't a plain `z.object` (`.transform()`, `z.preprocess()`, `.pipe()`, `.catch()`, `.default()`, the same wrapped in `.nullable()`, `.readonly()` or `z.lazy()`, or a shapeless `z.record()` or `z.any()`), or that has an `.overwrite()` check: it may turn any column's NULL into a value. The same goes for zod's v3 API (`import { z } from 'zod'` on zod 3.x), where `.refine()` also wraps the object and so counts too. Joined and exposed columns are added after the row schema runs, and stay NULL-aware. Cursor tokens are unchanged.

### Security

- **Cursor conditions escaped the caller's `where`:** `buildCursorConditions` OR-joined its conditions and returned them bare, and `buildSelect` ANDs that fragment with the caller's `where`. SQL binds `AND` tighter than `OR`, so with two or more order-by columns every branch after the first `OR` was evaluated without the caller's filter, and a filtered, paginated list could return rows the filter excludes. The OR-chain is now parenthesised. Single-column ordering emits byte-identical SQL, and cursor tokens are unchanged, so cursors already in flight keep working.

## [2.1.1]

### Fixed

- **Encryption of large values:** `encrypt` packed the ciphertext with `btoa(String.fromCharCode(...bytes))`, which spreads every byte into an argument list. A secret field past the engine's argument limit (roughly 256KB on workerd) threw `Maximum call stack size exceeded` and surfaced as a 500 on the write path. Base64 conversion now runs in fixed-size chunks in both directions; the encoding is byte-for-byte identical, so previously stored ciphertext decrypts unchanged.
- **Pagination over non-latin1 values:** cursors carry the values of the columns being sorted on, and `JSON.stringify` emits non-ASCII characters raw, so listing a table ordered by a text column containing a CJK name or an emoji threw `InvalidCharacterError` out of `btoa` — page 2 of the list 500'd. Code units above U+00FF are now escaped as `\uXXXX` before encoding, which is exactly the set `btoa` rejected, so cursors already in flight keep their identical byte representation and `decodeCursor` is unchanged. Deliberately not UTF-8: that would have re-encoded the latin1 range (`José`) that `btoa` accepts today, silently changing what a live cursor decodes to.
- **Keyset pagination:** missing equality prefix on the final cursor column could return rows already seen on a previous page when ordering by multiple columns.

## [2.1.0] - 2026-06-24

### Added

- **Joins v2:** `localColumn` for FK joins where the FK is on the base table (not the PK join pattern)
- **Chained joins:** nested `joins` on `JoinDef` with tree-shaped `include` (`{ parent: { child: true } }`)
- **`expose`:** project joined columns to the top-level row (`{ from: '$join.col', as: 'col' }`)
- **Join-aware `where` / `orderBy` / `count`:** filter and sort on `$alias.column` and exposed aliases; `count({ include, where })` uses `COUNT(DISTINCT base.pk)` with the same join graph
- **Scoped `get` on joined columns:** `get(id, { where: { environment_id: { in: [...] } } })` returns `null` when the join scope does not match
- **Per-call join projections:** nested `include.select` overrides for list vs detail shapes
- Collision-safe join SQL aliases: legacy `prefix_col` is kept unless it would collide with a base-table or expose column name (then `prefix__col`)

### Changed

- `include` accepts nested objects; boolean shorthand (`include: { profile: true }`) is unchanged
- `CountParams` accepts optional `include` for join-aware counts
- `JoinDef.localPk` is optional when `localColumn` is set

## [2.0.1] - 2026-06-11

### Changed

- Bump `@truto/sqlite-builder` to 2.0.1 (unforgeable SQL fragments, branded `compileFilter` output, placeholder integrity checks)

## [2.0.0] - 2026-06-11

### Security

- **Critical:** Fix SQL injection via forged pagination cursors — reject non-primitive cursor values and bind all dynamic SQL values through `toBindableValue()` so `{ text, values }` objects cannot be spliced as raw SQL
- **High:** Add optional `where` scope on `get`, `update`, and `delete` for row-level authorization (tenant/ownership isolation); `before` hooks can now inject `ctx.params.where` and the method honours it

### Changed

- **BREAKING:** `DefaultKeyProvider` no longer falls back to `process.env.SECRET_KEY` — encryption keys must be passed explicitly or via a custom `keyProvider`
- **BREAKING:** `before` hooks on `get`, `update`, and `delete` now affect the operation (params are read from `ctx.params` after hooks run)
- Enforce 256-bit (32-byte) AES keys in `DefaultKeyProvider.getKey`
- Validate `list` `limit` is a positive integer (rejects negative, zero, and non-integer values)
- Block ordering by declared secret columns regardless of row schema shape
- Emit safe `"table".*` wildcard in `buildSelect` instead of invalid `sql.ident('table.*')`

### Added

- `toBindableValue()` helper in `sql-builder` for safe parameter binding
- `where` option on `GetParams`, `UpdateParams`, and `DeleteParams`

### Documentation

- README: new "Row-level authorization (multi-tenant scoping)" section with hook pattern and `count`/`query` caveats

## [1.0.0]

### Added

- Core `Service` class with type-safe CRUD: `list`, `get`, `create`, `update`, `delete`, `count`, `query`
- Cursor-based pagination with opaque base64 tokens and `next`/`prev` support
- Declarative joins system (`one` / `many`) with conditional `include` and type-narrowed results
- AES-256-GCM field encryption via Web Crypto with `kid:iv:cipher` packed format
- `DefaultKeyProvider` with ENV fallback and per-key-ID caching
- Feathers.js-inspired hook system (`before` / `after` / `error` per method)
- Dependency injection via `deps` (available in hooks and custom methods)
- Custom `ServiceError` hierarchy: `NotFoundError`, `ValidationError`, `AuthError`, `DatabaseError`, `EncryptionError`, `HookError`, `CursorError`, `SqlBuilderError`, `DependencyError`
- Safe SQL generation via `@truto/sqlite-builder` — no raw string concatenation
- Full `d.ts` exports for all helper types (`JoinDef`, `SecretFieldDef`, `ListParams`, `OrderBy`, etc.)
- Comprehensive test suite (153 tests) covering encryption cycle, pagination cursors, joins, hook ordering, and SQL injection protection
