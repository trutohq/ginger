# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.1.1]

### Fixed

- **Encryption of large values:** `encrypt` packed the ciphertext with `btoa(String.fromCharCode(...bytes))`, which spreads every byte into an argument list. A secret field past the engine's argument limit (roughly 256KB on workerd) threw `Maximum call stack size exceeded` and surfaced as a 500 on the write path. Base64 conversion now runs in fixed-size chunks in both directions; the encoding is byte-for-byte identical, so previously stored ciphertext decrypts unchanged.
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
