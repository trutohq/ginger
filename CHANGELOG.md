# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
