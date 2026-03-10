# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **BREAKING:** Renamed D1-specific types to generic names: `D1Database` -> `Database`, `D1PreparedStatement` -> `PreparedStatement`, `D1Result` -> `QueryResult`, `D1ExecResult` -> `ExecResult`
- Deleted `MockD1Database` test utility; tests now use the production `fromBunSqlite` adapter

### Added

- `fromBunSqlite(db)` adapter — wraps a `bun:sqlite` `Database` into the generic `Database` interface
- `fromDurableObjectStorage(sql)` adapter — wraps a Durable Object `SqlStorage` into the generic `Database` interface
- Exported structural types: `BunSqliteDatabase`, `BunSqliteStatement`, `DurableObjectSqlStorage`, `SqlStorageCursor`

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
