# Ginger

A type-safe SQLite data access layer that works with **Cloudflare D1**, **Bun SQLite**, and **Durable Object SqlStorage**. Comes with cursor-based pagination, declarative joins, AES-256-GCM field encryption, and a Feathers.js-inspired hook system.

Built with TypeScript and [Zod v4](https://zod.dev/) for complete type safety. All dynamic SQL is generated via [@truto/sqlite-builder](https://github.com/trutohq/truto-sqlite-builder) — no raw string concatenation, ever.

## Install

```bash
bun add @truto/ginger
```

Peer dependencies:

```bash
bun add zod @truto/sqlite-builder
```

## Features

- **Fully type-safe** — Zod schemas drive runtime validation and static types
- **Cursor pagination** — opaque base64 cursors with `next` / `prev` support
- **Declarative joins** — `one` and `many` joins with conditional `include`
- **Field selection** — flat `select: ['id', 'name', '$teams.name']` array picks main-row and join columns, with type narrowing
- **Field encryption** — AES-256-GCM via Web Crypto, stored as `kid:iv:cipher`
- **Hook system** — `before` / `after` / `error` hooks per method, inspired by Feathers.js
- **Dependency injection** — pass other services via `deps` for cross-service logic
- **SQL injection protection** — every query is parameterised through `@truto/sqlite-builder`
- **Custom error hierarchy** — `NotFoundError`, `ValidationError`, `AuthError`, `EncryptionError`, etc.

## Quick example

A complete `users` service with an encrypted `apiKey`, a join to `teams`, a custom `withMembership` method, and a hook that enforces tenant filtering via `auth.user`.

```typescript
import {
  createService,
  Service,
  z,
  type AuthContext,
  type Database,
  type JoinDef,
  type SecretFieldDef,
} from '@truto/ginger'

// ── Schemas ──────────────────────────────────────────────────────────

// Secret fields (apiKey / api_key_encrypted) must NOT be in rowSchema.
// They are managed by the secrets config and returned only when includeSecrets: true.
const UserRow = z.object({
  id: z.number(),
  name: z.string(),
  email: z.string(),
  tenant_id: z.string(),
  created_at: z.string(),
  updated_at: z.string().nullable(),
})

const CreateUser = z.object({
  name: z.string().min(1).max(255),
  email: z.string().email(),
  apiKey: z.string().min(32),
  tenant_id: z.string(),
})

const UpdateUser = z.object({
  name: z.string().min(1).max(255).optional(),
  email: z.string().email().optional(),
})

const TeamRow = z.object({
  id: z.number(),
  name: z.string(),
  description: z.string(),
})

// ── Joins ────────────────────────────────────────────────────────────

const userJoins = {
  teams: {
    kind: 'many',
    localPk: 'id',
    through: {
      table: 'user_teams',
      from: 'user_id',
      to: 'team_id',
    },
    remote: {
      table: 'teams',
      pk: 'id',
      select: ['id', 'name', 'description'],
    },
    schema: TeamRow,
  },
} satisfies Record<string, JoinDef>

// ── Secrets ──────────────────────────────────────────────────────────

const userSecrets = [
  {
    logicalName: 'apiKey',
    columnName: 'api_key_encrypted',
    keyId: 'user-secrets',
  },
] as const satisfies readonly SecretFieldDef[]

// ── Service ──────────────────────────────────────────────────────────

class UsersService extends Service<
  typeof UserRow,
  typeof CreateUser,
  typeof UpdateUser,
  typeof userJoins,
  typeof userSecrets
> {
  /** Fetch a user together with their team memberships */
  async withMembership(id: number, auth: AuthContext) {
    return this.get(id, {
      auth,
      include: { teams: true },
    })
  }
}

// ── Factory ──────────────────────────────────────────────────────────

function createUsersService(
  db: Database,
  encryptionKeys: Record<string, string>,
) {
  return new UsersService({
    table: 'users',
    db: db as any,
    rowSchema: UserRow,
    createSchema: CreateUser,
    updateSchema: UpdateUser,
    joins: userJoins,
    secrets: userSecrets,
    encryptionKeys,
    timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
    hooks: {
      list: {
        before: async (ctx: any) => {
          if (!ctx.auth.user?.tenantId) throw new Error('Missing tenant')
          ctx.params.where = {
            ...ctx.params.where,
            tenant_id: ctx.auth.user.tenantId,
          }
        },
      },
      create: {
        before: async (ctx: any) => {
          if (!ctx.auth.user?.tenantId) throw new Error('Missing tenant')
          ctx.data.tenant_id = ctx.auth.user.tenantId
        },
      },
    },
  })
}

// ── Worker entry point ───────────────────────────────────────────────

export default {
  async fetch(request: Request, env: any): Promise<Response> {
    const usersService = createUsersService(env.DB, {
      default: env.ENCRYPTION_KEY,
      'user-secrets': env.ENCRYPTION_KEY,
    })

    const auth = {
      user: { id: 'usr_1', tenantId: 'tnt_1', roles: ['admin'] },
    }

    // Create (apiKey is encrypted transparently)
    const user = await usersService.create(
      {
        name: 'Jane Doe',
        email: 'jane@example.com',
        apiKey: 'sk_ex_abcdef1234567890abcdef1234567890',
        tenant_id: 'tnt_1',
      },
      { auth },
    )

    // List with pagination + joins
    const page = await usersService.list({
      auth,
      limit: 20,
      include: { teams: true },
      orderBy: [{ column: 'created_at', direction: 'desc' }],
    })

    // Get with decrypted secrets
    const full = await usersService.get(user.id, {
      auth,
      includeSecrets: true,
    })

    // Custom method
    const withTeams = await usersService.withMembership(user.id, auth)

    return Response.json({ user, page, full, withTeams })
  },
}
```

## Core concepts

### Database adapters

Ginger works with any SQLite database that satisfies the `Database` interface. Three adapters are provided out of the box:

**Cloudflare D1** — pass the binding directly, no adapter needed:

```typescript
import { createService } from '@truto/ginger'

const service = createService({
  table: 'users',
  db: env.DB, // D1 binding satisfies Database natively
  // ...
})
```

**Bun SQLite** — wrap with `fromBunSqlite`:

```typescript
import { Database } from 'bun:sqlite'
import { createService, fromBunSqlite } from '@truto/ginger'

const bunDb = new Database('myapp.sqlite')
const service = createService({
  table: 'users',
  db: fromBunSqlite(bunDb),
  // ...
})
```

**Durable Object SqlStorage** — wrap with `fromDurableObjectStorage`:

```typescript
import { DurableObject } from 'cloudflare:workers'
import { createService, fromDurableObjectStorage } from '@truto/ginger'

export class MyDO extends DurableObject {
  service = createService({
    table: 'users',
    db: fromDurableObjectStorage(this.ctx.storage.sql),
    // ...
  })
}
```

### Service configuration

```typescript
import { createService, z } from '@truto/ginger'

const service = createService({
  table: 'users',
  db, // Database instance (D1, fromBunSqlite, or fromDurableObjectStorage)
  rowSchema: UserRow, // canonical decoded row
  createSchema: CreateUser, // POST body schema
  updateSchema: UpdateUser, // PATCH body schema (partial)
  joins: userJoins, // declarative join map
  secrets: userSecrets, // secret field definitions
  hooks: {
    /* ... */
  }, // before/after/error hooks
  deps: { teams: teamsService }, // other services
  primaryKey: 'id', // default "id"
  defaultOrderBy: { column: 'created_at', direction: 'desc' },
  timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
  keyProvider: customProvider, // or pass encryptionKeys: { ... }
})
```

### CRUD methods

Every service gets these methods out of the box:

| Method                        | Description                                                      |
| ----------------------------- | ---------------------------------------------------------------- |
| `list(params)`                | Cursor-based paginated list with filtering, ordering, and joins  |
| `get(id, opts)`               | Single record by ID with optional `include` and `includeSecrets` |
| `create(data, opts)`          | Validates with `createSchema`, returns decoded row               |
| `update(id, data, opts)`      | Partial update, merges with existing row                         |
| `delete(id, opts)`            | Hard delete                                                      |
| `count(params)`               | Count rows matching a typed `where` clause                       |
| `query(sql, opts, ...params)` | Low-level escape hatch returning decoded rows                    |

All methods accept an `auth` object:

```typescript
interface AuthContext {
  user?: {
    id: string
    roles: string[]
    [k: string]: unknown
  }
}
```

### Pagination

Opaque cursor tokens (base64-encoded JSON) with `next` / `prev` support:

```typescript
const page1 = await service.list({
  auth,
  limit: 20,
  orderBy: [{ column: 'created_at', direction: 'desc' }],
})

// page1.result     — array of rows
// page1.nextCursor — pass to next call for the next page
// page1.prevCursor — pass to next call for the previous page

const page2 = await service.list({
  auth,
  cursor: page1.nextCursor,
  limit: 20,
})
```

### Joins

Define type-safe joins with conditional inclusion. The return type of `get` / `list` changes based on which joins are included:

```typescript
const joins = {
  profile: {
    kind: 'one' as const,
    localPk: 'id',
    remote: {
      table: 'profiles',
      pk: 'user_id',
      select: ['bio', 'avatar'],
    },
    schema: ProfileSchema,
  },
  teams: {
    kind: 'many' as const,
    localPk: 'id',
    through: {
      table: 'user_teams',
      from: 'user_id',
      to: 'team_id',
    },
    remote: {
      table: 'teams',
      pk: 'id',
      select: ['id', 'name'],
    },
    where: 'teams.active = 1',
    schema: TeamSchema,
  },
}

const user = await service.get(id, {
  auth,
  include: { profile: true, teams: true },
})
// user.profile → ProfileRow | null
// user.teams   → TeamRow[]
```

### Field selection

Limit the columns returned by `list` / `get` / `create` / `update` with a flat
`select` array. Tokens beginning with `$alias` (mirroring the alias-block
syntax used by `where`) pick columns from a configured join.

```typescript
// Main row only — pick a subset of the row schema
const slim = await usersService.get(1, {
  auth,
  select: ['id', 'name'],
})
// → { id: 1, name: 'Jane' }

// Mix main-row columns with join columns using `$alias.col`
const withTeam = await usersService.get(1, {
  auth,
  include: { teams: true },
  select: ['id', 'name', '$teams.name'],
})
// → { id: 1, name: 'Jane', teams: [{ id: 7, name: 'Eng' }, ...] }

// Bare `$alias` expands to all columns in that join's `remote.select`
const withFullTeams = await usersService.get(1, {
  auth,
  include: { teams: true },
  select: ['id', '$teams'],
})

// Cursor pagination keeps working — orderBy columns are silently included
const page = await usersService.list({
  auth,
  select: ['name'],
  orderBy: [{ column: 'created_at', direction: 'asc' }],
  limit: 20,
})
// page.result[0] → { id: 1, name: 'Jane', created_at: '...' }
```

Behaviour summary:

| Aspect                       | Behaviour                                                                                                                                                                                                   |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Primary key                  | Always silently included in SELECT and kept in the returned row                                                                                                                                             |
| `orderBy` columns            | Always silently included so `nextCursor` / `prevCursor` keep working                                                                                                                                        |
| Join columns                 | Use `$alias.column` (per-call override) or `$alias` (use the join's configured `remote.select`). The join must also be enabled via `include[alias] = true` — selection alone does not auto-include.         |
| Join column scope            | A per-call `$alias.column` may reference any column on the joined table (not just the configured `remote.select`). The join's PK is silently included.                                                      |
| `includeSecrets`             | Independent from `select`. Secrets are surfaced only when `includeSecrets: true`, irrespective of what's in `select`. Secret `logicalName`s are not valid `select` tokens — use `includeSecrets` to opt in. |
| Validation                   | Unknown columns and unknown join aliases throw `ValidationError`. Empty/omitted `select` returns the full row (backwards compatible).                                                                       |
| `count` / `delete` / `query` | Do not accept `select` (they don't return rows in the same shape).                                                                                                                                          |

Type narrowing — pass the array `as const` (or rely on the `<const>` type
parameter on the service methods) to get `Pick<Row, …>` in the return type.
Per-call join column overrides currently don't narrow the join schema in
types — at the type level the join still appears with its full configured
schema; at runtime the row only contains what was selected.

### Field encryption

Sensitive fields are encrypted with **AES-256-GCM** via Web Crypto and stored as `kid:iv:cipher` (base64 segments):

```typescript
const secrets = [
  {
    logicalName: 'apiKey', // field name in your application
    columnName: 'api_key_enc', // actual column in the DB
    keyId: 'api-keys', // key identifier
  },
] as const

// Provide keys directly
const service = createService({
  // ...
  secrets,
  encryptionKeys: {
    default: env.ENCRYPTION_KEY,
    'api-keys': env.API_KEY_ENCRYPTION_KEY,
  },
})

// Or provide a custom KeyProvider
const service = createService({
  // ...
  secrets,
  keyProvider: {
    async getKey(keyId: string): Promise<CryptoKey> {
      // your custom key retrieval logic
    },
  },
})
```

> **Important:** Do **not** include secret fields in `rowSchema` — neither the `columnName` (e.g. `api_key_enc`) nor the `logicalName` (e.g. `apiKey`). Ginger manages these separately:
>
> - The `columnName` is excluded from SELECT queries by default and included only when `includeSecrets: true`.
> - The `logicalName` is injected into the result after decryption when `includeSecrets: true`.
>
> If you accidentally include either in `rowSchema`, Ginger will throw a `ValidationError` at service creation time with a clear message explaining what to remove.

Generate a key:

```typescript
import { generateSecretKey } from '@truto/ginger'

const key = await generateSecretKey()
// → base64-encoded 256-bit key
```

Encryption is handled automatically:

- On `create` / `update` — the `logicalName` field is encrypted and stored in the `columnName` column
- On `get` / `list` (when `includeSecrets: true`) — the `columnName` column is decrypted and returned as `logicalName`

### Hooks

Feathers.js-inspired hooks with `before` / `after` / `error` phases:

```typescript
const service = createService({
  // ...
  hooks: {
    list: {
      before: [authHook, tenantFilterHook],
      after: [auditLogHook],
      error: [errorReportingHook],
    },
    create: {
      before: async (ctx) => {
        ctx.data.createdBy = ctx.auth.user?.id
      },
      after: async (ctx) => {
        await sendWelcomeEmail(ctx.result.email)
      },
    },
  },
})
```

Hooks receive a context object:

```typescript
interface BaseCtx {
  auth: AuthContext
  db: Database
  deps: ServiceDeps
  method: MethodName
  params?: unknown
  data?: unknown
  result?: unknown
}
```

Hooks run sequentially in registration order. If a `before` or `after` hook throws, control jumps to the `error` chain.

### Custom methods

Extend `Service` to add arbitrary async methods that can leverage all built-in functionality:

```typescript
class UsersService extends Service</* ... */> {
  async findByEmail(email: string, auth: AuthContext) {
    const results = await this.query(
      'SELECT * FROM users WHERE email = ?',
      { auth },
      email,
    )
    return results[0] ?? null
  }

  async deactivate(id: number, auth: AuthContext) {
    return this.update(id, { active: false }, { auth })
  }
}
```

### Dependency injection

Pass other services via `deps` — they're available on `this.deps` and in every hook context:

```typescript
const teamsService = createService({
  /* ... */
})

const usersService = createService({
  // ...
  deps: { teams: teamsService },
  hooks: {
    delete: {
      after: async (ctx) => {
        // Clean up team memberships when a user is deleted
        await ctx.deps.teams.query(
          'DELETE FROM user_teams WHERE user_id = ?',
          { auth: ctx.auth },
          ctx.params.id,
        )
      },
    },
  },
})
```

### Error handling

All errors extend `ServiceError` with structured `code` and `statusCode`:

```typescript
import {
  ServiceError,
  NotFoundError,
  ValidationError,
  AuthError,
  DatabaseError,
  EncryptionError,
  HookError,
  CursorError,
} from '@truto/ginger'

try {
  await service.get(id, { auth })
} catch (error) {
  if (error instanceof NotFoundError) {
    return new Response('Not found', { status: 404 })
  }
  if (error instanceof ValidationError) {
    return new Response(error.message, { status: 400 })
  }
}
```

| Error class       | Code               | Status |
| ----------------- | ------------------ | ------ |
| `NotFoundError`   | `NOT_FOUND`        | 404    |
| `ValidationError` | `VALIDATION_ERROR` | 400    |
| `AuthError`       | `AUTH_ERROR`       | 403    |
| `DatabaseError`   | `DATABASE_ERROR`   | 500    |
| `EncryptionError` | `ENCRYPTION_ERROR` | 500    |
| `HookError`       | `HOOK_ERROR`       | 500    |
| `CursorError`     | `CURSOR_ERROR`     | 400    |

## SQL schema example

```sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  api_key_encrypted TEXT,
  tenant_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT
);

CREATE TABLE teams (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT,
  active INTEGER DEFAULT 1
);

CREATE TABLE user_teams (
  user_id INTEGER NOT NULL,
  team_id INTEGER NOT NULL,
  PRIMARY KEY (user_id, team_id),
  FOREIGN KEY (user_id) REFERENCES users(id),
  FOREIGN KEY (team_id) REFERENCES teams(id)
);

CREATE TABLE profiles (
  user_id INTEGER PRIMARY KEY,
  bio TEXT,
  avatar TEXT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);
```

## Requirements

- Any runtime with Web Crypto (Cloudflare Workers, Bun, Node 20+)
- A supported SQLite backend: Cloudflare D1, `bun:sqlite`, or Durable Object `SqlStorage`
- TypeScript 5.0+
- Zod 3.25+ (v4)
- @truto/sqlite-builder 1.0+

## Development

```bash
bun install              # Install dependencies
bun test                 # Run tests
bun run dev              # Run tests in watch mode
bun run build            # Build the library
bun run typecheck        # TypeScript type checking
bun run lint             # ESLint
bun run format           # Prettier
```

## License

MIT — see [LICENSE](LICENSE) for details.
