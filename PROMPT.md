**Prompt for the LLM**

We are using Bun as the runtime.

---

> **Context**
> You are generating the source code for a TypeScript library that provides a thin, type‑safe data‑access layer on top of **Cloudflare D1** (SQLite).
> The host repository already has the usual scaffolding (build pipeline, ESLint, tests, CI, etc.). _Focus exclusively on the library implementation._
>
> **All requirements below are mandatory**; missing any of them is considered incorrect.

---

### 1  High‑level deliverable

- Provide **fully‑typed TypeScript** (latest ES target) that compiles with `strict`, `noUncheckedIndexedAccess`, and `exactOptionalPropertyTypes`.
- Include generous inline JSDoc plus external `README.md` snippets that explain public APIs and example usage.
- Follow idiomatic functional + class patterns—no decorators, no runtime reflection, no experimental stage‑3 features.

---

### 2  Core “Service” abstraction

1. **Constructor signature**

   ```ts
   interface ServiceOptions<
     TRow extends z.ZodTypeAny,
     TCreate extends z.ZodTypeAny,
     TUpdate extends z.ZodTypeAny,
     TJoins extends Record<string, JoinDef>,
     TSecrets extends readonly SecretFieldDef[] | undefined,
     TDeps extends Record<string, BaseService<any, any, any, any, any>> = {},
   > {
     table: string
     db: D1Database // Cloudflare binding (mandatory)
     builder?: SqliteBuilder // from @truto/sqlite-builder (optional override)
     rowSchema: TRow // canonical decoded row
     createSchema: TCreate // POST body
     updateSchema: TUpdate // PATCH body (partial)
     joins?: TJoins // declarative join map (see §4)
     secrets?: TSecrets // secret field map (see §5)
     hooks?: Partial<HookMap<TCtx<any>>> // user hooks
     deps?: TDeps // other services
     primaryKey?: string | string[] // default "id"
     defaultOrderBy?: OrderBy // default for list pagination
   }
   ```

2. **Built‑in CRUD methods**

   | Method                   | Signature sketch                                   |                                               |
   | ------------------------ | -------------------------------------------------- | --------------------------------------------- |
   | `list(params)`           | cursor‑based pagination (see §3)                   |                                               |
   | `get(id, opts)`          | optional `include`, optional `includeSecrets`      |                                               |
   | `create(data, opts)`     | validates with `createSchema`; returns decoded row |                                               |
   | `update(id, data, opts)` | partial update; merges with existing row           |                                               |
   | `delete(id, opts)`       | hard delete                                        |                                               |
   | `count(where, opts)`     | count rows matching typed `where`                  |                                               |
   | \`query(customSql        | builderCb)\`                                       | low‑level escape hatch returning decoded rows |

   Each public method accepts an **`AuthContext`** object:

   ```ts
   interface AuthContext {
     user?: {
       id: string
       roles: string[]
       [k: string]: unknown
     }
   }
   ```

   and a **`MethodOptions`** union that always contains that `auth` object.

3. **Custom methods**

   - Users extend the generated `BaseService` and add arbitrary async methods.
   - Those methods automatically gain `before/after/error` hook support by passing the method name to `runHooks`.

---

### 3  Pagination requirements

- Implement **opaque cursor tokens** (base64‑encoded JSON) with `next` / `prev` support.

- Allow caller to supply `orderBy` (array of `{ column, direction }`).

- Paginated response schema:

  ```ts
  interface ListResult<T> {
    result: T[]
    nextCursor?: string
    prevCursor?: string
  }
  ```

- The `builder` from **`@truto/sqlite-builder`** - @https://github.com/trutohq/truto-sqlite-builder must be used to generate all list/count/select/update/delete SQL. Never concatenate raw values.

---

### 4  Joins system

- Accept a `joins` map where **each key** defines:

  ```ts
  interface JoinDef {
    kind: 'one' | 'many'
    localPk: string // column on base table
    through?: { table; from; to } // optional join table
    remote: {
      table: string
      pk: string
      select: string[] // columns to project
      alias?: string // output name
    }
    where?: string | ((ctx: { auth: AuthContext }) => string)
    schema: z.ZodTypeAny // validates joined rows
  }
  ```

- `include` parameter (record of `boolean`) governs which joins are executed.

- Return type of `get` / `list` **changes** via conditional types so the presence of `include.<name>` is reflected in the result (teams → `TeamRow[]`, profile → `ProfileRow | null`).

---

### 5  Secret‑attribute encryption

- Accept `secrets?: SecretFieldDef[]` (see prior design).
- Use **AES‑256‑GCM** via Web Crypto.
- Store packed string `"kid:iv:cipher"` (base64 segments, separated by `:`).
- Provide a default `keyProvider` that reads `ENV.SECRET_KEY`; allow override for testing.
- Inject crypto via auto‑generated hooks:

  - `before.create`, `before.update` — encrypt logical fields → ciphertext column.
  - `after.get`, `after.list` (conditionally on `includeSecrets`) — decrypt back.

- Row & API schemas expose **only plaintext logical names**.

---

### 6  Hooks system

Feathers.js is the inspiration for the hooks system.

- `HookPhase = 'before' | 'after' | 'error'`.

- A `HookMap` is:

  ```ts
  type HookMap<TCtx> = {
    [P in MethodName]?: {
      before?: Hook<TCtx> | Hook<TCtx>[]
      after?: Hook<TCtx> | Hook<TCtx>[]
      error?: Hook<TCtx & { error: Error }> | Hook<TCtx & { error: Error }>[]
    }
  }
  ```

- Hooks run in registration order, `await`ed sequentially.

- If a hook **throws**, control jumps to the `error` chain.

- Hooks receive:

  ```ts
  interface BaseCtx {
    auth: AuthContext
    db: D1Database
    deps: ServiceDeps
    method: MethodName
    params?: unknown // exact generic per method
    data?: unknown
    result?: unknown
  }
  ```

  Every method accepts optional `opts` parameter, which will be mapped to the `params` object. There are some params which are standard - like `query`, `include`, `includeSecrets`, `orderBy`, `limit`, `offset`.

---

### 7  Dependency injection

- `deps` object (record of other services) is stored on `this.deps`.
- Exposed to every `HookCtx` and to any custom method via `this.deps.<serviceName>`.

---

### 8  Safe‑SQL with **@truto/sqlite-builder**

- All **dynamic** SQL (`SELECT`, `UPDATE`, `INSERT`, `DELETE`) **must** be produced with the builder.
- Provide thin wrapper helpers: `buildSelect`, `buildInsert`, `buildUpdate`, etc., that hide table/column names behind strongly‑typed generics.

---

### 9  Best‑practice guarantees

- No un‑parametrised `db.prepare()` anywhere.
- All errors are instances of a custom `ServiceError` hierarchy (`NotFound`, `ValidationError`, `AuthError`, etc.).
- 100 % unit test coverage for: encryption cycle, pagination cursors, conditional join return types, hook ordering, dependency injection. _(Test harness already exists in repo; just add test files.)_
- Public API surfaces **only** generics or Zod‑inferred types—no `any`, no `unknown` cast without validation.
- Provide `d.ts` exports for helper types (`JoinDef`, `SecretFieldDef`, `ListParams`, `OrderBy`, etc.).

---

### 10  Documentation snippet (include in README)

Show a **complete tiny example** creating a `users` service with:

- secret `apiKey`,
- join to `teams`,
- a custom method `withMembership`,
- a hook that enforces tenant filtering via `auth.user`.

Keep it runnable with D1 in Workers‑Preview.

---

> **Important:** > _Do not output any repository scaffolding (folder trees, ESLint configs, GitHub Actions, etc.)._
> Focus on the **library source**, inline docs, and tests that validate the behavior above.

---

**End of prompt**
