# Security Audit — `@truto/ginger`

I audited the data-access layer (`crypto`, `sql-builder`, `pagination`, `service`, adapters) and the underlying `@truto/sqlite-builder` primitives it relies on. I found one **critical, confirmed-exploitable SQL injection** plus several lower-severity issues.

> **Status: ALL FINDINGS FIXED & RE-VERIFIED.**
> The exploit suite (`src/security-exploits.test.ts`) was flipped to assert the secured behaviour and now passes (14 tests). Full project suite: **213 pass / 0 fail**, `tsc --noEmit` clean, `eslint` 0 errors. Original exploits were first reproduced against a live `bun:sqlite` database.
>
> | #   | Finding                                                             | Severity              | Original status                              | Fix status                                                       |
> | --- | ------------------------------------------------------------------- | --------------------- | -------------------------------------------- | ---------------------------------------------------------------- |
> | 1   | SQL injection via forged pagination cursor                          | 🔴 Critical           | ✅ Exploit confirmed (cross-tenant + oracle) | ✅ **Fixed** — forged cursor rejected; injection blocked         |
> | 2   | IDOR on `get`/`update`/`delete`                                     | 🟠 High               | ✅ Exploit confirmed                         | ✅ **Fixed** — `where` scope honoured; cross-tenant denied       |
> | 3   | `orderBy` allow-list skipped for shapeless schemas                  | 🟡 Med → ⚪️ Info      | ❌ Not exploitable (`list()` threw first)    | ✅ **Hardened** — secret-column ordering blocked + safe wildcard |
> | 4   | `limit` not lower-bounded                                           | 🟢 Low                | ✅ Confirmed                                 | ✅ **Fixed** — must be a positive integer ≤ 1000                 |
> | 5   | AES key length not enforced                                         | 🟢 Low                | ✅ Confirmed (128-bit accepted)              | ✅ **Fixed** — non-256-bit keys rejected                         |
> | 6   | `DefaultKeyProvider` env fallback                                   | 🟢 Low                | ✅ Confirmed                                 | ✅ **Fixed** — implicit `process.env.SECRET_KEY` removed         |
> | —   | `buildInsert`/`buildUpdate` fragment splice (same root cause as #1) | 🟠 (config-dependent) | ✅ Confirmed (unit)                          | ✅ **Fixed** — values forced through `toBindableValue`           |
>
> Exploit evidence is in **Verification**; applied patches are in **Remediation** at the very bottom.

---

## 🔴 CRITICAL — SQL injection via forged pagination cursor

**Where:** `decodeCursor` (`src/pagination.ts`) → `buildCursorConditions` (`src/pagination.ts`) → `list()` (`src/service.ts`)

**Root cause.** The "opaque" cursor is just `base64(JSON)` and is fully client-controlled. `decodeCursor` validates the _shape_ of `orderBy` and that `values` is an array — but **never the types of `values` elements**:

`src/pagination.ts` (lines 33-57):

```ts
    if (!Array.isArray(parsed.values)) {
      throw new Error('Invalid values in cursor')
    }
    ...
    // Validate orderBy structure
    for (const order of parsed.orderBy) { ... }   // values[] elements never checked
```

Those values then flow into `buildCursorConditions`, which interpolates them through the `sql` tagged template:

`src/pagination.ts` (lines 131-137):

```ts
    if (i === orderBy.length - 1) {
      conditions.push(
        useGreaterThan
          ? sql`${columnIdent} > ${value}`
          : sql`${columnIdent} < ${value}`,
      )
```

The library assumes `${value}` becomes a bound `?` placeholder. But `@truto/sqlite-builder`'s `sql` template treats **any object shaped like `{ text: string, values: [] }` as a pre-compiled SQL fragment and splices `.text` in raw**:

`node_modules/@truto/sqlite-builder/dist/index.js` (lines 141-148):

```js
    if (value && typeof value === "object" && "text" in value && "values" in value && ...) {
      const fragment = value;
      text += fragment.text;          // RAW — no escaping, no placeholder
      queryValues.push(...fragment.values);
    } else {
      text += "?";
      queryValues.push(sqlValue(value));
    }
```

So an attacker forges a cursor whose `values[0]` is `{ "text": "<arbitrary SQL>", "values": [] }`.

**Confirmed PoC** (I ran this against the real modules, then removed the scratch file). Forged token:

```json
{
  "orderBy": [{ "column": "id", "direction": "asc" }],
  "values": [
    {
      "text": "0 OR (SELECT substr(api_key_encrypted,1,1) FROM users LIMIT 1)='s'",
      "values": []
    }
  ],
  "direction": "next"
}
```

Resulting query actually emitted by `list({ cursor })` (with a tenant-scoping hook active):

```sql
SELECT "users"."id", "users"."name" FROM "users"
WHERE (("tenant_id" = ?)) AND "users"."id" > 0
   OR (SELECT substr(api_key_encrypted,1,1) FROM users LIMIT 1)='s'
ORDER BY "users"."id" ASC LIMIT ?
-- bound params: ["tenant-A", 51]
```

**Impact (severe, two ways):**

1. **Boolean-blind injection** into the `WHERE` clause → exfiltrate any data in the DB, including the AES-encrypted secret columns (`api_key_encrypted`), other tables, `sqlite_master`, etc. No `;` is needed, so the builder's stacked-query guard (`/;[\s\S]*\S/`) never trips.
2. **Tenant-isolation bypass.** Because of `AND`/`OR` precedence, the emitted `WHERE (tenant_id=?) AND <cursor> OR <injection>` collapses to `(... ) OR <injection>` — the `tenant_id` scope added by a `before` hook is completely defeated whenever the injected predicate is true (`... OR 1=1` dumps every tenant's rows).

This is reachable in normal usage: `cursor` is the value apps round-trip from `?cursor=...`.

**Fix (close it at the boundary).** Reject non-primitive cursor values in `decodeCursor`:

```typescript
for (const value of parsed.values) {
  if (
    value !== null &&
    typeof value !== 'string' &&
    typeof value !== 'number' &&
    typeof value !== 'boolean'
  ) {
    throw new Error('Invalid value in cursor: only primitives are allowed')
  }
}
```

Defense-in-depth: in `buildCursorConditions`, bind the value explicitly so it can never be interpreted as a fragment (e.g. construct the fragment manually with a literal `?` and `values: [sql.value(value)]`). The same `{text,values}` confusion also theoretically affects `buildInsert`/`buildUpdate` value interpolation (`` sql`${value}` ``) — only exploitable if a `createSchema`/`updateSchema` permits object-typed fields, but worth hardening for the same reason.

---

## 🟠 HIGH — No row-level authorization on `get` / `update` / `delete` (IDOR)

`get`, `update`, and `delete` look up **only by primary key** with no way to attach an ownership/tenant filter:

`src/service.ts` (lines 796-805):

```ts
      const where: Record<string, unknown> = {}
      if (Array.isArray(this.primaryKey)) { ... } else {
        where[this.primaryKey] = id
      }
```

`before` hooks can't scope these (they don't have the row yet), and for `update`/`delete` the mutation has already executed by the time an `after` hook could inspect `ctx.result`. The README's own multi-tenant example only adds tenant filtering to `list`/`create`, so an app built exactly per the docs is vulnerable: a user in tenant A can read/modify/delete tenant B's rows by guessing an `id`. Recommend a first-class `where`/scope option (or a documented before-hook contract) for `get`/`update`/`delete`.

---

## ⚪️ MEDIUM → INFORMATIONAL — `orderBy` allow-list skipped for shapeless schemas (NOT EXPLOITABLE — verified)

> **Verification correction:** I originally rated this Medium, but testing proved it is **not reachable end-to-end**, so it is downgraded to informational. Keeping the analysis here for the fixer.

`validateOrderBy` only runs when the row schema exposes an object `shape`:

`src/service.ts` (lines 399-402):

```ts
const allowedColumns = this.getAllowedColumns()
if (!allowedColumns.includes('*')) {
  validateOrderBy(orderBy, allowedColumns)
}
```

If `rowSchema` is `z.record(...)`, `z.any()`, etc., `getSchemaColumns()` returns `['*']` and the order/cursor column allow-list is bypassed. **However**, the same `['*']` value then propagates to `resolveMainColumns()` → `buildSelect`, which calls `sql.ident('users.*')`. The builder rejects that (`Invalid identifier: users.*`), so `list()` throws a `SqlBuilderError` **before** the skipped validation can ever be reached. In other words: a shapeless `rowSchema` cannot complete a `list()` query at all, so there is no exploitable window. (Confirmed by test — see Verification.)

Still worth tightening defensively: require an explicit column allow-list when the schema is shapeless, and/or only emit `"users".*` for the wildcard case rather than `sql.ident('users.*')`.

---

## 🟢 LOW

- **Unbounded / non-integer `limit`.** `list` only checks `limit > 1000`; negative or non-integer values aren't rejected. `actualLimit = limit + 1` with a negative limit yields `LIMIT -1` (= unlimited in SQLite) → pagination bypass / resource exhaustion. Validate `Number.isInteger(limit) && limit >= 1`.

  `src/service.ts` (lines 390-392):

  ```ts
  if (limit > 1000) {
    throw new ValidationError('Limit cannot exceed 1000')
  }
  ```

- **AES key length not enforced.** `getKey` imports whatever bytes are supplied; a 16-byte (AES-128) key is accepted despite the "AES-256-GCM" branding (tested behavior in `crypto.test.ts:180`). Reject keys that aren't 32 bytes.
- **`DefaultKeyProvider` env fallback.** Silently adopts `process.env.SECRET_KEY` as the `default` key when none is passed — surprising key provenance in shared/CI environments.

  `src/crypto.ts` (lines 14-20):

  ```ts
  if (
    this.keys.size === 0 &&
    typeof process !== 'undefined' &&
    process.env?.SECRET_KEY
  ) {
    this.keys.set('default', process.env.SECRET_KEY)
  }
  ```

- **`RETURNING` clause hand-quotes identifiers** instead of using `sql.ident`, bypassing the "never concatenate" guarantee (`src/service.ts:700-701`). Dev-config only (`primaryKey`), so low risk, but inconsistent.

## ℹ️ Informational

- `query()` is a raw-SQL escape hatch — safe only if callers never interpolate untrusted input into the `sql` string (it is documented, but a sharp edge).
- Cursors are **unauthenticated** (base64, not signed/encrypted). Even after the critical fix, treat cursor contents as untrusted input; consider an HMAC if cursors should be tamper-evident.

---

### Bottom line

The encryption (AES-256-GCM, random 96-bit IV, authenticated, fresh ciphertext per call) and the `where`/filter path (validated identifiers + parameterized values) are solid. The **critical issue is the forged-cursor SQL injection**, which also defeats tenant isolation — it should be patched before any further release.

---

## Verification

All exploits were reproduced against a **real in-memory SQLite database** (`bun:sqlite`, driven through the library's own `fromBunSqlite` adapter — i.e. the exact code path a Bun consumer uses). The suite lives at `src/security-exploits.test.ts`.

```bash
bun test src/security-exploits.test.ts
# → 12 pass / 0 fail
```

**Fixture:** a `users` table seeded with 4 rows — ids 1–2 in `tenant-A`, ids 3–4 in `tenant-B` — each with a real, key-matched `api_key_encrypted` value produced by the library's `encrypt()`. The service applies a `before.list` hook that scopes `where.tenant_id = auth.user.tenantId` (mirroring the README's multi-tenant example). Every SQL string prepared by the service is captured for evidence.

### #1 Critical — forged-cursor SQL injection ✅ CONFIRMED

- **`EXPLOIT 1a` (cross-tenant read / WHERE-filter bypass).** As `tenant-A`, a normal `list()` returns exactly `[1, 2]`. Submitting a forged cursor whose `values[0]` is `{ "text": "0 OR 1=1", "values": [] }` returns `[1, 2, 3, 4]` — **tenant B's rows leak**. The exact SQL the DB executed (captured live):

  ```sql
  SELECT "users"."id", "users"."name", "users"."email", "users"."tenant_id",
         "users"."created_at", "users"."updated_at"
  FROM "users"
  WHERE (("tenant_id" = ?)) AND "users"."id" > 0 OR 1=1
  ORDER BY "users"."id" ASC LIMIT ?
  -- bound params: ["tenant-A", 51]
  ```

  Note `... AND "users"."id" > 0 OR 1=1`: the injected `OR 1=1` is spliced **raw** (not bound), and `AND`/`OR` precedence makes the whole `tenant_id` scope collapse.

- **`EXPLOIT 1b` (blind boolean oracle on a secret column).** Forging `values[0].text = "0 OR (SELECT substr(api_key_encrypted,1,1) FROM users WHERE id=3)='Z'"` returns **all 4 rows** (predicate true — `'Z'` is the correct first char of `btoa('default')`), while guessing `'X'` returns only **2 rows** (predicate false). The differing row counts are a working oracle that reads another tenant's `api_key_encrypted` byte-by-byte. Verified: `true → 4`, `false → 2`.

- **Root cause unit check.** `buildInsert('users', { name: 'x', evil: { text: '(SELECT password FROM admins)', values: [] } })` produces SQL whose `.text` **contains `(SELECT password FROM admins)`** and whose `.values` does **not** — confirming the `{text,values}` fragment-confusion is the shared root cause (also affects `buildInsert`/`buildUpdate` whenever a permissive create/update schema lets an object value through).

### #2 High — IDOR on `get`/`update`/`delete` ✅ CONFIRMED

With the tenant hook present only on `list` (per the README), acting as `tenant-A`:

- **`EXPLOIT 2a`:** `get(3, { auth: TENANT_A })` returns tenant B's row (`tenant_id === 'tenant-B'`, `name === 'Carol'`).
- **`EXPLOIT 2b`:** `update(3, { name: 'PWNED-BY-A' }, { auth: TENANT_A })` succeeds; re-reading id 3 shows `name === 'PWNED-BY-A'`.
- **`EXPLOIT 2c`:** `delete(4, { auth: TENANT_A })` returns `true`; id 4 is gone.

### #3 Medium → Informational — `orderBy` skip ❌ NOT EXPLOITABLE

- Object schema **correctly rejects** `orderBy: [{ column: 'api_key_encrypted' }]` (allow-list works).
- Shapeless schema (`z.record(...)`) makes `list()` throw `SqlBuilderError: Invalid identifier: users.*` — the query can't be built, so the skipped validation is never reached. Downgraded to informational.

### #4 Low — `limit` not lower-bounded ✅ CONFIRMED

- `list({ limit: 1001 })` throws `ValidationError` (the only bound).
- With 100 rows seeded, `list({ limit: -2 })` is **accepted** and returns ~98 rows (`actualLimit = limit + 1 = -1` → SQLite `LIMIT -1` = unlimited). Verified `result.length > 50`.

### #5 Low — AES key length not enforced ✅ CONFIRMED

- `new DefaultKeyProvider({ default: btoa('1234567890123456') }).getKey('default')` resolves to a `CryptoKey` with `algorithm.length === 128` and still round-trips `encrypt()` — a 128-bit key is silently accepted despite the "AES-256-GCM" branding.

### #6 Low — `DefaultKeyProvider` env fallback ✅ CONFIRMED

- With no keys passed to the constructor, `new DefaultKeyProvider()` silently adopts `process.env.SECRET_KEY` as the `default` key; `getKey('default')` resolves successfully.

---

## Remediation (applied)

All fixes landed and the exploit suite was flipped to assert the secured behaviour. Re-verified: `bun test` → **213 pass / 0 fail**; `bun run typecheck` clean; `bun run lint` 0 errors.

### #1 Critical — forged-cursor SQL injection

- **`src/pagination.ts` — `decodeCursor`:** added a guard that rejects any cursor `values[]` element that is not a primitive (`string | number | boolean | null`). This closes the attack at the trust boundary — a `{ text, values }`-shaped object can no longer enter the pipeline. (`list()` surfaces it as a `ValidationError: Invalid cursor`.)
- **`src/sql-builder.ts` — new `toBindableValue(value)` helper:** the single choke point that coerces an untrusted value to a bindable scalar and **throws on objects/arrays**. Used everywhere a dynamic value is interpolated, so the `sql` template can never receive a fragment-shaped object to splice raw.
- **`src/pagination.ts` — `buildCursorConditions`:** every comparison/equality value now goes through `toBindableValue(...)` (defense-in-depth).
- **`src/sql-builder.ts` — `buildInsert` / `buildUpdate`:** column values now go through `toBindableValue(...)` (closes the same root cause for INSERT/UPDATE with permissive schemas).
- Regression: legitimate cursor pagination (`nextCursor` round-trip) still works — covered by `FIXED 1a (regression)`.

### #2 High — IDOR on `get` / `update` / `delete`

- **`src/types.ts`:** added an optional `where?: Record<string, unknown>` scope to `GetParams`, `UpdateParams`, `DeleteParams`.
- **`src/service.ts`:** `get` / `update` / `delete` now read their effective options from `ctx.params` **after** `before` hooks run, so a hook can inject `ctx.params.where = { tenant_id: ... }` (previously hook mutations to these methods were silently ignored).
- **`src/sql-builder.ts` — `buildSelectById`:** the scope filter is AND-combined with the primary-key lookup (qualified under `$table` when joins are active).
- `update` / `delete` apply the scope to (a) the existence check (non-matching → `NotFoundError`, so the row is never touched) **and** (b) the `UPDATE`/`DELETE` `WHERE` itself (defense-in-depth).
- Usage: scope `get`/`update`/`delete` from a `before` hook exactly like `list` (the README's tenant example should be extended to all methods).

### #3 Medium → Informational — `orderBy` / shapeless schema

- **`src/service.ts` — `list`:** added an explicit guard that **rejects ordering by any declared secret column**, enforced for both shaped and shapeless schemas (`Cannot order by secret column "…"`).
- **`src/sql-builder.ts` — `buildSelect`:** a bare `'*'` column is now emitted as `"table".*` instead of `sql.ident('table.*')`, so wildcard SELECTs are valid (the latent crash from the audit is gone) without widening the attack surface.

### #4 Low — `limit` not lower-bounded

- **`src/service.ts` — `list`:** `limit` must now be a positive integer (`Number.isInteger(limit) && limit >= 1`) in addition to the existing `<= 1000` cap. Negative / zero / non-integer limits throw `ValidationError`.

### #5 Low — AES key length not enforced

- **`src/crypto.ts` — `DefaultKeyProvider.getKey`:** the base64-decoded key must be exactly **32 bytes (256-bit)**; otherwise it throws `EncryptionError`. The `catch` re-throws typed `EncryptionError`s unchanged so the message is clear.

### #6 Low — `DefaultKeyProvider` env fallback

- **`src/crypto.ts` — constructor:** removed the implicit `process.env.SECRET_KEY` fallback. Keys must be passed explicitly (or via a custom `keyProvider`). **Behaviour change** — this contradicts the original PROMPT.md spec (§5) that asked for an env default; callers relying on it must now read the env var and pass it in.

### Tests updated

- `src/security-exploits.test.ts` — all `EXPLOIT *` cases flipped to `FIXED *` (assert the attack is blocked) plus regression cases for legit pagination, valid limits, normal-column ordering, and own-tenant access.
- `src/crypto.test.ts` — "handle keys of different lengths" → "reject keys that are not 256-bit".
- `src/index.test.ts` — "fallback to process.env" → "NOT fall back to process.env".
