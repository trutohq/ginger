import type { z } from 'zod/v4'

/**
 * Generic SQLite database interface.
 * Compatible with Cloudflare D1, Bun SQLite (via adapter), and
 * Durable Object SqlStorage (via adapter).
 */
export interface Database {
  prepare(query: string): PreparedStatement
  dump(): Promise<ArrayBuffer>
  batch<T = unknown>(statements: PreparedStatement[]): Promise<QueryResult<T>[]>
  exec(query: string): Promise<ExecResult>
}

export interface PreparedStatement {
  bind(...values: unknown[]): PreparedStatement
  first<T = unknown>(): Promise<T | null>
  run(): Promise<QueryResult>
  all<T = unknown>(): Promise<QueryResult<T[]>>
  raw<T = unknown>(): Promise<T[]>
}

export interface QueryResult<T = unknown> {
  results?: T
  success: boolean
  meta: {
    duration: number
    size_after: number
    rows_read: number
    rows_written: number
    last_row_id: number
    changed_db: boolean
    changes: number
  }
}

export interface ExecResult {
  count: number
  duration: number
}

/**
 * Authentication context passed to all service methods
 */
export interface AuthContext {
  user?: {
    id: string
    roles: string[]
    [k: string]: unknown
  }
}

/**
 * Base context for all hooks
 */
export interface BaseCtx {
  auth: AuthContext
  db: Database
  deps: ServiceDeps
  method: MethodName
  params?: unknown
  data?: unknown
  result?: unknown
  error?: Error | undefined
}

/**
 * Error context for error hooks
 */
export interface ErrorCtx extends BaseCtx {
  error: Error
}

/**
 * Hook function type
 */
export type Hook<TCtx = BaseCtx> = (ctx: TCtx) => Promise<void> | void

/**
 * Hook phases
 */
export type HookPhase = 'before' | 'after' | 'error'

/**
 * Method names that can have hooks
 */
export type MethodName =
  | 'list'
  | 'get'
  | 'create'
  | 'update'
  | 'delete'
  | 'count'
  | 'query'
  | string

/**
 * Hook map structure
 */
export type HookMap<TCtx = BaseCtx> = {
  [P in MethodName]?: {
    before?: Hook<TCtx> | Hook<TCtx>[]
    after?: Hook<TCtx> | Hook<TCtx>[]
    error?: Hook<TCtx & { error: Error }> | Hook<TCtx & { error: Error }>[]
  }
}

/**
 * Service dependencies
 */
export type ServiceDeps = Record<string, BaseService<any, any, any, any, any>>

/**
 * Order direction for sorting
 */
export type OrderDirection = 'asc' | 'desc'

/**
 * Order by clause
 */
export interface OrderBy {
  column: string
  direction: OrderDirection
}

/**
 * Field selection for read methods.
 *
 * Use a flat array of column names to project the result to a subset of the
 * row schema. To select columns from a configured join, use `$alias.column`
 * notation (mirrors the alias-block syntax used by `where`):
 *
 * @example
 * ```ts
 * service.list({
 *   auth,
 *   select: ['id', 'name', '$teams.id', '$teams.name'],
 *   include: { teams: true },
 * })
 * ```
 *
 * Notes:
 * - The primary key and any `orderBy` columns are always included internally
 *   (and surfaced in the returned row) so cursor pagination keeps working.
 * - `select` is independent of `includeSecrets`; secrets are still controlled
 *   exclusively by the `includeSecrets` flag.
 * - `$alias.column` selects the given column from the joined table. A bare
 *   `$alias` token expands to all columns configured in the join's
 *   `remote.select`. The join must also be enabled via `include[alias] = true`.
 * - Pass the array `as const` (or rely on the `<const>` type parameter) to
 *   narrow the return type to `Pick<Row, …>`.
 */
export type SelectField = string

/**
 * Narrow a row type based on the columns chosen via `select`.
 *
 * - When `TSelect` is omitted/undefined → returns the full row.
 * - When `TSelect` is a tuple (preserved by the `<const>` type parameter on
 *   service methods) → returns `Pick<Row, …non-$tokens…>`.
 * - When `TSelect` widens to `string[]` (e.g. user did not pass `as const` and
 *   compiler couldn't preserve the tuple) → falls back to the full row.
 *
 * `$alias.column` and bare `$alias` tokens are stripped from the picked keys
 * because join columns live on the join object, not on the row itself.
 */
export type SelectedRow<
  TRow,
  TSelect extends readonly SelectField[] | undefined,
> = TSelect extends readonly SelectField[]
  ? string[] extends TSelect
    ? TRow
    : Pick<TRow, Extract<Exclude<TSelect[number], `$${string}`>, keyof TRow>>
  : TRow

/**
 * Pagination parameters
 */
export interface ListParams<
  TSelect extends readonly SelectField[] | undefined = undefined,
> {
  cursor?: string
  limit?: number
  orderBy?: OrderBy[]
  where?: Record<string, unknown>
  include?: Record<string, boolean>
  includeSecrets?: boolean
  select?: TSelect
}

/**
 * Paginated list result
 */
export interface ListResult<T> {
  result: T[]
  nextCursor?: string | undefined
  prevCursor?: string | undefined
}

/**
 * Count parameters
 */
export interface CountParams {
  where?: Record<string, unknown>
}

/**
 * Get parameters
 */
export interface GetParams<
  TSelect extends readonly SelectField[] | undefined = undefined,
> {
  include?: Record<string, boolean>
  includeSecrets?: boolean
  select?: TSelect
}

/**
 * Create parameters
 */
export interface CreateParams<
  TSelect extends readonly SelectField[] | undefined = undefined,
> {
  include?: Record<string, boolean>
  includeSecrets?: boolean
  select?: TSelect
}

/**
 * Update parameters
 */
export interface UpdateParams<
  TSelect extends readonly SelectField[] | undefined = undefined,
> {
  include?: Record<string, boolean>
  includeSecrets?: boolean
  select?: TSelect
}

/**
 * Delete parameters
 */
export interface DeleteParams {
  include?: Record<string, boolean>
}

/**
 * Query parameters for custom SQL
 */
export interface QueryParams {
  includeSecrets?: boolean
}

/**
 * Method options base interface
 */
export interface MethodOptions {
  auth: AuthContext
}

/**
 * Join definition kinds
 */
export type JoinKind = 'one' | 'many'

/**
 * Join definition
 */
export interface JoinDef {
  kind: JoinKind
  localPk: string
  through?: {
    table: string
    from: string
    to: string
  }
  remote: {
    table: string
    pk: string
    select: string[]
    alias?: string
  }
  where?: Record<string, unknown>
  schema: z.ZodTypeAny
}

/**
 * Secret field definition
 */
export interface SecretFieldDef {
  logicalName: string
  columnName: string
  keyId?: string
}

/**
 * Encryption key provider
 */
export interface KeyProvider {
  getKey(keyId: string): Promise<CryptoKey> | CryptoKey
}

/**
 * Cursor token structure (internal)
 */
export interface CursorToken {
  orderBy: OrderBy[]
  values: unknown[]
  direction: 'next' | 'prev'
}

/**
 * Base service interface
 */
export interface BaseService<
  TRow extends z.ZodTypeAny,
  TCreate extends z.ZodTypeAny,
  TUpdate extends z.ZodTypeAny,
  TJoins extends Record<string, JoinDef>,
  _TSecrets extends readonly SecretFieldDef[] | undefined,
> {
  list<const TSelect extends readonly SelectField[] | undefined = undefined>(
    params?: ListParams<TSelect> & MethodOptions,
  ): Promise<
    ListResult<
      SelectedRow<z.infer<TRow>, TSelect> &
        ComputeJoins<TJoins, ListParams['include']>
    >
  >

  get<const TSelect extends readonly SelectField[] | undefined = undefined>(
    id: string | number,
    opts?: GetParams<TSelect> & MethodOptions,
  ): Promise<
    | (SelectedRow<z.infer<TRow>, TSelect> &
        ComputeJoins<TJoins, GetParams['include']>)
    | null
  >

  create<const TSelect extends readonly SelectField[] | undefined = undefined>(
    data: z.infer<TCreate>,
    opts?: CreateParams<TSelect> & MethodOptions,
  ): Promise<
    SelectedRow<z.infer<TRow>, TSelect> &
      ComputeJoins<TJoins, CreateParams['include']>
  >

  update<const TSelect extends readonly SelectField[] | undefined = undefined>(
    id: string | number,
    data: Partial<z.infer<TUpdate>>,
    opts?: UpdateParams<TSelect> & MethodOptions,
  ): Promise<
    | (SelectedRow<z.infer<TRow>, TSelect> &
        ComputeJoins<TJoins, UpdateParams['include']>)
    | null
  >

  delete(
    id: string | number,
    opts?: DeleteParams & MethodOptions,
  ): Promise<boolean>

  count(params?: CountParams & MethodOptions): Promise<number>

  query<T = z.infer<TRow>>(
    sql: string,
    params?: QueryParams & MethodOptions,
  ): Promise<T[]>
}

/**
 * Timestamp column configuration.
 * Only configured columns will be auto-populated on create/update.
 */
export interface TimestampConfig {
  createdAt?: string
  updatedAt?: string
}

/**
 * Service options interface
 */
export interface ServiceOptions<
  TRow extends z.ZodTypeAny,
  TCreate extends z.ZodTypeAny,
  TUpdate extends z.ZodTypeAny,
  TJoins extends Record<string, JoinDef>,
  TSecrets extends readonly SecretFieldDef[] | undefined,
  TDeps extends Record<string, BaseService<any, any, any, any, any>> = Record<
    string,
    never
  >,
> {
  table: string
  db: Database
  builder?: any // SqliteBuilder from @truto/sqlite-builder
  rowSchema: TRow
  createSchema: TCreate
  updateSchema: TUpdate
  joins?: TJoins
  secrets?: TSecrets
  timestamps?: TimestampConfig
  hooks?: Partial<HookMap<BaseCtx>>
  deps?: TDeps
  primaryKey?: string | string[]
  defaultOrderBy?: OrderBy
  keyProvider?: KeyProvider
  encryptionKeys?: Record<string, string>
}

/**
 * Compute join types based on include parameter
 */
export type ComputeJoins<
  TJoins extends Record<string, JoinDef>,
  TInclude extends Record<string, boolean> | undefined,
> =
  TInclude extends Record<string, boolean>
    ? {
        [K in keyof TInclude & keyof TJoins]: TInclude[K] extends true
          ? TJoins[K]['kind'] extends 'one'
            ? z.infer<TJoins[K]['schema']> | null
            : z.infer<TJoins[K]['schema']>[]
          : never
      }
    : Record<string, never>

/**
 * Extract row type from service
 */
export type ServiceRow<T> =
  T extends BaseService<infer TRow, any, any, any, any> ? z.infer<TRow> : never

/**
 * Extract create type from service
 */
export type ServiceCreate<T> =
  T extends BaseService<any, infer TCreate, any, any, any>
    ? z.infer<TCreate>
    : never

/**
 * Extract update type from service
 */
export type ServiceUpdate<T> =
  T extends BaseService<any, any, infer TUpdate, any, any>
    ? z.infer<TUpdate>
    : never
