import type { z } from 'zod/v4'

/**
 * Cloudflare D1 Database interface
 */
export interface D1Database {
  prepare(query: string): D1PreparedStatement
  dump(): Promise<ArrayBuffer>
  batch<T = unknown>(statements: D1PreparedStatement[]): Promise<D1Result<T>[]>
  exec(query: string): Promise<D1ExecResult>
}

export interface D1PreparedStatement {
  bind(...values: unknown[]): D1PreparedStatement
  first<T = unknown>(): Promise<T | null>
  run(): Promise<D1Result>
  all<T = unknown>(): Promise<D1Result<T[]>>
  raw<T = unknown>(): Promise<T[]>
}

export interface D1Result<T = unknown> {
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

export interface D1ExecResult {
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
  db: D1Database
  deps: ServiceDeps
  method: MethodName
  params?: unknown
  data?: unknown
  result?: unknown
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
 * Pagination parameters
 */
export interface ListParams {
  cursor?: string
  limit?: number
  orderBy?: OrderBy[]
  where?: Record<string, unknown>
  include?: Record<string, boolean>
  includeSecrets?: boolean
}

/**
 * Paginated list result
 */
export interface ListResult<T> {
  result: T[]
  nextCursor?: string
  prevCursor?: string
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
export interface GetParams {
  include?: Record<string, boolean>
  includeSecrets?: boolean
}

/**
 * Create parameters
 */
export interface CreateParams {
  include?: Record<string, boolean>
  includeSecrets?: boolean
}

/**
 * Update parameters
 */
export interface UpdateParams {
  include?: Record<string, boolean>
  includeSecrets?: boolean
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
  where?: string | ((ctx: { auth: AuthContext }) => string)
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
  TSecrets extends readonly SecretFieldDef[] | undefined,
> {
  list(
    params?: ListParams & MethodOptions,
  ): Promise<
    ListResult<z.infer<TRow> & ComputeJoins<TJoins, ListParams['include']>>
  >

  get(
    id: string | number,
    opts?: GetParams & MethodOptions,
  ): Promise<
    (z.infer<TRow> & ComputeJoins<TJoins, GetParams['include']>) | null
  >

  create(
    data: z.infer<TCreate>,
    opts?: CreateParams & MethodOptions,
  ): Promise<z.infer<TRow> & ComputeJoins<TJoins, CreateParams['include']>>

  update(
    id: string | number,
    data: Partial<z.infer<TUpdate>>,
    opts?: UpdateParams & MethodOptions,
  ): Promise<
    (z.infer<TRow> & ComputeJoins<TJoins, UpdateParams['include']>) | null
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
  db: D1Database
  builder?: any // SqliteBuilder from @truto/sqlite-builder
  rowSchema: TRow
  createSchema: TCreate
  updateSchema: TUpdate
  joins?: TJoins
  secrets?: TSecrets
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
