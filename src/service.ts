import { sql } from '@truto/sqlite-builder'
import type { z } from 'zod/v4'
import { decryptSecrets, DefaultKeyProvider, encryptSecrets } from './crypto.js'
import {
  DatabaseError,
  HookError,
  NotFoundError,
  ValidationError,
} from './errors.js'
import {
  buildCursorConditions,
  createCursor,
  decodeCursor,
  encodeCursor,
  getDefaultOrderBy,
  reverseOrderBy,
  validateOrderBy,
} from './pagination.js'
import {
  buildCount,
  buildDelete,
  buildInsert,
  buildSelect,
  buildSelectById,
  buildUpdate,
} from './sql-builder.js'
import type {
  BaseCtx,
  BaseService,
  ComputeJoins,
  CountParams,
  CreateParams,
  Database,
  DeleteParams,
  GetParams,
  HookMap,
  JoinDef,
  KeyProvider,
  ListParams,
  ListResult,
  MethodOptions,
  OrderBy,
  QueryParams,
  SecretFieldDef,
  SelectedRow,
  SelectField,
  ServiceOptions,
  TimestampConfig,
  UpdateParams,
} from './types.js'

/**
 * Result of parsing a `select` array into main-row columns and per-join
 * column overrides. `'*'` for a join means "use the join's configured
 * `remote.select`" (triggered by a bare `$alias` token in the select).
 */
interface ParsedSelect {
  mainColumns: string[]
  joinColumns: Record<string, string[] | '*'>
}

/**
 * Main service class that provides type-safe data access for SQLite databases
 */
export class Service<
  TRow extends z.ZodTypeAny,
  TCreate extends z.ZodTypeAny,
  TUpdate extends z.ZodTypeAny,
  TJoins extends Record<string, JoinDef>,
  TSecrets extends readonly SecretFieldDef[] | undefined,
> implements BaseService<TRow, TCreate, TUpdate, TJoins, TSecrets>
{
  public readonly table: string
  public readonly db: Database
  public readonly rowSchema: TRow
  public readonly createSchema: TCreate
  public readonly updateSchema: TUpdate
  public readonly joins: TJoins | undefined
  public readonly secrets: TSecrets | undefined
  public readonly primaryKey: string | string[]
  public readonly defaultOrderBy: OrderBy | undefined
  public readonly keyProvider: KeyProvider
  public readonly deps: Record<string, BaseService<any, any, any, any, any>>
  public readonly timestamps: TimestampConfig | undefined

  private readonly hooks: Partial<HookMap<BaseCtx>>
  private readonly secretKeysToStrip: Set<string>

  constructor(
    options: ServiceOptions<TRow, TCreate, TUpdate, TJoins, TSecrets>,
  ) {
    this.table = options.table
    this.db = options.db
    this.rowSchema = options.rowSchema
    this.createSchema = options.createSchema
    this.updateSchema = options.updateSchema
    this.joins = options.joins
    this.secrets = options.secrets as TSecrets | undefined
    this.hooks = options.hooks || {}
    this.deps = options.deps || {}
    this.primaryKey = options.primaryKey || 'id'
    this.defaultOrderBy = options.defaultOrderBy
    this.timestamps = options.timestamps
    this.keyProvider =
      options.keyProvider ||
      new DefaultKeyProvider(options.encryptionKeys || {})

    this.secretKeysToStrip = new Set<string>()
    if (this.secrets) {
      for (const s of this.secrets) {
        this.secretKeysToStrip.add(s.columnName)
        this.secretKeysToStrip.add(s.logicalName)
      }
    }

    this.validateSecretsNotInRowSchema()
  }

  private stripSecretKeys(
    row: Record<string, unknown>,
  ): Record<string, unknown> {
    if (this.secretKeysToStrip.size === 0) return row
    const cleaned = { ...row }
    for (const key of this.secretKeysToStrip) {
      delete cleaned[key]
    }
    return cleaned
  }

  private extractSecretFields(
    row: Record<string, unknown>,
  ): Record<string, unknown> {
    if (!this.secrets) return {}
    const fields: Record<string, unknown> = {}
    for (const s of this.secrets) {
      if (row[s.logicalName] !== undefined) {
        fields[s.logicalName] = row[s.logicalName]
      }
    }
    return fields
  }

  /**
   * Parse a `select` array into main-row columns and per-join column overrides.
   *
   * Tokens:
   * - `'col'`            → main row column
   * - `'$alias.col'`     → column from the join named `alias`
   * - `'$alias'`         → all configured columns for the join named `alias`
   *
   * Validates that:
   * - Main columns exist in the row schema (when shape is available).
   * - `$alias` references a configured join AND the join is enabled via
   *   `include[alias] = true`. We require explicit include so the return
   *   type (which is keyed off `include`) stays accurate.
   */
  private parseSelect(
    select: readonly SelectField[] | undefined,
    include: Record<string, boolean>,
  ): ParsedSelect | undefined {
    if (!select || select.length === 0) return undefined

    const mainSet = new Set<string>()
    const joinAcc: Record<string, Set<string> | '*'> = {}
    const allowedMain = this.getSchemaColumns()
    const wildcard = allowedMain.length === 1 && allowedMain[0] === '*'
    const joinNames = this.joins ? new Set(Object.keys(this.joins)) : new Set()

    for (const raw of select) {
      if (typeof raw !== 'string' || raw.length === 0) {
        throw new ValidationError(
          `Invalid select token: ${JSON.stringify(raw)}. Expected a non-empty string.`,
        )
      }

      if (raw.startsWith('$')) {
        const body = raw.slice(1)
        const dot = body.indexOf('.')
        const alias = dot === -1 ? body : body.slice(0, dot)
        const column = dot === -1 ? undefined : body.slice(dot + 1)

        if (!alias) {
          throw new ValidationError(
            `Invalid select token "${raw}": missing alias name after '$'.`,
          )
        }
        if (!joinNames.has(alias)) {
          throw new ValidationError(
            `Unknown join alias "${alias}" in select token "${raw}". Configured joins: ${
              Array.from(joinNames).join(', ') || '<none>'
            }.`,
          )
        }
        if (!include[alias]) {
          throw new ValidationError(
            `select token "${raw}" requires include.${alias} = true.`,
          )
        }
        if (column === undefined) {
          joinAcc[alias] = '*'
        } else if (!column) {
          throw new ValidationError(
            `Invalid select token "${raw}": missing column name after '$${alias}.'.`,
          )
        } else {
          const existing = joinAcc[alias]
          if (existing === '*') {
            // already wildcard; keep wildcard
            continue
          }
          if (!existing) {
            joinAcc[alias] = new Set([column])
          } else {
            existing.add(column)
          }
        }
        continue
      }

      if (!wildcard && !allowedMain.includes(raw)) {
        throw new ValidationError(
          `Unknown column "${raw}" in select. Allowed columns: ${allowedMain.join(
            ', ',
          )}.`,
        )
      }
      mainSet.add(raw)
    }

    const joinColumns: Record<string, string[] | '*'> = {}
    for (const [alias, value] of Object.entries(joinAcc)) {
      joinColumns[alias] = value === '*' ? '*' : Array.from(value)
    }

    return {
      mainColumns: Array.from(mainSet),
      joinColumns,
    }
  }

  /**
   * Compute the effective list of columns to SELECT from the main table.
   *
   * The user's main-row select is augmented with the primary key columns and
   * any `orderBy` columns so cursor pagination keeps working and so the
   * canonical record is always identifiable. These extras are kept in the
   * returned row (per the design choice "silently include and keep").
   */
  private resolveMainColumns(
    parsed: ParsedSelect | undefined,
    orderBy: OrderBy[],
    includeSecrets: boolean,
  ): string[] | undefined {
    if (!parsed) return this.getSelectColumns(includeSecrets)

    const out = new Set<string>(parsed.mainColumns)

    for (const pk of Array.isArray(this.primaryKey)
      ? this.primaryKey
      : [this.primaryKey]) {
      out.add(pk)
    }
    for (const order of orderBy) {
      out.add(order.column)
    }

    if (includeSecrets && this.secrets) {
      for (const s of this.secrets) {
        out.add(s.columnName)
      }
    }

    return Array.from(out)
  }

  /**
   * Compute the effective per-join column override map.
   *
   * For each enabled join: if the user asked for specific columns via
   * `$alias.col`, use those; if they used a bare `$alias`, fall back to the
   * join's configured `remote.select`. Joins with no override aren't returned
   * (so `buildSelect` keeps using the configured select for them).
   *
   * The join's PK is silently included so `processJoinedRows` can detect the
   * presence of related data.
   */
  private resolveJoinOverrides(
    parsed: ParsedSelect | undefined,
    include: Record<string, boolean>,
  ): Record<string, string[]> | undefined {
    if (!parsed || !this.joins) return undefined

    const overrides: Record<string, string[]> = {}
    for (const [alias, value] of Object.entries(parsed.joinColumns)) {
      if (!include[alias]) continue
      const def = this.joins[alias]
      if (!def) continue
      if (value === '*') continue // use configured remote.select
      const cols = new Set<string>(value)
      cols.add(def.remote.pk)
      overrides[alias] = Array.from(cols)
    }
    return Object.keys(overrides).length > 0 ? overrides : undefined
  }

  private validateSecretsNotInRowSchema(): void {
    if (!this.secrets) return

    const shape = (this.rowSchema as any).shape
    if (!shape || typeof shape !== 'object') return

    const schemaKeys = new Set(Object.keys(shape))

    for (const secret of this.secrets) {
      if (schemaKeys.has(secret.columnName)) {
        throw new ValidationError(
          `Secret column '${secret.columnName}' (columnName) should not be in rowSchema — ` +
            `secret fields are managed separately via the secrets config. Remove it from rowSchema.`,
        )
      }
      if (schemaKeys.has(secret.logicalName)) {
        throw new ValidationError(
          `Secret field '${secret.logicalName}' (logicalName) should not be in rowSchema — ` +
            `when includeSecrets is true, it is automatically added to the result after decryption. ` +
            `Remove it from rowSchema.`,
        )
      }
    }
  }

  /**
   * List records with cursor-based pagination
   */
  async list<
    const TSelect extends readonly SelectField[] | undefined = undefined,
  >(
    params: ListParams<TSelect> & MethodOptions = { auth: {} },
  ): Promise<
    ListResult<
      SelectedRow<z.infer<TRow>, TSelect> &
        ComputeJoins<TJoins, ListParams['include']>
    >
  > {
    const ctx: BaseCtx = {
      auth: params.auth,
      db: this.db,
      deps: this.deps,
      method: 'list',
      params,
    }

    try {
      await this.runHooks('before', 'list', ctx)

      const {
        cursor,
        limit = 50,
        orderBy: paramOrderBy,
        where = {},
        include = {},
        includeSecrets = false,
        select,
      } = params

      // Validate limit
      if (limit > 1000) {
        throw new ValidationError('Limit cannot exceed 1000')
      }

      // Determine order by
      let orderBy =
        paramOrderBy || getDefaultOrderBy(this.primaryKey, this.defaultOrderBy)

      // Validate order by columns (skip if schema shape is unavailable)
      const allowedColumns = this.getAllowedColumns()
      if (!allowedColumns.includes('*')) {
        validateOrderBy(orderBy, allowedColumns)
      }

      // Handle cursor pagination
      let cursorConditions: ReturnType<typeof sql> | undefined
      let actualLimit = limit + 1 // Fetch one extra to determine if there's a next page

      if (cursor) {
        try {
          const cursorToken = decodeCursor(cursor)

          // Use cursor's orderBy if present
          if (cursorToken.orderBy.length > 0) {
            // Re-validate cursor columns against allowlist to prevent bypass
            if (!allowedColumns.includes('*')) {
              validateOrderBy(cursorToken.orderBy, allowedColumns)
            }
            orderBy =
              cursorToken.direction === 'prev'
                ? reverseOrderBy(cursorToken.orderBy)
                : cursorToken.orderBy
          }

          cursorConditions = buildCursorConditions(cursorToken, this.table)
        } catch (error) {
          throw new ValidationError(
            `Invalid cursor: ${error instanceof Error ? error.message : 'Unknown error'}`,
          )
        }
      }

      const parsedSelect = this.parseSelect(select, include)
      const columns = this.resolveMainColumns(
        parsedSelect,
        orderBy,
        includeSecrets,
      )
      const joinColumnOverrides = this.resolveJoinOverrides(
        parsedSelect,
        include,
      )

      // Build and execute query
      const { text: query, values: sqlParams } = buildSelect(this.table, {
        ...(columns ? { columns } : {}),
        where,
        ...(this.joins ? { joins: this.joins } : {}),
        include,
        ...(joinColumnOverrides ? { joinColumnOverrides } : {}),
        orderBy,
        limit: actualLimit,
        ...(cursorConditions ? { cursorConditions } : {}),
      })

      const stmt = this.db.prepare(query)
      const result = await stmt.bind(...sqlParams).all()

      if (!result.success) {
        throw new DatabaseError('Failed to execute list query')
      }

      let rows = (result.results as Record<string, unknown>[]) || []

      // Process results
      const hasNextPage = rows.length > limit
      if (hasNextPage) {
        rows = rows.slice(0, limit) // Remove the extra row
      }

      // Decrypt secrets if requested
      if (includeSecrets && this.secrets) {
        rows = await Promise.all(rows.map((row) => this.decryptRowSecrets(row)))
      }

      // Process joins (honour per-call join column overrides)
      const processedRows = this.processJoinedRows(
        rows,
        include,
        joinColumnOverrides,
      )

      // Validate rows. When `select` is provided we project to the requested
      // subset and skip full schema validation (Zod would fail on missing
      // required fields). PK + orderBy columns are always present because
      // resolveMainColumns added them.
      const validatedRows = processedRows.map((row) => {
        const r = row as Record<string, unknown>
        if (parsedSelect) {
          return this.projectRow(r, include, includeSecrets)
        }
        const validated = this.rowSchema.parse(this.stripSecretKeys(r))
        return includeSecrets
          ? {
              ...(validated as Record<string, unknown>),
              ...this.extractSecretFields(r),
            }
          : validated
      })

      // Generate cursors
      let nextCursor: string | undefined
      let prevCursor: string | undefined

      if (validatedRows.length > 0) {
        if (hasNextPage) {
          const lastRow = validatedRows[validatedRows.length - 1]!
          nextCursor = encodeCursor(createCursor(lastRow, orderBy, 'next'))
        }

        if (cursor) {
          const firstRow = validatedRows[0]!
          prevCursor = encodeCursor(createCursor(firstRow, orderBy, 'prev'))
        }
      }

      const listResult: ListResult<any> = {
        result: validatedRows,
        nextCursor,
        prevCursor,
      }

      ctx.result = listResult
      await this.runHooks('after', 'list', ctx)

      return listResult
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'list', ctx)
      throw error
    }
  }

  /**
   * Get a single record by ID
   */
  async get<
    const TSelect extends readonly SelectField[] | undefined = undefined,
  >(
    id: string | number,
    opts: GetParams<TSelect> & MethodOptions = { auth: {} },
  ): Promise<
    | (SelectedRow<z.infer<TRow>, TSelect> &
        ComputeJoins<TJoins, GetParams['include']>)
    | null
  > {
    const ctx: BaseCtx = {
      auth: opts.auth,
      db: this.db,
      deps: this.deps,
      method: 'get',
      params: { id, ...opts },
    }

    try {
      await this.runHooks('before', 'get', ctx)

      const { include = {}, includeSecrets = false, select } = opts

      const parsedSelect = this.parseSelect(select, include)
      const columns = this.resolveMainColumns(parsedSelect, [], includeSecrets)
      const joinColumnOverrides = this.resolveJoinOverrides(
        parsedSelect,
        include,
      )

      // Build and execute query
      const { text: query, values: params } = buildSelectById(
        this.table,
        this.primaryKey,
        id,
        {
          ...(columns ? { columns } : {}),
          ...(this.joins ? { joins: this.joins } : {}),
          include,
          ...(joinColumnOverrides ? { joinColumnOverrides } : {}),
        },
      )

      const stmt = this.db.prepare(query)
      const result = await stmt.bind(...params).first()

      if (!result) {
        ctx.result = null
        await this.runHooks('after', 'get', ctx)
        return null
      }

      let row = result as Record<string, unknown>

      // Decrypt secrets if requested
      if (includeSecrets && this.secrets) {
        row = await this.decryptRowSecrets(row)
      }

      // Process joins (honour per-call join column overrides)
      let processedRow = this.processJoinedRows(
        [row],
        include,
        joinColumnOverrides,
      )[0]!

      // Handle one-to-many joins with separate queries
      if (this.joins && Object.keys(include).length > 0) {
        processedRow = await this.fetchOneToManyJoins(
          processedRow,
          include,
          id,
          joinColumnOverrides,
        )
      }

      // When `select` was provided, project to the requested subset rather
      // than running Zod validation (which would fail on missing required
      // fields). Otherwise validate the full row through the schema.
      let validatedRow: Record<string, unknown>
      if (parsedSelect) {
        // processedRow already has aliased join keys removed and join data
        // attached via processJoinedRows + fetchOneToManyJoins.
        validatedRow = this.projectRow(processedRow, include, includeSecrets)
      } else {
        const baseRow = this.rowSchema.parse(
          this.stripSecretKeys(row),
        ) as Record<string, unknown>
        validatedRow = {
          ...baseRow,
          ...(includeSecrets ? this.extractSecretFields(row) : {}),
          ...this.extractJoinData(processedRow, include),
        }
      }

      ctx.result = validatedRow
      await this.runHooks('after', 'get', ctx)

      return validatedRow as any
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'get', ctx)
      throw error
    }
  }

  /**
   * Create a new record
   */
  async create<
    const TSelect extends readonly SelectField[] | undefined = undefined,
  >(
    data: z.infer<TCreate>,
    opts: CreateParams<TSelect> & MethodOptions = { auth: {} },
  ): Promise<
    SelectedRow<z.infer<TRow>, TSelect> &
      ComputeJoins<TJoins, CreateParams['include']>
  > {
    const ctx: BaseCtx = {
      auth: opts.auth,
      db: this.db,
      deps: this.deps,
      method: 'create',
      params: opts,
      data,
    }

    try {
      await this.runHooks('before', 'create', ctx)

      const { include = {}, includeSecrets = false, select } = opts

      // Validate input data
      const validatedData = this.createSchema.parse(data) as Record<
        string,
        unknown
      >

      // Add timestamp fields if configured
      let dataWithTimestamps: Record<string, unknown> = { ...validatedData }
      if (this.timestamps) {
        const now = new Date().toISOString()
        if (this.timestamps.createdAt) {
          dataWithTimestamps[this.timestamps.createdAt] = now
        }
        if (this.timestamps.updatedAt) {
          dataWithTimestamps[this.timestamps.updatedAt] = now
        }
      }

      // Encrypt secrets
      let processedData = dataWithTimestamps
      if (this.secrets) {
        processedData = await this.encryptDataSecrets(processedData)
      }

      // Build insert with RETURNING to get the primary key directly
      const { text: insertQuery, values: params } = buildInsert(
        this.table,
        processedData,
      )
      const pkColumns = Array.isArray(this.primaryKey)
        ? this.primaryKey
        : [this.primaryKey]
      const returningClause = pkColumns.map((c) => `"${c}"`).join(', ')
      const query = `${insertQuery} RETURNING ${returningClause}`

      const stmt = this.db.prepare(query)
      const inserted = (await stmt.bind(...params).first()) as Record<
        string,
        unknown
      > | null

      if (!inserted) {
        throw new DatabaseError('Failed to create record')
      }

      const insertId: string | number | Record<string, unknown> = Array.isArray(
        this.primaryKey,
      )
        ? Object.fromEntries(this.primaryKey.map((k) => [k, inserted[k]]))
        : (inserted[this.primaryKey] as string | number)

      const createdRecord = await this.get(
        insertId as string | number,
        {
          include,
          includeSecrets,
          ...(select ? { select } : {}),
          auth: opts.auth,
        } as GetParams<TSelect> & MethodOptions,
      )

      if (!createdRecord) {
        throw new DatabaseError('Failed to retrieve created record')
      }

      ctx.result = createdRecord
      await this.runHooks('after', 'create', ctx)

      return createdRecord as any
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'create', ctx)
      throw error
    }
  }

  /**
   * Update a record by ID
   */
  async update<
    const TSelect extends readonly SelectField[] | undefined = undefined,
  >(
    id: string | number,
    data: Partial<z.infer<TUpdate>>,
    opts: UpdateParams<TSelect> & MethodOptions = { auth: {} },
  ): Promise<
    | (SelectedRow<z.infer<TRow>, TSelect> &
        ComputeJoins<TJoins, UpdateParams['include']>)
    | null
  > {
    const ctx: BaseCtx = {
      auth: opts.auth,
      db: this.db,
      deps: this.deps,
      method: 'update',
      params: { id, ...opts },
      data,
    }

    try {
      await this.runHooks('before', 'update', ctx)

      const { include = {}, includeSecrets = false, select } = opts

      // Check if record exists
      const existingRecord = await this.get(id, { auth: opts.auth })
      if (!existingRecord) {
        throw new NotFoundError(this.table, id)
      }

      // Validate input data
      const validatedData = this.updateSchema.parse(data) as Record<
        string,
        unknown
      >

      // Add updated timestamp if configured
      let dataWithTimestamp: Record<string, unknown> = { ...validatedData }
      if (this.timestamps?.updatedAt) {
        dataWithTimestamp[this.timestamps.updatedAt] = new Date().toISOString()
      }

      // Encrypt secrets
      let processedData = dataWithTimestamp
      if (this.secrets) {
        processedData = await this.encryptDataSecrets(processedData)
      }

      // Build where clause for update
      const where: Record<string, unknown> = {}
      if (Array.isArray(this.primaryKey)) {
        if (typeof id !== 'object') {
          throw new ValidationError('Composite primary key requires an object')
        }
        Object.assign(where, id)
      } else {
        where[this.primaryKey] = id
      }

      // Build and execute update
      const { text: query, values: params } = buildUpdate(
        this.table,
        processedData,
        where,
      )
      const stmt = this.db.prepare(query)
      const result = await stmt.bind(...params).run()

      if (!result.success) {
        throw new DatabaseError('Failed to update record')
      }

      // Get the updated record
      const updatedRecord = await this.get(id, {
        include,
        includeSecrets,
        ...(select ? { select } : {}),
        auth: opts.auth,
      } as GetParams<TSelect> & MethodOptions)

      ctx.result = updatedRecord
      await this.runHooks('after', 'update', ctx)

      return updatedRecord as any
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'update', ctx)
      throw error
    }
  }

  /**
   * Delete a record by ID
   */
  async delete(
    id: string | number,
    opts: DeleteParams & MethodOptions = { auth: {} },
  ): Promise<boolean> {
    const ctx: BaseCtx = {
      auth: opts.auth,
      db: this.db,
      deps: this.deps,
      method: 'delete',
      params: { id, ...opts },
    }

    try {
      await this.runHooks('before', 'delete', ctx)

      // Check if record exists
      const existingRecord = await this.get(id, { auth: opts.auth })
      if (!existingRecord) {
        throw new NotFoundError(this.table, id)
      }

      // Build where clause for delete
      const where: Record<string, unknown> = {}
      if (Array.isArray(this.primaryKey)) {
        if (typeof id !== 'object') {
          throw new ValidationError('Composite primary key requires an object')
        }
        Object.assign(where, id)
      } else {
        where[this.primaryKey] = id
      }

      // Build and execute delete
      const { text: query, values: params } = buildDelete(this.table, where)
      const stmt = this.db.prepare(query)
      const result = await stmt.bind(...params).run()

      if (!result.success) {
        throw new DatabaseError('Failed to delete record')
      }

      const deleted = result.meta.changes > 0

      ctx.result = deleted
      await this.runHooks('after', 'delete', ctx)

      return deleted
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'delete', ctx)
      throw error
    }
  }

  /**
   * Count records matching criteria
   */
  async count(
    params: CountParams & MethodOptions = { auth: {} },
  ): Promise<number> {
    const ctx: BaseCtx = {
      auth: params.auth,
      db: this.db,
      deps: this.deps,
      method: 'count',
      params,
    }

    try {
      await this.runHooks('before', 'count', ctx)

      const { where = {} } = params

      // Build and execute count query
      const { text: query, values: sqlParams } = buildCount(this.table, where)
      const stmt = this.db.prepare(query)
      const result = await stmt.bind(...sqlParams).first()

      if (!result) {
        throw new DatabaseError('Failed to execute count query')
      }

      const count = (result as { count: number }).count

      ctx.result = count
      await this.runHooks('after', 'count', ctx)

      return count
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'count', ctx)
      throw error
    }
  }

  /**
   * Execute custom SQL query
   */
  async query<T = z.infer<TRow>>(
    query: string,
    opts: QueryParams & MethodOptions = { auth: {} },
    ...sqlParams: unknown[]
  ): Promise<T[]> {
    const ctx: BaseCtx = {
      auth: opts.auth,
      db: this.db,
      deps: this.deps,
      method: 'query',
      params: opts,
    }

    try {
      await this.runHooks('before', 'query', ctx)

      const { includeSecrets = false } = opts

      // Execute the query
      const stmt = this.db.prepare(query)
      const result =
        sqlParams.length > 0
          ? await stmt.bind(...sqlParams).all()
          : await stmt.all()

      if (!result.success) {
        throw new DatabaseError('Failed to execute custom query')
      }

      let rows = (result.results as Record<string, unknown>[]) || []

      // Decrypt secrets if requested
      if (includeSecrets && this.secrets) {
        rows = await Promise.all(rows.map((row) => this.decryptRowSecrets(row)))
      }

      ctx.result = rows
      await this.runHooks('after', 'query', ctx)

      return rows as T[]
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'query', ctx)
      throw error
    }
  }

  /**
   * Run hooks with context for a specific method
   */
  protected async runHooks<TMethod extends string>(
    phase: 'before' | 'after' | 'error',
    method: TMethod,
    ctx: BaseCtx,
  ): Promise<void> {
    const methodHooks = this.hooks[method]
    if (!methodHooks) return

    const hooks = methodHooks[phase]
    if (!hooks) return

    const hooksArray = Array.isArray(hooks) ? hooks : [hooks]

    for (const hook of hooksArray) {
      try {
        await hook(ctx as any)
      } catch (error) {
        if (phase !== 'error') {
          throw new HookError(phase, method, error as Error)
        }
        // If an error hook throws, we just log it and continue
        console.error(`Error in error hook for ${method}:`, error)
      }
    }
  }

  /**
   * Get columns to select based on secrets configuration
   */
  private getSelectColumns(includeSecrets: boolean): string[] {
    const schemaColumns = this.getSchemaColumns()

    if (!this.secrets) {
      return schemaColumns
    }

    const secretColumnNames = new Set(this.secrets.map((s) => s.columnName))

    if (includeSecrets) {
      const columns = [...schemaColumns]
      for (const col of secretColumnNames) {
        if (!columns.includes(col)) {
          columns.push(col)
        }
      }
      return columns
    }

    return schemaColumns.filter((col) => !secretColumnNames.has(col))
  }

  /**
   * Get allowed columns for ordering
   */
  private getAllowedColumns(): string[] {
    return this.getSchemaColumns()
  }

  /**
   * Derive column names from the row schema shape.
   * Falls back to wildcard if the schema doesn't expose a shape.
   */
  private getSchemaColumns(): string[] {
    const shape = (this.rowSchema as any).shape
    if (shape && typeof shape === 'object') {
      return Object.keys(shape)
    }
    return ['*']
  }

  /**
   * Encrypt secrets in data
   */
  private async encryptDataSecrets(
    data: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    if (!this.secrets) return data
    return encryptSecrets(data, this.secrets, this.keyProvider)
  }

  /**
   * Decrypt secrets in a row
   */
  private async decryptRowSecrets(
    row: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    if (!this.secrets) return row
    return decryptSecrets(row, this.secrets, this.keyProvider)
  }

  /**
   * Project a processed row down to the caller-requested subset.
   *
   * Callers must pass a row that has already been through
   * `processJoinedRows` (and, for `get()`, `fetchOneToManyJoins`). That means
   * it contains exactly:
   *   parsed.mainColumns + primary key + orderBy + secret columns
   *   + nested join data under each join key (object | array | null).
   *
   * Here we only need to:
   *   1. Drop the raw secret column names (e.g. `api_key_encrypted`) — those
   *      are surfaced under the logicalName when `includeSecrets` is true.
   *   2. Merge in decrypted logical secrets when `includeSecrets` is true.
   *
   * Zod validation is intentionally skipped — selecting a subset would
   * otherwise fail required-field checks.
   *
   * The `include` parameter is currently unused but kept on the signature
   * for symmetry with other helpers and to make future per-join projection
   * easy to thread in.
   */
  private projectRow(
    row: Record<string, unknown>,
    _include: Record<string, boolean>,
    includeSecrets: boolean,
  ): Record<string, unknown> {
    const out: Record<string, unknown> = { ...row }

    if (this.secrets) {
      for (const s of this.secrets) {
        delete out[s.columnName]
      }
    }

    if (includeSecrets) {
      Object.assign(out, this.extractSecretFields(row))
    }

    return out
  }

  /**
   * Process joined rows by extracting join data
   */
  private processJoinedRows(
    rows: Record<string, unknown>[],
    include: Record<string, boolean>,
    joinColumnOverrides?: Record<string, string[]>,
  ): Record<string, unknown>[] {
    if (!this.joins || Object.keys(include).length === 0) {
      return rows
    }

    const joins = this.joins!

    return rows.map((row) => {
      const processedRow = { ...row }

      for (const [joinName, joinDef] of Object.entries(joins)) {
        if (!include[joinName]) continue

        const joinAlias = joinDef.remote.alias || joinName
        const joinData: Record<string, unknown> = {}
        const keysToRemove: string[] = []

        const effectiveCols =
          joinColumnOverrides?.[joinName] ?? joinDef.remote.select
        for (const column of effectiveCols) {
          const aliasedKey = `${joinAlias}_${column}`
          if (aliasedKey in row) {
            joinData[column] = row[aliasedKey]
            keysToRemove.push(aliasedKey)
          }
        }

        // Remove aliased keys from main row
        for (const key of keysToRemove) {
          delete processedRow[key]
        }

        // Add join data based on join kind
        if (joinDef.kind === 'one') {
          // For one-to-one, add as object or null
          const hasData =
            Object.keys(joinData).length > 0 &&
            Object.values(joinData).some(
              (value) => value !== null && value !== undefined,
            )
          processedRow[joinName] = hasData ? joinData : null
        } else {
          // For one-to-many, this will be overridden by fetchOneToManyJoins
          // But set a default empty array for now
          processedRow[joinName] = []
        }
      }

      return processedRow
    })
  }

  /**
   * Extract only join data from processed row
   */
  private extractJoinData(
    processedRow: Record<string, unknown>,
    include: Record<string, boolean>,
  ): Record<string, unknown> {
    if (!this.joins) return {}

    const joinData: Record<string, unknown> = {}

    for (const joinName of Object.keys(this.joins)) {
      if (include[joinName] && joinName in processedRow) {
        joinData[joinName] = processedRow[joinName]
      }
    }

    return joinData
  }

  /**
   * Fetch one-to-many joins using separate queries
   */
  private async fetchOneToManyJoins(
    processedRow: Record<string, unknown>,
    include: Record<string, boolean>,
    mainRecordId: string | number,
    joinColumnOverrides?: Record<string, string[]>,
  ): Promise<Record<string, unknown>> {
    if (!this.joins) return processedRow

    const result = { ...processedRow }

    for (const [joinName, joinDef] of Object.entries(this.joins)) {
      if (!include[joinName] || joinDef.kind !== 'many') continue

      let relatedRecords: any[] = []

      const effectiveCols =
        joinColumnOverrides?.[joinName] ?? joinDef.remote.select

      if (joinDef.through) {
        const selectColumns = effectiveCols.map(
          (col) => sql`${sql.ident(`${joinDef.remote.table}.${col}`)}`,
        )
        const query = sql`SELECT ${sql.join(selectColumns, ', ')}
            FROM ${sql.ident(joinDef.remote.table)}
            INNER JOIN ${sql.ident(joinDef.through.table)}
              ON ${sql.ident(`${joinDef.remote.table}.${joinDef.remote.pk}`)} = ${sql.ident(`${joinDef.through.table}.${joinDef.through.to}`)}
            WHERE ${sql.ident(`${joinDef.through.table}.${joinDef.through.from}`)} = ${mainRecordId}`

        const stmt = this.db.prepare(query.text)
        const queryResult = await stmt.bind(...query.values).all()

        if (queryResult.success) {
          relatedRecords = queryResult.results as any[]
        }
      } else {
        const selectColumns = effectiveCols.map((col) => sql`${sql.ident(col)}`)
        const query = sql`SELECT ${sql.join(selectColumns, ', ')}
            FROM ${sql.ident(joinDef.remote.table)}
            WHERE ${sql.ident(joinDef.remote.pk)} = ${mainRecordId}`

        const stmt = this.db.prepare(query.text)
        const queryResult = await stmt.bind(...query.values).all()

        if (queryResult.success) {
          relatedRecords = queryResult.results as any[]
        }
      }

      result[joinName] = relatedRecords || []
    }

    return result
  }
}
