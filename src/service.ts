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
  ServiceOptions,
  TimestampConfig,
  UpdateParams,
} from './types.js'

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
  }

  /**
   * List records with cursor-based pagination
   */
  async list(
    params: ListParams & MethodOptions = { auth: {} },
  ): Promise<
    ListResult<z.infer<TRow> & ComputeJoins<TJoins, ListParams['include']>>
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

      // Get columns to select (excluding secret columns if not requested)
      const columns = this.getSelectColumns(includeSecrets)

      // Build and execute query
      const { text: query, values: sqlParams } = buildSelect(this.table, {
        columns,
        where,
        ...(this.joins ? { joins: this.joins } : {}),
        include,
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

      // Process joins
      const processedRows = this.processJoinedRows(rows, include)

      // Validate rows
      const validatedRows = processedRows.map((row) =>
        this.rowSchema.parse(row),
      )

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
  async get(
    id: string | number,
    opts: GetParams & MethodOptions = { auth: {} },
  ): Promise<
    (z.infer<TRow> & ComputeJoins<TJoins, GetParams['include']>) | null
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

      const { include = {}, includeSecrets = false } = opts

      // Get columns to select
      const columns = this.getSelectColumns(includeSecrets)

      // Build and execute query
      const { text: query, values: params } = buildSelectById(
        this.table,
        this.primaryKey,
        id,
        {
          columns,
          ...(this.joins ? { joins: this.joins } : {}),
          include,
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

      // Process joins
      let processedRow = this.processJoinedRows([row], include)[0]!

      // Handle one-to-many joins with separate queries
      if (this.joins && Object.keys(include).length > 0) {
        processedRow = await this.fetchOneToManyJoins(processedRow, include, id)
      }

      // Validate row (but preserve join data)
      const baseRow = this.rowSchema.parse(row) as Record<string, unknown>
      const validatedRow = {
        ...baseRow,
        ...this.extractJoinData(processedRow, include),
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
  async create(
    data: z.infer<TCreate>,
    opts: CreateParams & MethodOptions = { auth: {} },
  ): Promise<z.infer<TRow> & ComputeJoins<TJoins, CreateParams['include']>> {
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

      const { include = {}, includeSecrets = false } = opts

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

      // Build and execute insert
      const { text: query, values: params } = buildInsert(
        this.table,
        processedData,
      )
      const stmt = this.db.prepare(query)
      const result = await stmt.bind(...params).run()

      if (!result.success) {
        throw new DatabaseError('Failed to create record')
      }

      // Get the created record
      const insertId = result.meta.last_row_id
      const createdRecord = await this.get(insertId, {
        include,
        includeSecrets,
        auth: opts.auth,
      })

      if (!createdRecord) {
        throw new DatabaseError('Failed to retrieve created record')
      }

      ctx.result = createdRecord
      await this.runHooks('after', 'create', ctx)

      return createdRecord
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'create', ctx)
      throw error
    }
  }

  /**
   * Update a record by ID
   */
  async update(
    id: string | number,
    data: Partial<z.infer<TUpdate>>,
    opts: UpdateParams & MethodOptions = { auth: {} },
  ): Promise<
    (z.infer<TRow> & ComputeJoins<TJoins, UpdateParams['include']>) | null
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

      const { include = {}, includeSecrets = false } = opts

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
        auth: opts.auth,
      })

      ctx.result = updatedRecord
      await this.runHooks('after', 'update', ctx)

      return updatedRecord
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
   * Process joined rows by extracting join data
   */
  private processJoinedRows(
    rows: Record<string, unknown>[],
    include: Record<string, boolean>,
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

        // Extract join columns
        for (const column of joinDef.remote.select) {
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
  ): Promise<Record<string, unknown>> {
    if (!this.joins) return processedRow

    const result = { ...processedRow }

    for (const [joinName, joinDef] of Object.entries(this.joins)) {
      if (!include[joinName] || joinDef.kind !== 'many') continue

      let relatedRecords: any[] = []

      if (joinDef.through) {
        const selectColumns = joinDef.remote.select.map(
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
        const selectColumns = joinDef.remote.select.map(
          (col) => sql`${sql.ident(col)}`,
        )
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
