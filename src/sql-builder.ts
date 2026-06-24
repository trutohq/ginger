import { compileFilter, sql } from '@truto/sqlite-builder'
import { SqlBuilderError } from './errors.js'
import {
  buildExposeSelectColumns,
  buildJoinFragments,
  buildJoinSelectColumns,
  isUnsafeObjectKey,
  legacyJoinColumnAlias,
  resolveOrderByColumn,
  translateWhereForJoins,
  type ResolvedJoin,
} from './joins.js'
import type { ExposeDef, JoinDef, OrderBy } from './types.js'

/**
 * Coerce an untrusted value into a scalar that is safe to bind as a SQL
 * parameter, throwing on anything else.
 *
 * SECURITY: this is the single choke point that prevents the
 * `{ text, values }` fragment-confusion attack. `@truto/sqlite-builder`'s
 * `sql` template treats any object shaped like a pre-compiled fragment as raw
 * SQL. By forcing every dynamic value through this guard (which rejects
 * objects/arrays) before interpolation, such a value can never be spliced in
 * unescaped — it is always emitted as a bound `?` placeholder instead.
 */
export function toBindableValue(
  value: unknown,
): string | number | boolean | Date | null {
  if (value === null || value === undefined) return null
  const t = typeof value
  if (t === 'string' || t === 'number' || t === 'boolean') {
    return value as string | number | boolean
  }
  if (value instanceof Date) return value
  throw new SqlBuilderError(
    `Unsupported bound value of type "${t}". Only string, number, boolean, Date, or null may be bound as parameters.`,
    { valueType: t },
  )
}

export interface BuildSelectOptions {
  columns?: string[]
  where?: Record<string, unknown>
  /** @deprecated Use resolvedJoins instead. */
  joins?: Record<string, JoinDef>
  /** @deprecated Use resolvedJoins instead. */
  include?: Record<string, boolean>
  /** @deprecated Use resolvedJoins instead. */
  joinColumnOverrides?: Record<string, string[]>
  resolvedJoins?: ResolvedJoin[]
  expose?: readonly ExposeDef[]
  orderBy?: OrderBy[]
  limit?: number
  offset?: number
  cursorConditions?: ReturnType<typeof sql>
}

/**
 * Build a SELECT query with joins and pagination.
 */
export function buildSelect(
  table: string,
  options: BuildSelectOptions = {},
): ReturnType<typeof sql> {
  try {
    const selectColumns: ReturnType<typeof sql>[] = []

    if (options.columns && options.columns.length > 0) {
      selectColumns.push(
        ...options.columns.map((col) =>
          col === '*'
            ? sql`${sql.ident(table)}.*`
            : sql.ident(`${table}.${col}`),
        ),
      )
    } else {
      selectColumns.push(sql`${sql.ident(table)}.*`)
    }

    const resolved = options.resolvedJoins ?? []
    if (resolved.length > 0) {
      selectColumns.push(...buildJoinSelectColumns(resolved))
    } else if (options.joins && options.include) {
      // Legacy flat join path (backward compat)
      for (const [joinName, joinDef] of Object.entries(options.joins)) {
        if (options.include[joinName]) {
          const joinAlias = joinDef.remote.alias || joinName
          const cols =
            options.joinColumnOverrides?.[joinName] ?? joinDef.remote.select
          const joinColumns = cols.map(
            (col) =>
              sql`${sql.ident(`${joinDef.remote.table}.${col}`)} as ${sql.ident(legacyJoinColumnAlias(joinAlias, col))}`,
          )
          selectColumns.push(...joinColumns)
        }
      }
    }

    if (options.expose && options.expose.length > 0) {
      selectColumns.push(
        ...buildExposeSelectColumns(options.expose, resolved, table),
      )
    }

    let query = sql`SELECT ${sql.join(selectColumns, ', ')} FROM ${sql.ident(table)}`

    if (resolved.length > 0) {
      const joinFragments = buildJoinFragments(resolved)
      if (joinFragments.length > 0) {
        query = sql.join([query, ...joinFragments], ' ')
      }
    } else if (options.joins && options.include) {
      const joinFragments: ReturnType<typeof sql>[] = []
      for (const [joinName, joinDef] of Object.entries(options.joins)) {
        if (options.include[joinName]) {
          joinFragments.push(buildJoin(table, joinDef))
        }
      }
      if (joinFragments.length > 0) {
        query = sql.join([query, ...joinFragments], ' ')
      }
    }

    const finalParts: ReturnType<typeof sql>[] = [query]
    const whereFragments: ReturnType<typeof sql>[] = []

    if (options.where && Object.keys(options.where).length > 0) {
      const translated =
        resolved.length > 0
          ? translateWhereForJoins(
              options.where,
              table,
              resolved,
              options.expose,
            )
          : options.where
      whereFragments.push(compileFilter(translated as any))
    }

    if (options.cursorConditions && options.cursorConditions.text) {
      whereFragments.push(options.cursorConditions)
    }

    if (whereFragments.length > 0) {
      const whereClause = sql.join(whereFragments, ' AND ')
      finalParts.push(sql`WHERE ${whereClause}`)
    }

    if (options.orderBy && options.orderBy.length > 0) {
      const orderFragments = options.orderBy.map((order) => {
        const colRef = resolveOrderByColumn(
          order.column,
          table,
          resolved,
          options.expose,
        )
        const col = sql.ident(colRef)
        switch (order.direction.toLowerCase()) {
          case 'asc':
            return sql`${col} ASC`
          case 'desc':
            return sql`${col} DESC`
          default:
            throw new SqlBuilderError(
              `Invalid ORDER BY direction: "${order.direction}". Must be "asc" or "desc".`,
              { direction: order.direction },
            )
        }
      })
      finalParts.push(sql`ORDER BY ${sql.join(orderFragments, ', ')}`)
    }

    if (options.limit) {
      finalParts.push(sql`LIMIT ${options.limit}`)
    }

    if (options.offset) {
      finalParts.push(sql`OFFSET ${options.offset}`)
    }

    return sql.join(finalParts, ' ')
  } catch (error) {
    throw new SqlBuilderError(
      `Failed to build SELECT query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { table, options, error },
    )
  }
}

/**
 * Build a JOIN clause from the base table (legacy single-hop API).
 */
export function buildJoin(
  baseTable: string,
  joinDef: JoinDef,
): ReturnType<typeof sql> {
  const localCol = joinDef.localColumn ?? joinDef.localPk
  if (!localCol) {
    throw new SqlBuilderError('JoinDef requires localColumn or localPk', {
      joinDef,
    })
  }
  const { remote, through } = joinDef

  if (through) {
    const throughJoin = sql`LEFT JOIN ${sql.ident(through.table)} ON ${sql.ident(`${baseTable}.${localCol}`)} = ${sql.ident(`${through.table}.${through.from}`)}`
    const remoteJoin = sql`LEFT JOIN ${sql.ident(remote.table)} ON ${sql.ident(`${through.table}.${through.to}`)} = ${sql.ident(`${remote.table}.${remote.pk}`)}`

    const joinFragments = [throughJoin, remoteJoin]

    if (joinDef.where && Object.keys(joinDef.where).length > 0) {
      joinFragments.push(sql`AND ${compileFilter(joinDef.where as any)}`)
    }

    return sql.join(joinFragments, ' ')
  }

  const joinFragments = [
    sql`LEFT JOIN ${sql.ident(remote.table)} ON ${sql.ident(`${baseTable}.${localCol}`)} = ${sql.ident(`${remote.table}.${remote.pk}`)}`,
  ]

  if (joinDef.where && Object.keys(joinDef.where).length > 0) {
    joinFragments.push(sql`AND ${compileFilter(joinDef.where as any)}`)
  }

  return sql.join(joinFragments, ' ')
}

/**
 * Build an INSERT query
 */
export function buildInsert(
  table: string,
  data: Record<string, unknown>,
): ReturnType<typeof sql> {
  try {
    const columns = Object.keys(data)
    const values = Object.values(data)

    const placeholderFragments = values.map(
      (value) => sql`${toBindableValue(value)}`,
    )

    const query = sql`
      INSERT INTO ${sql.ident(table)} (${sql.ident(columns)}) 
      VALUES (${sql.join(placeholderFragments, ', ')})
    `

    return query
  } catch (error) {
    throw new SqlBuilderError(
      `Failed to build INSERT query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { table, data, error },
    )
  }
}

/**
 * Build an UPDATE query
 */
export function buildUpdate(
  table: string,
  data: Record<string, unknown>,
  where: Record<string, unknown>,
): ReturnType<typeof sql> {
  try {
    const setFragments = Object.entries(data).map(
      ([column, value]) =>
        sql`${sql.ident(column)} = ${toBindableValue(value)}`,
    )

    const whereFilter = compileFilter(where as any)

    const queryParts = [
      sql`UPDATE ${sql.ident(table)}`,
      sql`SET ${sql.join(setFragments, ', ')}`,
      sql`WHERE ${whereFilter}`,
    ]

    return sql.join(queryParts, ' ')
  } catch (error) {
    throw new SqlBuilderError(
      `Failed to build UPDATE query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { table, data, where, error },
    )
  }
}

/**
 * Build a DELETE query
 */
export function buildDelete(
  table: string,
  where: Record<string, unknown>,
): ReturnType<typeof sql> {
  try {
    const whereFilter = compileFilter(where as any)

    const queryParts = [
      sql`DELETE FROM ${sql.ident(table)}`,
      sql`WHERE ${whereFilter}`,
    ]

    return sql.join(queryParts, ' ')
  } catch (error) {
    throw new SqlBuilderError(
      `Failed to build DELETE query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { table, where, error },
    )
  }
}

export interface BuildCountOptions {
  where?: Record<string, unknown>
  resolvedJoins?: ResolvedJoin[]
  expose?: readonly ExposeDef[]
  primaryKey?: string
}

/**
 * Build a COUNT query, optionally with the same join graph as list/get.
 */
export function buildCount(
  table: string,
  whereOrOptions?: Record<string, unknown> | BuildCountOptions,
  legacyWhere?: Record<string, unknown>,
): ReturnType<typeof sql> {
  try {
    let where: Record<string, unknown> | undefined
    let resolved: ResolvedJoin[] = []
    let expose: readonly ExposeDef[] | undefined
    let primaryKey = 'id'

    if (
      whereOrOptions &&
      ('resolvedJoins' in whereOrOptions ||
        'expose' in whereOrOptions ||
        'primaryKey' in whereOrOptions)
    ) {
      const opts = whereOrOptions as BuildCountOptions
      where = opts.where
      resolved = opts.resolvedJoins ?? []
      expose = opts.expose
      primaryKey = opts.primaryKey ?? 'id'
    } else {
      where = (whereOrOptions as Record<string, unknown>) ?? legacyWhere
    }

    const countExpr =
      resolved.length > 0
        ? sql`COUNT(DISTINCT ${sql.ident(`${table}.${primaryKey}`)})`
        : sql`COUNT(*)`

    const queryParts = [
      sql`SELECT ${countExpr} as count FROM ${sql.ident(table)}`,
    ]

    if (resolved.length > 0) {
      const joinFragments = buildJoinFragments(resolved)
      if (joinFragments.length > 0) {
        queryParts.push(sql.join(joinFragments, ' '))
      }
    }

    if (where && Object.keys(where).length > 0) {
      const translated =
        resolved.length > 0
          ? translateWhereForJoins(where, table, resolved, expose)
          : where
      const whereFilter = compileFilter(translated as any)
      queryParts.push(sql`WHERE ${whereFilter}`)
    }

    return sql.join(queryParts, ' ')
  } catch (error) {
    throw new SqlBuilderError(
      `Failed to build COUNT query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { table, whereOrOptions, error },
    )
  }
}

export interface BuildSelectByIdOptions {
  columns?: string[]
  joins?: Record<string, JoinDef>
  include?: Record<string, boolean>
  joinColumnOverrides?: Record<string, string[]>
  resolvedJoins?: ResolvedJoin[]
  expose?: readonly ExposeDef[]
  where?: Record<string, unknown>
}

/**
 * Build a SELECT query for a single record by primary key
 */
export function buildSelectById(
  table: string,
  primaryKey: string | string[],
  id: string | number | Record<string, unknown>,
  options: BuildSelectByIdOptions = {},
): ReturnType<typeof sql> {
  const { where: scope, ...restOptions } = options
  const where: Record<string, unknown> = {}

  const hasActiveJoins =
    (options.resolvedJoins && options.resolvedJoins.length > 0) ||
    (options.joins &&
      options.include &&
      Object.keys(options.include).some((key) => options.include![key]))

  const hasScope = scope && Object.keys(scope).length > 0

  let baseScope: Record<string, unknown> = {}
  let joinScope: Record<string, unknown> = {}

  if (hasScope) {
    const translated = hasActiveJoins
      ? translateWhereForJoins(
          scope,
          table,
          options.resolvedJoins ?? [],
          options.expose,
        )
      : { ...scope }

    for (const [key, value] of Object.entries(translated)) {
      if (key.startsWith('$')) {
        if (!isUnsafeObjectKey(key.slice(1))) joinScope[key] = value
      } else if (!isUnsafeObjectKey(key)) {
        baseScope[key] = value
      }
    }
  }

  if (Array.isArray(primaryKey)) {
    if (typeof id !== 'object' || id === null || Array.isArray(id)) {
      throw new SqlBuilderError(
        'Composite primary key requires an object with key-value pairs',
        { primaryKey, id },
      )
    }

    if (hasActiveJoins) {
      where[`$${table}`] = { ...id, ...baseScope }
      Object.assign(where, joinScope)
    } else {
      Object.assign(where, id)
      if (hasScope) Object.assign(where, baseScope)
    }
  } else {
    if (hasActiveJoins) {
      where[`$${table}`] = { [primaryKey]: id, ...baseScope }
      Object.assign(where, joinScope)
    } else {
      where[primaryKey] = id
      if (hasScope) Object.assign(where, baseScope)
    }
  }

  return buildSelect(table, {
    ...restOptions,
    where,
    limit: 1,
  })
}

/**
 * Escape a column name using sql.ident
 */
export function escapeColumn(column: string): string {
  return sql.ident(column).text
}

/**
 * Escape a table name using sql.ident
 */
export function escapeTable(table: string): string {
  return sql.ident(table).text
}
