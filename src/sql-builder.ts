import { compileFilter, sql } from '@truto/sqlite-builder'
import { SqlBuilderError } from './errors.js'
import type { JoinDef, OrderBy } from './types.js'

/**
 * Build a SELECT query with joins and pagination.
 *
 * `joinColumnOverrides[joinName]` overrides the columns pulled from that
 * join's remote table for this query only (instead of using the join's
 * configured `remote.select`). The base service uses this to honour the
 * caller's `select: ['$alias.col', ...]` request.
 */
export function buildSelect(
  table: string,
  options: {
    columns?: string[]
    where?: Record<string, unknown>
    joins?: Record<string, JoinDef>
    include?: Record<string, boolean>
    joinColumnOverrides?: Record<string, string[]>
    orderBy?: OrderBy[]
    limit?: number
    offset?: number
    cursorConditions?: ReturnType<typeof sql>
  } = {},
): ReturnType<typeof sql> {
  try {
    // Build column list
    const selectColumns: ReturnType<typeof sql>[] = []

    if (options.columns && options.columns.length > 0) {
      selectColumns.push(
        ...options.columns.map((col) => sql.ident(`${table}.${col}`)),
      )
    } else {
      selectColumns.push(sql`${sql.ident(table)}.*`)
    }

    if (options.joins && options.include) {
      for (const [joinName, joinDef] of Object.entries(options.joins)) {
        if (options.include[joinName]) {
          const joinAlias = joinDef.remote.alias || joinName
          const cols =
            options.joinColumnOverrides?.[joinName] ?? joinDef.remote.select
          const joinColumns = cols.map(
            (col) =>
              sql`${sql.ident(`${joinDef.remote.table}.${col}`)} as ${sql.ident(`${joinAlias}_${col}`)}`,
          )
          selectColumns.push(...joinColumns)
        }
      }
    }

    // Start building the query using sql.join for the column fragments
    let query = sql`SELECT ${sql.join(selectColumns, ', ')} FROM ${sql.ident(table)}`

    // Add joins
    if (options.joins && options.include) {
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

    // Build final query parts
    const finalParts: ReturnType<typeof sql>[] = [query]

    // Build WHERE conditions
    const whereFragments: ReturnType<typeof sql>[] = []

    // Regular WHERE conditions
    if (options.where && Object.keys(options.where).length > 0) {
      const whereFilter = compileFilter(options.where as any)
      whereFragments.push(whereFilter)
    }

    // Cursor conditions
    if (options.cursorConditions && options.cursorConditions.text) {
      whereFragments.push(options.cursorConditions)
    }

    // Add WHERE clause if needed
    if (whereFragments.length > 0) {
      const whereClause = sql.join(whereFragments, ' AND ')
      finalParts.push(sql`WHERE ${whereClause}`)
    }

    // Add ORDER BY
    if (options.orderBy && options.orderBy.length > 0) {
      const orderFragments = options.orderBy.map((order) => {
        const col = sql.ident(`${table}.${order.column}`)
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

    // Add LIMIT
    if (options.limit) {
      finalParts.push(sql`LIMIT ${options.limit}`)
    }

    // Add OFFSET
    if (options.offset) {
      finalParts.push(sql`OFFSET ${options.offset}`)
    }

    // If no WHERE conditions, just join all parts
    return sql.join(finalParts, ' ')
  } catch (error) {
    throw new SqlBuilderError(
      `Failed to build SELECT query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { table, options, error },
    )
  }
}

/**
 * Build a JOIN clause
 */
function buildJoin(
  baseTable: string,
  joinDef: JoinDef,
): ReturnType<typeof sql> {
  const { remote, localPk, through } = joinDef

  if (through) {
    // Many-to-many join through junction table
    const throughJoin = sql`LEFT JOIN ${sql.ident(through.table)} ON ${sql.ident(`${baseTable}.${localPk}`)} = ${sql.ident(`${through.table}.${through.from}`)}`
    const remoteJoin = sql`LEFT JOIN ${sql.ident(remote.table)} ON ${sql.ident(`${through.table}.${through.to}`)} = ${sql.ident(`${remote.table}.${remote.pk}`)}`

    const joinFragments = [throughJoin, remoteJoin]

    if (joinDef.where && Object.keys(joinDef.where).length > 0) {
      joinFragments.push(sql`AND ${compileFilter(joinDef.where as any)}`)
    }

    return sql.join(joinFragments, ' ')
  } else {
    // Direct join
    const joinFragments = [
      sql`LEFT JOIN ${sql.ident(remote.table)} ON ${sql.ident(`${baseTable}.${localPk}`)} = ${sql.ident(`${remote.table}.${remote.pk}`)}`,
    ]

    if (joinDef.where && Object.keys(joinDef.where).length > 0) {
      joinFragments.push(sql`AND ${compileFilter(joinDef.where as any)}`)
    }

    return sql.join(joinFragments, ' ')
  }
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

    // Create individual placeholders for the VALUES clause
    const placeholderFragments = values.map((value) => sql`${value}`)

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
    // Build SET clauses using sql fragments
    const setFragments = Object.entries(data).map(
      ([column, value]) => sql`${sql.ident(column)} = ${value}`,
    )

    // Build WHERE clause
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

/**
 * Build a COUNT query
 */
export function buildCount(
  table: string,
  where?: Record<string, unknown>,
): ReturnType<typeof sql> {
  try {
    const queryParts = [sql`SELECT COUNT(*) as count FROM ${sql.ident(table)}`]

    if (where && Object.keys(where).length > 0) {
      const whereFilter = compileFilter(where as any)
      queryParts.push(sql`WHERE ${whereFilter}`)
    }

    return sql.join(queryParts, ' ')
  } catch (error) {
    throw new SqlBuilderError(
      `Failed to build COUNT query: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { table, where, error },
    )
  }
}

/**
 * Build a SELECT query for a single record by primary key
 */
export function buildSelectById(
  table: string,
  primaryKey: string | string[],
  id: string | number | Record<string, unknown>,
  options: {
    columns?: string[]
    joins?: Record<string, JoinDef>
    include?: Record<string, boolean>
    joinColumnOverrides?: Record<string, string[]>
  } = {},
): ReturnType<typeof sql> {
  const where: Record<string, unknown> = {}

  // Check if there are active JOINs that could cause column ambiguity
  const hasActiveJoins =
    options.joins &&
    options.include &&
    Object.keys(options.include).some((key) => options.include![key])

  if (Array.isArray(primaryKey)) {
    if (typeof id !== 'object' || id === null || Array.isArray(id)) {
      throw new SqlBuilderError(
        'Composite primary key requires an object with key-value pairs',
        { primaryKey, id },
      )
    }

    if (hasActiveJoins) {
      // Use qualified table syntax to avoid column ambiguity
      where[`$${table}`] = { ...id }
    } else {
      Object.assign(where, id)
    }
  } else {
    if (hasActiveJoins) {
      // Use qualified table syntax to avoid column ambiguity
      where[`$${table}`] = { [primaryKey]: id }
    } else {
      where[primaryKey] = id
    }
  }

  return buildSelect(table, {
    ...options,
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
