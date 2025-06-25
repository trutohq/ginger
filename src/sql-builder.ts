import { compileFilter, sql } from '@truto/sqlite-builder'
import { SqlBuilderError } from './errors.js'
import type { AuthContext, JoinDef, OrderBy } from './types.js'

/**
 * Build a SELECT query with joins and pagination
 */
export function buildSelect(
  table: string,
  options: {
    columns?: string[]
    where?: Record<string, unknown>
    joins?: Record<string, JoinDef>
    include?: Record<string, boolean>
    orderBy?: OrderBy[]
    limit?: number
    offset?: number
    auth?: AuthContext
    cursorConditions?: { sql: string; params: unknown[] }
  } = {},
): { sql: string; params: unknown[] } {
  try {
    // Build column list
    const selectColumns: string[] = []

    if (options.columns && options.columns.length > 0) {
      selectColumns.push(...options.columns.map((col) => `${table}.${col}`))
    } else {
      selectColumns.push(`${table}.*`)
    }

    // Add join columns
    if (options.joins && options.include) {
      for (const [joinName, joinDef] of Object.entries(options.joins)) {
        if (options.include[joinName]) {
          const joinAlias = joinDef.remote.alias || joinName
          const joinColumns = joinDef.remote.select.map(
            (col) => `${joinDef.remote.table}.${col} as ${joinAlias}_${col}`,
          )
          selectColumns.push(...joinColumns)
        }
      }
    }

    // Start building the query
    // Handle wildcard columns separately since sql.ident doesn't support them
    const columnList =
      selectColumns.length === 1 && selectColumns[0] === `${table}.*`
        ? sql.raw(`${table}.*`)
        : sql.raw(selectColumns.join(', '))

    let query = sql`SELECT ${columnList} FROM ${sql.ident(table)}`

    // Add joins
    if (options.joins && options.include) {
      for (const [joinName, joinDef] of Object.entries(options.joins)) {
        if (options.include[joinName]) {
          const joinSql = buildJoin(table, joinName, joinDef, options.auth)
          query = sql`${query} ${sql.raw(joinSql.sql)}`
          // Note: Join conditions are embedded in the JOIN clause
        }
      }
    }

    // Build WHERE conditions and collect parameters
    const whereConditions: Parameters<typeof sql.join>[0] = []
    const allParams: unknown[] = []

    // Regular WHERE conditions
    if (options.where && Object.keys(options.where).length > 0) {
      const whereFilter = compileFilter(options.where as any)
      whereConditions.push(sql.raw(whereFilter.text))
      allParams.push(...whereFilter.values)
    }

    // Cursor conditions
    if (options.cursorConditions && options.cursorConditions.sql) {
      whereConditions.push(sql.raw(options.cursorConditions.sql))
      allParams.push(...options.cursorConditions.params)
    }

    // Add WHERE clause if needed
    if (whereConditions.length > 0) {
      const combinedWhere = sql.join(whereConditions, ' AND ')
      query = sql`${query} WHERE ${combinedWhere}`
    }

    // Add ORDER BY
    if (options.orderBy && options.orderBy.length > 0) {
      const orderClauses = options.orderBy.map(
        (order) =>
          `${sql.ident(`${table}.${order.column}`).text} ${order.direction.toUpperCase()}`,
      )
      query = sql`${query} ORDER BY ${sql.raw(orderClauses.join(', '))}`
    }

    // Add LIMIT
    if (options.limit) {
      query = sql`${query} LIMIT ${options.limit}`
      allParams.push(options.limit)
    }

    // Add OFFSET
    if (options.offset) {
      query = sql`${query} OFFSET ${options.offset}`
      allParams.push(options.offset)
    }

    return {
      sql: query.text,
      params: allParams,
    }
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
  joinName: string,
  joinDef: JoinDef,
  auth?: AuthContext,
): { sql: string; params: unknown[] } {
  const { remote, localPk, through } = joinDef

  let joinSql: string

  if (through) {
    // Many-to-many join through junction table
    joinSql = `LEFT JOIN ${sql.ident(through.table).text} ON ${sql.ident(`${baseTable}.${localPk}`).text} = ${sql.ident(`${through.table}.${through.from}`).text} `
    joinSql += `LEFT JOIN ${sql.ident(remote.table).text} ON ${sql.ident(`${through.table}.${through.to}`).text} = ${sql.ident(`${remote.table}.${remote.pk}`).text}`
  } else {
    // Direct join
    joinSql = `LEFT JOIN ${sql.ident(remote.table).text} ON ${sql.ident(`${baseTable}.${localPk}`).text} = ${sql.ident(`${remote.table}.${remote.pk}`).text}`
  }

  // Add WHERE condition for the join if specified
  if (joinDef.where) {
    const whereCondition =
      typeof joinDef.where === 'function'
        ? joinDef.where({ auth: auth || {} })
        : joinDef.where

    if (whereCondition) {
      joinSql += ` AND ${whereCondition}`
    }
  }

  return { sql: joinSql, params: [] }
}

/**
 * Build an INSERT query
 */
export function buildInsert(
  table: string,
  data: Record<string, unknown>,
): { sql: string; params: unknown[] } {
  try {
    const columns = Object.keys(data)
    const values = Object.values(data)

    // Create placeholder string manually to avoid double parentheses
    const placeholders = values.map(() => '?').join(', ')

    const query = sql`
      INSERT INTO ${sql.ident(table)} (${sql.ident(columns)}) 
      VALUES (${sql.raw(placeholders)})
    `

    return {
      sql: query.text,
      params: [...values],
    }
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
): { sql: string; params: unknown[] } {
  try {
    // Build SET clauses
    const setClauses = Object.keys(data).map(
      (column) => `${sql.ident(column).text} = ?`,
    )
    const setValues = Object.values(data)

    // Build WHERE clause
    const whereFilter = compileFilter(where)

    const query = sql`
      UPDATE ${sql.ident(table)} 
      SET ${sql.raw(setClauses.join(', '))}
      WHERE ${sql.raw(whereFilter.text)}
    `

    return {
      sql: query.text,
      params: [...setValues, ...whereFilter.values],
    }
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
): { sql: string; params: unknown[] } {
  try {
    const whereFilter = compileFilter(where)

    const query = sql`
      DELETE FROM ${sql.ident(table)}
      WHERE ${sql.raw(whereFilter.text)}
    `

    return {
      sql: query.text,
      params: [...whereFilter.values],
    }
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
): { sql: string; params: unknown[] } {
  try {
    let query = sql`SELECT COUNT(*) as count FROM ${sql.ident(table)}`
    const allParams: unknown[] = []

    if (where && Object.keys(where).length > 0) {
      const whereFilter = compileFilter(where)
      query = sql`${query} WHERE ${sql.raw(whereFilter.text)}`
      allParams.push(...whereFilter.values)
    }

    return {
      sql: query.text,
      params: allParams,
    }
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
    auth?: AuthContext
  } = {},
): { sql: string; params: unknown[] } {
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
