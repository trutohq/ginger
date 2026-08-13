import { sql } from '@truto/sqlite-builder'
import { CursorError } from './errors.js'
import { toBindableValue } from './sql-builder.js'
import type { CursorToken, OrderBy } from './types.js'

/**
 * Encode a cursor token to an opaque base64 string
 */
export function encodeCursor(token: CursorToken): string {
  try {
    const json = JSON.stringify(token)
    return btoa(json)
  } catch (error) {
    throw new CursorError(
      `Failed to encode cursor: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { token, error },
    )
  }
}

/**
 * Decode an opaque cursor string back to a cursor token
 */
export function decodeCursor(cursor: string): CursorToken {
  try {
    const json = atob(cursor)
    const parsed = JSON.parse(json) as CursorToken

    // Validate cursor structure
    if (!parsed || typeof parsed !== 'object') {
      throw new Error('Invalid cursor structure')
    }

    if (!Array.isArray(parsed.orderBy)) {
      throw new Error('Invalid orderBy in cursor')
    }

    if (!Array.isArray(parsed.values)) {
      throw new Error('Invalid values in cursor')
    }

    // SECURITY: cursor values are client-supplied (the cursor is just
    // base64(JSON)). Only allow primitives. Without this guard an attacker can
    // smuggle a `{ text, values }`-shaped object that @truto/sqlite-builder's
    // `sql` template would splice in as a RAW SQL fragment → SQL injection.
    for (const value of parsed.values) {
      const t = typeof value
      if (
        value !== null &&
        t !== 'string' &&
        t !== 'number' &&
        t !== 'boolean'
      ) {
        throw new Error(
          'Invalid value in cursor: only string, number, boolean, or null are allowed',
        )
      }
    }

    if (!parsed.direction || !['next', 'prev'].includes(parsed.direction)) {
      throw new Error('Invalid direction in cursor')
    }

    // Validate orderBy structure
    for (const order of parsed.orderBy) {
      if (!order || typeof order !== 'object') {
        throw new Error('Invalid order object in cursor')
      }
      if (!order.column || typeof order.column !== 'string') {
        throw new Error('Invalid column in cursor orderBy')
      }
      if (!order.direction || !['asc', 'desc'].includes(order.direction)) {
        throw new Error('Invalid direction in cursor orderBy')
      }
    }

    return parsed
  } catch (error) {
    throw new CursorError(
      `Failed to decode cursor: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { cursor, error },
    )
  }
}

/**
 * Create a cursor token from a row and order specification
 */
export function createCursor(
  row: Record<string, unknown>,
  orderBy: OrderBy[],
  direction: 'next' | 'prev',
): CursorToken {
  const values = orderBy.map((order) => {
    const value = row[order.column]
    if (value === undefined) {
      throw new CursorError(
        `Column "${order.column}" not found in row for cursor creation`,
        { row, orderBy, column: order.column },
      )
    }
    return value
  })

  return {
    orderBy,
    values,
    direction,
  }
}

/**
 * Generate WHERE conditions for cursor-based pagination
 */
export function buildCursorConditions(
  cursor: CursorToken,
  tableName?: string,
  resolveColumn?: (column: string) => string,
): ReturnType<typeof sql> {
  const { orderBy, values, direction } = cursor

  if (orderBy.length !== values.length) {
    throw new CursorError(
      'Cursor orderBy and values arrays must have the same length',
      { orderBy, values },
    )
  }

  if (orderBy.length === 0) {
    return sql``
  }

  const conditions: ReturnType<typeof sql>[] = []

  const columnRef = (order: OrderBy): string => {
    if (resolveColumn) return resolveColumn(order.column)
    if (tableName) return `${tableName}.${order.column}`
    return order.column
  }

  for (let i = 0; i < orderBy.length; i++) {
    const order = orderBy[i]!
    const value = values[i]

    const columnIdent = sql.ident(columnRef(order))

    // Determine if we need > or < based on cursor direction and sort direction
    const useGreaterThan =
      (direction === 'next') === (order.direction === 'asc')

    if (orderBy.length === 1) {
      // Single-column order: simple comparison, no equality prefix possible.
      // `toBindableValue()` coerces/validates the bound value and throws on
      // non-scalar objects, so it can never be treated as a raw fragment.
      conditions.push(
        useGreaterThan
          ? sql`${columnIdent} > ${toBindableValue(value)}`
          : sql`${columnIdent} < ${toBindableValue(value)}`,
      )
    } else {
      // Multi-column: build composite condition, one branch per order
      // column, each pinning every earlier column to its cursor value —
      // (col1 > ?) OR
      // (col1 = ? AND col2 > ?) OR
      // (col1 = ? AND col2 = ? AND col3 > ?) OR ...
      // This applies to every column, including the last: without the
      // equality prefix on the earlier columns, a row whose later column
      // happens to satisfy the bare comparison would leak onto this page
      // even though it doesn't belong there under the full ordering,
      // causing pages to overlap or skip rows.
      const equalityConditions: ReturnType<typeof sql>[] = []

      for (let j = 0; j <= i; j++) {
        const currentOrder = orderBy[j]!
        const currentValue = values[j]

        const currentColumnIdent = sql.ident(columnRef(currentOrder))

        if (j === i) {
          // Last condition in this group: use comparison
          const currentUseGt =
            (direction === 'next') === (currentOrder.direction === 'asc')
          equalityConditions.push(
            currentUseGt
              ? sql`${currentColumnIdent} > ${toBindableValue(currentValue)}`
              : sql`${currentColumnIdent} < ${toBindableValue(currentValue)}`,
          )
        } else {
          // Earlier conditions: use equality
          equalityConditions.push(
            sql`${currentColumnIdent} = ${toBindableValue(currentValue)}`,
          )
        }
      }

      const compositeCondition = sql.join(equalityConditions, ' AND ')
      conditions.push(sql`(${compositeCondition})`)
    }
  }

  return conditions.length > 0 ? sql.join(conditions, ' OR ') : sql``
}

/**
 * Get default ordering for a table
 */
export function getDefaultOrderBy(
  primaryKey: string | string[],
  defaultOrderBy?: OrderBy,
): OrderBy[] {
  if (defaultOrderBy) {
    return [defaultOrderBy]
  }

  if (Array.isArray(primaryKey)) {
    return primaryKey.map((key) => ({ column: key, direction: 'asc' as const }))
  }

  return [{ column: primaryKey, direction: 'asc' as const }]
}

/**
 * Validate order by columns against allowed columns
 */
export function validateOrderBy(
  orderBy: OrderBy[],
  allowedColumns: string[],
): void {
  for (const order of orderBy) {
    if (!allowedColumns.includes(order.column)) {
      throw new CursorError(
        `Invalid order column "${order.column}". Allowed columns: ${allowedColumns.join(', ')}`,
        { column: order.column, allowedColumns },
      )
    }
  }
}

/**
 * Reverse the direction of OrderBy for previous page navigation
 */
export function reverseOrderBy(orderBy: OrderBy[]): OrderBy[] {
  return orderBy.map((order) => ({
    ...order,
    direction: order.direction === 'asc' ? 'desc' : 'asc',
  }))
}
