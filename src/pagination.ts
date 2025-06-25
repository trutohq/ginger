import { CursorError } from './errors.js'
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
): { sql: string; params: unknown[] } {
  const { orderBy, values, direction } = cursor

  if (orderBy.length !== values.length) {
    throw new CursorError(
      'Cursor orderBy and values arrays must have the same length',
      { orderBy, values },
    )
  }

  if (orderBy.length === 0) {
    return { sql: '', params: [] }
  }

  // Build comparison conditions for cursor pagination
  // For next: use > or < based on sort direction
  // For prev: use < or > based on sort direction (opposite)
  const conditions: string[] = []
  const params: unknown[] = []

  for (let i = 0; i < orderBy.length; i++) {
    const order = orderBy[i]!
    const value = values[i]
    const columnName = tableName ? `${tableName}.${order.column}` : order.column

    // Determine comparison operator
    let operator: string
    if (direction === 'next') {
      operator = order.direction === 'asc' ? '>' : '<'
    } else {
      operator = order.direction === 'asc' ? '<' : '>'
    }

    if (i === orderBy.length - 1) {
      // Last column: simple comparison
      conditions.push(`${columnName} ${operator} ?`)
      params.push(value)
    } else {
      // Multi-column: build composite condition
      // (col1 = ? AND col2 = ? AND ... AND colN > ?) OR
      // (col1 = ? AND col2 = ? AND ... AND colN-1 > ?) OR
      // ...
      // (col1 > ?)
      const equalityConditions: string[] = []
      const equalityParams: unknown[] = []

      for (let j = 0; j <= i; j++) {
        const currentOrder = orderBy[j]!
        const currentValue = values[j]
        const currentColumnName = tableName
          ? `${tableName}.${currentOrder.column}`
          : currentOrder.column

        if (j === i) {
          // Last condition in this group: use comparison
          let currentOperator: string
          if (direction === 'next') {
            currentOperator = currentOrder.direction === 'asc' ? '>' : '<'
          } else {
            currentOperator = currentOrder.direction === 'asc' ? '<' : '>'
          }
          equalityConditions.push(`${currentColumnName} ${currentOperator} ?`)
        } else {
          // Earlier conditions: use equality
          equalityConditions.push(`${currentColumnName} = ?`)
        }
        equalityParams.push(currentValue)
      }

      conditions.push(`(${equalityConditions.join(' AND ')})`)
      params.push(...equalityParams)
    }
  }

  const sql = conditions.length > 0 ? conditions.join(' OR ') : ''
  return { sql, params }
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
