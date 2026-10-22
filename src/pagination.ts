import { sql } from '@truto/sqlite-builder'
import { CursorError } from './errors.js'
import { toBindableValue } from './sql-builder.js'
import type { CursorToken, OrderBy } from './types.js'

/**
 * Encode a cursor token to an opaque base64 string
 */
export function encodeCursor(token: CursorToken): string {
  try {
    // `btoa` only accepts code units <= 0xFF, and `JSON.stringify` emits
    // non-ASCII characters raw. Cursors carry the values of the columns being
    // sorted on, so paginating over any text column holding a CJK name or an
    // emoji used to throw `InvalidCharacterError` and 500 the request.
    //
    // We escape exactly the code units `btoa` rejects (> 0xFF) as `\uXXXX`.
    // That range is precisely the set of inputs that could not be encoded
    // before, so every cursor that encodes today keeps its byte-for-byte
    // identical representation and stays decodable. Deliberately NOT UTF-8:
    // that would re-encode the latin1 range (U+0080–U+00FF, e.g. "José") which
    // `btoa` accepts today, so cursors already in flight would decode to a
    // different value — and it is not even distinguishable on decode, since
    // legacy("Ã©") and utf8("é") are the same bytes.
    //
    // Escaping only ever fires inside JSON string literals; all JSON structural
    // characters are ASCII. `decodeCursor` needs no counterpart — `JSON.parse`
    // already turns `\uXXXX` back into the original character.
    const json = JSON.stringify(token).replace(
      // Matches exactly the code units btoa rejects (>= U+0100).
      /[\u0100-\uffff]/g,
      (char) => '\\u' + char.charCodeAt(0).toString(16).padStart(4, '0'),
    )
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
 *
 * `isNullable` says whether a column can hold NULL. SQLite sorts NULL below
 * every value (first on ASC, last on DESC), and `= NULL`, `> NULL` and
 * `< NULL` are never true, so a plain keyset condition loses rows around
 * NULLs: after a row whose earlier sort column is NULL, `col = ?` matches
 * nothing and paging ends early, and on DESC the NULL rows, which come last,
 * are never reached. For a column that can hold NULL, and for any cursor
 * value that is NULL, the condition is NULL-aware instead:
 *
 * - equality on an earlier column is `col IS ?`, which matches NULL too;
 * - "after" a NULL value is `col IS NOT NULL` where the walk goes toward
 *   larger values, and nothing where it goes toward smaller ones (no value
 *   sorts below NULL);
 * - "before" a non-NULL value is `(col < ? OR col IS NULL)`.
 *
 * A column that can't hold NULL, with a non-NULL value, keeps the plain
 * `=`, `>` and `<`, so its SQL is byte-identical to before and can still use
 * an index range.
 */
export function buildCursorConditions(
  cursor: CursorToken,
  tableName?: string,
  resolveColumn?: (column: string) => string,
  isNullable: (column: string) => boolean = () => false,
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

  const nullAware = (index: number): boolean =>
    values[index] === null ||
    values[index] === undefined ||
    isNullable(orderBy[index]!.column)

  // The comparison that puts a row strictly after the cursor on this column,
  // or `undefined` when no row can be (after a NULL, toward smaller values).
  // `toBindableValue()` coerces/validates the bound value and throws on
  // non-scalar objects, so it can never be treated as a raw fragment.
  const comparison = (index: number): ReturnType<typeof sql> | undefined => {
    const order = orderBy[index]!
    const value = values[index]
    const columnIdent = sql.ident(columnRef(order))
    // Determine if we need > or < based on cursor direction and sort direction
    const useGreaterThan =
      (direction === 'next') === (order.direction === 'asc')
    if (value === null || value === undefined) {
      return useGreaterThan ? sql`${columnIdent} IS NOT NULL` : undefined
    }
    if (useGreaterThan) {
      return sql`${columnIdent} > ${toBindableValue(value)}`
    }
    return nullAware(index)
      ? sql`(${columnIdent} < ${toBindableValue(value)} OR ${columnIdent} IS NULL)`
      : sql`${columnIdent} < ${toBindableValue(value)}`
  }

  for (let i = 0; i < orderBy.length; i++) {
    const compare = comparison(i)
    if (!compare) {
      // Nothing sorts past a NULL in this direction; later branches, which
      // pin this column with IS, still page through the NULL rows.
      continue
    }

    if (orderBy.length === 1) {
      // Single-column order: simple comparison, no equality prefix possible.
      conditions.push(compare)
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

      for (let j = 0; j < i; j++) {
        const currentColumnIdent = sql.ident(columnRef(orderBy[j]!))
        const currentValue = toBindableValue(values[j])
        // Earlier conditions: use equality, NULL-safe where NULL can occur
        equalityConditions.push(
          nullAware(j)
            ? sql`${currentColumnIdent} IS ${currentValue}`
            : sql`${currentColumnIdent} = ${currentValue}`,
        )
      }
      // Last condition in this group: use comparison
      equalityConditions.push(compare)

      const compositeCondition = sql.join(equalityConditions, ' AND ')
      conditions.push(sql`(${compositeCondition})`)
    }
  }

  // Every branch was impossible: the cursor sits on the last possible row in
  // this direction. Without a condition the query would start over at the
  // first page.
  if (conditions.length === 0) return sql`(1 = 0)`

  // Parenthesised, because the caller ANDs this with its own WHERE.
  //
  // These conditions are OR-joined, and SQL binds AND tighter than OR. Returned
  // bare, `WHERE job_id = ? AND <cursor>` parses as
  //
  //   (job_id = ? AND created_at < ?) OR (created_at = ? AND id < ?)
  //
  // and the second disjunct carries NO filter — so a paginated, filtered list
  // returns rows belonging to other records. Measured on a real database: a
  // job with 640 config keys came back with 645, the extra five belonging to a
  // different job, and only on the page where the tie-break disjunct matched.
  //
  // It needs two or more order-by columns to be reachable, since a single
  // column produces one condition and no OR. Any table whose default ordering
  // is a composite primary key is therefore exposed by default.
  //
  // One condition needs no parentheses and gets none, so the emitted SQL for
  // single-column ordering — the common case — is unchanged.
  if (conditions.length === 1) return conditions[0]!
  return sql`(${sql.join(conditions, ' OR ')})`
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
