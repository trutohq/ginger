import type {
  Database,
  ExecResult,
  PreparedStatement,
  QueryResult,
} from '../types.js'

/**
 * Structural interface matching the cursor returned by
 * Cloudflare Durable Object `SqlStorage.exec()`.
 */
export interface SqlStorageCursor<T = Record<string, unknown>> {
  toArray(): T[]
  one(): T
  readonly columnNames: string[]
  readonly rowsRead: number
  readonly rowsWritten: number
}

/**
 * Structural interface matching Cloudflare Durable Object's `SqlStorage`
 * (available as `ctx.storage.sql` inside a Durable Object).
 *
 * No runtime import of `@cloudflare/workers-types` is needed — TypeScript's
 * structural typing lets the real `SqlStorage` satisfy this automatically.
 */
export interface DurableObjectSqlStorage {
  exec<T = Record<string, unknown>>(
    query: string,
    ...bindings: unknown[]
  ): SqlStorageCursor<T>
}

/**
 * Wrap a Durable Object `SqlStorage` instance so it satisfies the
 * generic {@link Database} interface used by Ginger services.
 *
 * @example
 * ```typescript
 * import { createService, fromDurableObjectStorage } from '@truto/ginger'
 *
 * export class MyDO extends DurableObject {
 *   service = createService({
 *     db: fromDurableObjectStorage(this.ctx.storage.sql),
 *     ...
 *   })
 * }
 * ```
 */
export function fromDurableObjectStorage(
  sql: DurableObjectSqlStorage,
): Database {
  return {
    prepare(query: string): PreparedStatement {
      function makeBound(values: unknown[]): PreparedStatement {
        return {
          bind(...more: unknown[]) {
            return makeBound([...values, ...more])
          },
          first<T>(): Promise<T | null> {
            const cursor = sql.exec<T>(query, ...values)
            const rows = cursor.toArray()
            return Promise.resolve(rows[0] ?? null)
          },
          all<T>(): Promise<QueryResult<T[]>> {
            const cursor = sql.exec<T>(query, ...values)
            const rows = cursor.toArray()
            return Promise.resolve({
              success: true,
              results: rows,
              meta: {
                duration: 0,
                size_after: 0,
                rows_read: cursor.rowsRead,
                rows_written: cursor.rowsWritten,
                last_row_id: 0,
                changed_db: cursor.rowsWritten > 0,
                changes: cursor.rowsWritten,
              },
            })
          },
          run(): Promise<QueryResult> {
            const cursor = sql.exec(query, ...values)
            const lastRowId = sql
              .exec<{ lid: number }>('SELECT last_insert_rowid() AS lid')
              .one().lid
            return Promise.resolve({
              success: true,
              meta: {
                duration: 0,
                size_after: 0,
                rows_read: cursor.rowsRead,
                rows_written: cursor.rowsWritten,
                last_row_id: lastRowId,
                changed_db: cursor.rowsWritten > 0,
                changes: cursor.rowsWritten,
              },
            })
          },
          raw<T>(): Promise<T[]> {
            const cursor = sql.exec<T>(query, ...values)
            return Promise.resolve(cursor.toArray())
          },
        }
      }

      return {
        bind(...values: unknown[]) {
          return makeBound(values)
        },
        first<T>(): Promise<T | null> {
          return makeBound([]).first<T>()
        },
        all<T>(): Promise<QueryResult<T[]>> {
          return makeBound([]).all<T>()
        },
        run(): Promise<QueryResult> {
          return makeBound([]).run()
        },
        raw<T>(): Promise<T[]> {
          return makeBound([]).raw<T>()
        },
      }
    },

    async batch<T>(statements: PreparedStatement[]): Promise<QueryResult<T>[]> {
      const results = await Promise.all(statements.map((s) => s.run()))
      return results as unknown as QueryResult<T>[]
    },

    exec(query: string): Promise<ExecResult> {
      sql.exec(query)
      return Promise.resolve({ count: 1, duration: 0 })
    },

    dump(): Promise<ArrayBuffer> {
      throw new Error(
        'dump() is not supported by the Durable Object SqlStorage adapter',
      )
    },
  }
}
