import type {
  Database,
  ExecResult,
  PreparedStatement,
  QueryResult,
} from '../types.js'

/**
 * Structural interface matching Bun's `bun:sqlite` Statement.
 * No runtime import of `bun:sqlite` is needed — TypeScript's structural
 * typing lets the real Bun `Database` satisfy this automatically.
 */
export interface BunSqliteStatement {
  get(...params: unknown[]): unknown
  all(...params: unknown[]): unknown[]
  run(...params: unknown[]): {
    changes: number
    lastInsertRowid: number | bigint
  }
}

/**
 * Structural interface matching Bun's `bun:sqlite` Database.
 */
export interface BunSqliteDatabase {
  prepare(query: string): BunSqliteStatement
  exec(query: string): void
  serialize?(): Uint8Array
}

/**
 * Wrap a Bun `Database` (from `bun:sqlite`) so it satisfies the
 * generic {@link Database} interface used by Ginger services.
 *
 * @example
 * ```typescript
 * import { Database } from 'bun:sqlite'
 * import { createService, fromBunSqlite } from '@truto/ginger'
 *
 * const bunDb = new Database(':memory:')
 * const db = fromBunSqlite(bunDb)
 * const service = createService({ db, ... })
 * ```
 */
export function fromBunSqlite(bunDb: BunSqliteDatabase): Database {
  return {
    prepare(query: string): PreparedStatement {
      const stmt = bunDb.prepare(query)

      function makebound(values: unknown[]): PreparedStatement {
        return {
          bind(...more: unknown[]) {
            return makebound([...values, ...more])
          },
          first<T>(): Promise<T | null> {
            const row = stmt.get(...values)
            return Promise.resolve((row as T) ?? null)
          },
          all<T>(): Promise<QueryResult<T[]>> {
            const rows = stmt.all(...values)
            return Promise.resolve({
              success: true,
              results: rows as T[],
              meta: {
                duration: 0,
                size_after: 0,
                rows_read: rows.length,
                rows_written: 0,
                last_row_id: 0,
                changed_db: false,
                changes: 0,
              },
            })
          },
          run(): Promise<QueryResult> {
            const result = stmt.run(...values)
            const changes = result.changes ?? 0
            return Promise.resolve({
              success: true,
              meta: {
                duration: 0,
                size_after: 0,
                rows_read: 0,
                rows_written: changes,
                last_row_id: Number(result.lastInsertRowid ?? 0),
                changed_db: changes > 0,
                changes,
              },
            })
          },
          raw<T>(): Promise<T[]> {
            return Promise.resolve(stmt.all(...values) as T[])
          },
        }
      }

      return {
        bind(...values: unknown[]) {
          return makebound(values)
        },
        first<T>(): Promise<T | null> {
          return makebound([]).first<T>()
        },
        all<T>(): Promise<QueryResult<T[]>> {
          return makebound([]).all<T>()
        },
        run(): Promise<QueryResult> {
          return makebound([]).run()
        },
        raw<T>(): Promise<T[]> {
          return makebound([]).raw<T>()
        },
      }
    },

    async batch<T>(statements: PreparedStatement[]): Promise<QueryResult<T>[]> {
      const results = await Promise.all(statements.map((s) => s.run()))
      return results as unknown as QueryResult<T>[]
    },

    exec(query: string): Promise<ExecResult> {
      bunDb.exec(query)
      return Promise.resolve({ count: 1, duration: 0 })
    },

    dump(): Promise<ArrayBuffer> {
      if (!bunDb.serialize) {
        throw new Error(
          'dump() is not supported: the underlying database does not implement serialize()',
        )
      }
      const bytes = bunDb.serialize()
      return Promise.resolve(bytes.buffer as ArrayBuffer)
    },
  }
}
