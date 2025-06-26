import { Database } from 'bun:sqlite'

/**
 * Mock implementation of Cloudflare D1Database interface for testing
 * Based on the official Cloudflare D1 Worker API documentation
 */
export class MockD1Database {
  private db: Database

  constructor(db: Database) {
    this.db = db
  }

  /**
   * Prepares a query statement to be later executed
   * @param query The SQL query you wish to execute on the database
   * @returns D1PreparedStatement object
   */
  prepare(query: string) {
    const stmt = this.db.prepare(query)

    return {
      /**
       * Bind parameters to the prepared statement
       * @param values Values to bind to the prepared statement
       */
      bind: (...values: unknown[]) => {
        return {
          first: () => {
            try {
              const result = stmt.get(...(values as any))
              return result || null
            } catch (error) {
              return null
            }
          },
          all: () => {
            try {
              const results = stmt.all(...(values as any))
              return {
                success: true,
                results: results,
                meta: {
                  served_by: 'mock.db',
                  duration: 1,
                  changes: 0,
                  last_row_id: 0,
                  changed_db: false,
                  size_after: 8192,
                  rows_read: results.length,
                  rows_written: 0,
                },
              }
            } catch (error) {
              return {
                success: false,
                results: [],
                meta: {
                  served_by: 'mock.db',
                  duration: 1,
                  changes: 0,
                  last_row_id: 0,
                  changed_db: false,
                  size_after: 8192,
                  rows_read: 0,
                  rows_written: 0,
                },
              }
            }
          },
          run: () => {
            try {
              const result = stmt.run(...(values as any))
              return {
                success: true,
                meta: {
                  served_by: 'mock.db',
                  duration: 1,
                  changes: result.changes || 0,
                  last_row_id: result.lastInsertRowid || 0,
                  changed_db: (result.changes || 0) > 0,
                  size_after: 8192,
                  rows_read: 1,
                  rows_written: result.changes || 0,
                },
              }
            } catch (error) {
              return {
                success: false,
                meta: {
                  served_by: 'mock.db',
                  duration: 1,
                  changes: 0,
                  last_row_id: 0,
                  changed_db: false,
                  size_after: 8192,
                  rows_read: 0,
                  rows_written: 0,
                },
              }
            }
          },
        }
      },
      first: () => {
        try {
          const result = stmt.get()
          return result || null
        } catch (error) {
          return null
        }
      },
      all: () => {
        try {
          const results = stmt.all()
          return {
            success: true,
            results: results,
            meta: {
              served_by: 'mock.db',
              duration: 1,
              changes: 0,
              last_row_id: 0,
              changed_db: false,
              size_after: 8192,
              rows_read: results.length,
              rows_written: 0,
            },
          }
        } catch (error) {
          return {
            success: false,
            results: [],
            meta: {
              served_by: 'mock.db',
              duration: 1,
              changes: 0,
              last_row_id: 0,
              changed_db: false,
              size_after: 8192,
              rows_read: 0,
              rows_written: 0,
            },
          }
        }
      },
      run: () => {
        try {
          const result = stmt.run()
          return {
            success: true,
            meta: {
              served_by: 'mock.db',
              duration: 1,
              changes: result.changes || 0,
              last_row_id: result.lastInsertRowid || 0,
              changed_db: (result.changes || 0) > 0,
              size_after: 8192,
              rows_read: 1,
              rows_written: result.changes || 0,
            },
          }
        } catch (error) {
          return {
            success: false,
            meta: {
              served_by: 'mock.db',
              duration: 1,
              changes: 0,
              last_row_id: 0,
              changed_db: false,
              size_after: 8192,
              rows_read: 0,
              rows_written: 0,
            },
          }
        }
      },
    }
  }

  /**
   * Sends multiple SQL statements inside a single call to the database
   * @param statements Array of D1PreparedStatement objects
   * @returns Promise resolving to array of D1Result objects
   */
  batch(statements: any[]) {
    return Promise.resolve(statements.map((stmt) => stmt.run()))
  }

  /**
   * Executes one or more queries directly without prepared statements or parameter bindings
   * @param query The SQL query statement without parameter binding
   * @returns Promise resolving to D1ExecResult object
   */
  exec(query: string) {
    this.db.exec(query)
    return Promise.resolve({
      count: 1,
      duration: 1,
    })
  }

  /**
   * Dumps the entire D1 database to an SQLite compatible file inside an ArrayBuffer
   * Note: This API only works on databases created during D1's alpha period
   * @returns Promise resolving to ArrayBuffer
   */
  dump() {
    return Promise.resolve(new ArrayBuffer(0))
  }
}
