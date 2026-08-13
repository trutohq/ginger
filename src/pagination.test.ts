import { Database as BunDatabase } from 'bun:sqlite'
import { describe, expect, it } from 'vitest'
import { CursorError } from './errors.js'
import {
  buildCursorConditions,
  createCursor,
  decodeCursor,
  encodeCursor,
  getDefaultOrderBy,
  reverseOrderBy,
  validateOrderBy,
} from './pagination.js'
import type { CursorToken, OrderBy } from './types.js'

describe('Pagination System', () => {
  describe('encodeCursor', () => {
    it('should encode cursor tokens correctly', () => {
      const token: CursorToken = {
        orderBy: [{ column: 'id', direction: 'asc' }],
        values: [10],
        direction: 'next',
      }

      const encoded = encodeCursor(token)
      expect(typeof encoded).toBe('string')
      expect(encoded.length).toBeGreaterThan(0)
    })

    it('should encode complex cursor tokens', () => {
      const token: CursorToken = {
        orderBy: [
          { column: 'name', direction: 'desc' },
          { column: 'id', direction: 'asc' },
        ],
        values: ['John Doe', 42],
        direction: 'prev',
      }

      const encoded = encodeCursor(token)
      expect(typeof encoded).toBe('string')
      expect(encoded.length).toBeGreaterThan(0)
    })

    it('should throw CursorError for encoding failures', () => {
      // Create a token with circular reference to trigger JSON.stringify error
      const token = { orderBy: [], values: [], direction: 'next' } as any
      token.circular = token

      expect(() => encodeCursor(token)).toThrow(CursorError)
    })
  })

  describe('decodeCursor', () => {
    it('should decode valid cursor tokens', () => {
      const originalToken: CursorToken = {
        orderBy: [{ column: 'id', direction: 'asc' }],
        values: [10],
        direction: 'next',
      }

      const encoded = encodeCursor(originalToken)
      const decoded = decodeCursor(encoded)

      expect(decoded).toEqual(originalToken)
    })

    it('should decode complex cursor tokens', () => {
      const originalToken: CursorToken = {
        orderBy: [
          { column: 'created_at', direction: 'desc' },
          { column: 'id', direction: 'asc' },
        ],
        values: ['2024-01-01T00:00:00Z', 123],
        direction: 'prev',
      }

      const encoded = encodeCursor(originalToken)
      const decoded = decodeCursor(encoded)

      expect(decoded).toEqual(originalToken)
    })

    it('should throw CursorError for invalid base64', () => {
      expect(() => decodeCursor('invalid-base64!')).toThrow(CursorError)
      expect(() => decodeCursor('not-base64')).toThrow(CursorError)
    })

    it('should throw CursorError for invalid JSON', () => {
      const invalidJson = btoa('invalid json')
      expect(() => decodeCursor(invalidJson)).toThrow(CursorError)
    })

    it('should throw CursorError for invalid cursor structure', () => {
      const invalidStructures = [
        null,
        'string',
        123,
        [],
        { invalid: 'structure' },
        { orderBy: 'not-array', values: [], direction: 'next' },
        { orderBy: [], values: 'not-array', direction: 'next' },
        { orderBy: [], values: [], direction: 'invalid' },
        { orderBy: [{ invalid: 'order' }], values: [], direction: 'next' },
        {
          orderBy: [{ column: 123, direction: 'asc' }],
          values: [],
          direction: 'next',
        },
        {
          orderBy: [{ column: 'id', direction: 'invalid' }],
          values: [],
          direction: 'next',
        },
      ]

      for (const invalid of invalidStructures) {
        const encoded = btoa(JSON.stringify(invalid))
        expect(() => decodeCursor(encoded)).toThrow(CursorError)
      }
    })
  })

  describe('createCursor', () => {
    it('should create cursor from row data', () => {
      const row = { id: 10, name: 'John', email: 'john@test.com' }
      const orderBy: OrderBy[] = [{ column: 'id', direction: 'asc' }]

      const cursor = createCursor(row, orderBy, 'next')

      expect(cursor).toEqual({
        orderBy,
        values: [10],
        direction: 'next',
      })
    })

    it('should create cursor with multiple columns', () => {
      const row = { id: 42, name: 'Jane', created_at: '2024-01-01' }
      const orderBy: OrderBy[] = [
        { column: 'name', direction: 'desc' },
        { column: 'id', direction: 'asc' },
      ]

      const cursor = createCursor(row, orderBy, 'prev')

      expect(cursor).toEqual({
        orderBy,
        values: ['Jane', 42],
        direction: 'prev',
      })
    })

    it('should throw CursorError for missing column', () => {
      const row = { id: 10, name: 'John' }
      const orderBy: OrderBy[] = [
        { column: 'missing_column', direction: 'asc' },
      ]

      expect(() => createCursor(row, orderBy, 'next')).toThrow(CursorError)
    })
  })

  describe('buildCursorConditions', () => {
    it('should return empty sql for empty orderBy', () => {
      const cursor: CursorToken = {
        orderBy: [],
        values: [],
        direction: 'next',
      }

      const result = buildCursorConditions(cursor)

      expect(result.text).toBe('')
      expect(result.values).toEqual([])
    })

    it('should build simple cursor conditions', () => {
      const cursor: CursorToken = {
        orderBy: [{ column: 'id', direction: 'asc' }],
        values: [10],
        direction: 'next',
      }

      const result = buildCursorConditions(cursor)

      expect(result.text).toContain('"id"')
      expect(result.text).toContain('>')
      expect(result.values).toContain(10)
    })

    it('should build cursor conditions with table name', () => {
      const cursor: CursorToken = {
        orderBy: [{ column: 'id', direction: 'asc' }],
        values: [10],
        direction: 'next',
      }

      const result = buildCursorConditions(cursor, 'users')

      expect(result.text).toContain('"users"."id"')
      expect(result.text).toContain('>')
      expect(result.values).toContain(10)
    })

    it('should build multi-column cursor conditions', () => {
      const cursor: CursorToken = {
        orderBy: [
          { column: 'name', direction: 'desc' },
          { column: 'id', direction: 'asc' },
        ],
        values: ['John', 5],
        direction: 'next',
      }

      const result = buildCursorConditions(cursor)

      expect(result.text).toContain('"name"')
      expect(result.text).toContain('"id"')
      expect(result.text).toContain('OR')
      expect(result.values).toContain('John')
      expect(result.values).toContain(5)
    })

    it('should handle prev direction correctly', () => {
      const cursor: CursorToken = {
        orderBy: [{ column: 'id', direction: 'asc' }],
        values: [10],
        direction: 'prev',
      }

      const result = buildCursorConditions(cursor)

      expect(result.text).toContain('<') // Should be < for prev direction
      expect(result.values).toContain(10)
    })

    it('should handle desc ordering correctly', () => {
      const cursor: CursorToken = {
        orderBy: [{ column: 'id', direction: 'desc' }],
        values: [10],
        direction: 'next',
      }

      const result = buildCursorConditions(cursor)

      expect(result.text).toContain('<') // Should be < for desc + next
      expect(result.values).toContain(10)
    })

    it('should throw CursorError for mismatched arrays', () => {
      const cursor: CursorToken = {
        orderBy: [{ column: 'id', direction: 'asc' }],
        values: [10, 20], // Too many values
        direction: 'next',
      }

      expect(() => buildCursorConditions(cursor)).toThrow(CursorError)
    })

    it('should prefix every branch (including the last column) with equality on all earlier columns', () => {
      // Regression test: for ORDER BY a DESC, b DESC, c DESC, correct keyset
      // pagination requires
      //   (a < ?) OR (a = ? AND b < ?) OR (a = ? AND b = ? AND c < ?)
      // The bug emitted a bare `c < ?` for the last column with no equality
      // prefix on `a`/`b`, so a row with a different `a` but a smaller `c`
      // would leak onto the page even though it doesn't belong there.
      const cursor: CursorToken = {
        orderBy: [
          { column: 'a', direction: 'desc' },
          { column: 'b', direction: 'desc' },
          { column: 'c', direction: 'desc' },
        ],
        values: [1, 2, 3],
        direction: 'next',
      }

      const result = buildCursorConditions(cursor)

      expect(result.text).toBe(
        '("a" < ?) OR ("a" = ? AND "b" < ?) OR ("a" = ? AND "b" = ? AND "c" < ?)',
      )
      expect(result.values).toEqual([1, 1, 2, 1, 2, 3])
    })
  })

  describe('getDefaultOrderBy', () => {
    it('should return default order by when provided', () => {
      const defaultOrder: OrderBy = { column: 'created_at', direction: 'desc' }
      const result = getDefaultOrderBy('id', defaultOrder)

      expect(result).toEqual([defaultOrder])
    })

    it('should create order by from single primary key', () => {
      const result = getDefaultOrderBy('id')

      expect(result).toEqual([{ column: 'id', direction: 'asc' }])
    })

    it('should create order by from composite primary key', () => {
      const result = getDefaultOrderBy(['user_id', 'team_id'])

      expect(result).toEqual([
        { column: 'user_id', direction: 'asc' },
        { column: 'team_id', direction: 'asc' },
      ])
    })
  })

  describe('validateOrderBy', () => {
    it('should pass validation for allowed columns', () => {
      const orderBy: OrderBy[] = [
        { column: 'id', direction: 'asc' },
        { column: 'name', direction: 'desc' },
      ]
      const allowedColumns = ['id', 'name', 'email']

      expect(() => validateOrderBy(orderBy, allowedColumns)).not.toThrow()
    })

    it('should throw CursorError for disallowed columns', () => {
      const orderBy: OrderBy[] = [
        { column: 'id', direction: 'asc' },
        { column: 'forbidden', direction: 'desc' },
      ]
      const allowedColumns = ['id', 'name', 'email']

      expect(() => validateOrderBy(orderBy, allowedColumns)).toThrow(
        CursorError,
      )
    })
  })

  describe('keyset pagination correctness (regression)', () => {
    it('should page through duplicate values in the first sort column without skipping or repeating rows', () => {
      // Reproduces the real-world failure mode: ORDER BY a DESC, id DESC where
      // `a` (e.g. created_at) repeats across rows and `id` is the unique
      // tiebreaker. With the bug (`a < ? OR id < ?`), a row from a different
      // `a` group could satisfy `id < cursor_id` and leak onto the page, or a
      // row could be skipped, and paging could fail to terminate.
      const bunDb = new BunDatabase(':memory:')
      bunDb.exec(`
        CREATE TABLE items (
          id INTEGER PRIMARY KEY,
          a INTEGER NOT NULL
        );
      `)

      // Deliberately many duplicate `a` values, with `id` NOT correlated with
      // `a` (ids are assigned out of `a` order within and across groups). If
      // `id` happened to increase in step with `a`, sorting by `id` alone
      // would coincide with sorting by `(a, id)` and the bug's dropped
      // equality prefix (effectively `a < ? OR id < ?`) would never surface —
      // this shape is required to actually exercise the bug.
      const rows: Array<{ id: number; a: number }> = [
        { id: 5, a: 1 },
        { id: 2, a: 1 },
        { id: 8, a: 1 },
        { id: 1, a: 2 },
        { id: 9, a: 2 },
        { id: 3, a: 3 },
        { id: 7, a: 3 },
        { id: 4, a: 3 },
        { id: 10, a: 3 },
        { id: 6, a: 4 },
      ]
      const insert = bunDb.prepare('INSERT INTO items (id, a) VALUES (?, ?)')
      for (const row of rows) insert.run(row.id, row.a)

      const orderBy: OrderBy[] = [
        { column: 'a', direction: 'desc' },
        { column: 'id', direction: 'desc' },
      ]
      const expectedOrder = [...rows]
        .sort((x, y) => y.a - x.a || y.id - x.id)
        .map((r) => r.id)

      const pageSize = 3
      const seen: number[] = []
      let cursor: CursorToken | undefined
      let iterations = 0
      // Bound the loop generously so a non-terminating bug fails the test
      // instead of hanging the suite.
      const maxIterations = Math.ceil(rows.length / pageSize) + 3

      while (iterations < maxIterations) {
        iterations++

        const conditions = cursor
          ? buildCursorConditions(cursor)
          : { text: '', values: [] as unknown[] }
        const whereClause = conditions.text ? `WHERE ${conditions.text}` : ''
        const query = bunDb.prepare(
          `SELECT id, a FROM items ${whereClause} ORDER BY a DESC, id DESC LIMIT ${pageSize}`,
        )
        const page = query.all(...conditions.values) as Array<{
          id: number
          a: number
        }>

        if (page.length === 0) break

        seen.push(...page.map((r) => r.id))

        const last = page[page.length - 1]!
        cursor = createCursor(last, orderBy, 'next')
      }

      expect(iterations).toBeLessThan(maxIterations)
      expect(seen).toEqual(expectedOrder)
      expect(new Set(seen).size).toBe(seen.length) // no repeats
    })
  })

  describe('reverseOrderBy', () => {
    it('should reverse asc to desc', () => {
      const orderBy: OrderBy[] = [{ column: 'id', direction: 'asc' }]
      const result = reverseOrderBy(orderBy)

      expect(result).toEqual([{ column: 'id', direction: 'desc' }])
    })

    it('should reverse desc to asc', () => {
      const orderBy: OrderBy[] = [{ column: 'name', direction: 'desc' }]
      const result = reverseOrderBy(orderBy)

      expect(result).toEqual([{ column: 'name', direction: 'asc' }])
    })

    it('should reverse multiple order by clauses', () => {
      const orderBy: OrderBy[] = [
        { column: 'name', direction: 'desc' },
        { column: 'id', direction: 'asc' },
      ]
      const result = reverseOrderBy(orderBy)

      expect(result).toEqual([
        { column: 'name', direction: 'asc' },
        { column: 'id', direction: 'desc' },
      ])
    })
  })

  describe('SQL Injection Security Tests', () => {
    it('should reject malicious column names', () => {
      const maliciousColumns = [
        'id; DROP TABLE users; --',
        'id" OR 1=1 --',
        "id'; DELETE FROM users; --",
        'id UNION SELECT * FROM passwords',
        'id/**/OR/**/1=1',
        "id'; EXEC xp_cmdshell('dir'); --",
      ]

      for (const maliciousColumn of maliciousColumns) {
        const cursor: CursorToken = {
          orderBy: [{ column: maliciousColumn, direction: 'asc' }],
          values: [10],
          direction: 'next',
        }

        // Should throw TypeError for invalid identifiers - this is the correct security behavior
        expect(() => buildCursorConditions(cursor)).toThrow(TypeError)
      }
    })

    it('should reject malicious table names', () => {
      const maliciousTables = [
        'users; DROP TABLE sessions; --',
        'users" UNION SELECT * FROM passwords --',
        "users'; DELETE FROM logs; --",
        'users/**/OR/**/1=1',
        "users'; EXEC xp_cmdshell('rm -rf /'); --",
      ]

      for (const maliciousTable of maliciousTables) {
        const cursor: CursorToken = {
          orderBy: [{ column: 'id', direction: 'asc' }],
          values: [10],
          direction: 'next',
        }

        // Should throw TypeError for invalid identifiers - this is the correct security behavior
        expect(() => buildCursorConditions(cursor, maliciousTable)).toThrow(
          TypeError,
        )
      }
    })

    it('should properly parameterize values to prevent injection', () => {
      const maliciousValues = [
        "'; DROP TABLE users; --",
        '" OR 1=1 --',
        '1; DELETE FROM sessions; --',
        '1 UNION SELECT password FROM users',
        '1/**/OR/**/1=1',
      ]

      for (const maliciousValue of maliciousValues) {
        const cursor: CursorToken = {
          orderBy: [{ column: 'id', direction: 'asc' }],
          values: [maliciousValue],
          direction: 'next',
        }

        const result = buildCursorConditions(cursor)

        // Values should be parameterized, not directly in SQL
        expect(result.text).not.toContain('DROP TABLE')
        expect(result.text).not.toContain('DELETE FROM')
        expect(result.text).not.toContain('UNION SELECT')
        expect(result.text).not.toContain('--')
        expect(result.values).toContain(maliciousValue)
      }
    })

    it('should reject special SQL characters in identifiers', () => {
      const specialChars = [
        'column"name',
        "column'name",
        'column;name',
        'column\nname',
        'column\tname',
        'column\rname',
        'column/*comment*/name',
        'column--comment',
      ]

      for (const specialChar of specialChars) {
        const cursor: CursorToken = {
          orderBy: [{ column: specialChar, direction: 'asc' }],
          values: [10],
          direction: 'next',
        }

        // Should throw TypeError for invalid identifiers containing special chars
        expect(() => buildCursorConditions(cursor)).toThrow(TypeError)
      }
    })

    it('should reject Unicode and special characters in identifiers', () => {
      const unicodeColumns = [
        'مستخدم', // Arabic
        '用户', // Chinese
        'пользователь', // Russian
        'ユーザー', // Japanese
        'emoji🔥test',
        'column\u0000null', // Null byte
        'column\uFEFFbom', // BOM character
      ]

      for (const unicodeColumn of unicodeColumns) {
        const cursor: CursorToken = {
          orderBy: [{ column: unicodeColumn, direction: 'asc' }],
          values: [10],
          direction: 'next',
        }

        // Should throw TypeError for non-ASCII identifiers as they don't match ANSI SQL identifier rules
        expect(() => buildCursorConditions(cursor)).toThrow(TypeError)
      }
    })

    it('should reject complex multi-column injection attempts', () => {
      const cursor: CursorToken = {
        orderBy: [
          { column: 'id"; DROP TABLE users; --', direction: 'asc' },
          { column: "name'; DELETE FROM logs; --", direction: 'desc' },
        ],
        values: ["'; UNION SELECT password FROM secrets; --", 999],
        direction: 'next',
      }

      // Should throw TypeError for invalid identifiers in both columns and table name
      expect(() =>
        buildCursorConditions(cursor, 'users"; DROP TABLE sessions; --'),
      ).toThrow(TypeError)
    })

    it('should work correctly with valid identifiers', () => {
      const validColumns = [
        'id',
        'user_id',
        'created_at',
        'name123',
        'table1',
        'column_name_with_underscores',
      ]

      for (const validColumn of validColumns) {
        const cursor: CursorToken = {
          orderBy: [{ column: validColumn, direction: 'asc' }],
          values: [10],
          direction: 'next',
        }

        // Should work fine with valid identifiers
        const result = buildCursorConditions(cursor, 'users')
        expect(result.text).toContain(`"users"."${validColumn}"`)
        expect(result.text).toContain('>')
        expect(result.values).toEqual([10])
      }
    })

    it('should maintain SQL structure integrity with valid inputs', () => {
      const cursor: CursorToken = {
        orderBy: [{ column: 'id', direction: 'asc' }],
        values: [10],
        direction: 'next',
      }

      const result = buildCursorConditions(cursor, 'valid_table')

      // Should produce valid SQL structure
      expect(result.text).toMatch(/^"[^"]*"\."[^"]*"\s*>\s*\?$/)
      expect(result.values).toEqual([10])
    })
  })
})

describe('cursor encoding of non-ASCII values', () => {
  // The pre-fix encoder, verbatim. Every cursor currently in flight was
  // produced by this, so the new encoder must agree with it on every input it
  // accepts — the inputs it rejects are the bug.
  const legacyEncode = (token: CursorToken): string =>
    btoa(JSON.stringify(token))

  const tokenFor = (value: unknown): CursorToken =>
    ({
      orderBy: [{ column: 'name', direction: 'asc' }],
      values: [value],
      direction: 'next',
    }) as CursorToken

  describe('compatibility with cursors already in flight', () => {
    it('is byte-for-byte identical for every code unit btoa accepts (U+0000-U+00FF)', () => {
      // This is the crux: latin1 characters above U+007F encode as ONE byte
      // under the existing scheme, and btoa accepts them, so such cursors are
      // live today. Any scheme that re-encodes them (UTF-8 would make them two
      // bytes) silently changes the value a live cursor decodes to.
      for (let code = 0; code <= 0xff; code++) {
        const token = tokenFor(`a${String.fromCharCode(code)}z`)
        expect(encodeCursor(token)).toBe(legacyEncode(token))
      }
    })

    it('is byte-for-byte identical for the whole latin1 range at once', () => {
      const latin1 = Array.from({ length: 256 }, (_, i) =>
        String.fromCharCode(i),
      ).join('')
      const token = tokenFor(latin1)

      expect(encodeCursor(token)).toBe(legacyEncode(token))
      expect(decodeCursor(encodeCursor(token)).values[0]).toBe(latin1)
    })

    it('is byte-for-byte identical for realistic values', () => {
      for (const value of [
        'alice',
        'José',
        'Ã©',
        'Müller',
        'ñ',
        'a/b+c=d',
        '',
        null,
        42,
        true,
      ]) {
        const token = tokenFor(value)
        expect(encodeCursor(token)).toBe(legacyEncode(token))
      }
    })

    it('decodes cursors produced by the previous encoder unchanged', () => {
      for (const value of ['alice', 'José', 'Ã©', 'a/b+c=d']) {
        const token = tokenFor(value)
        expect(decodeCursor(legacyEncode(token)).values[0]).toBe(value)
      }
    })
  })

  describe('values the previous encoder could not encode', () => {
    // Regression: cursors carry the values of the column being sorted on, so
    // paginating over a text column holding any of these used to 500 on page 2.
    const nonLatin1: Array<[string, string]> = [
      ['CJK', '张伟'],
      ['Japanese', '日本語のツールボックス'],
      ['emoji', 'prod 🔐 connection'],
      ['Cyrillic', 'Привет'],
      ['mixed', 'José 日本 🔐'],
      ['astral only', '😀😀😀'],
    ]

    it.each(nonLatin1)('previously threw on %s', (_name, value) => {
      expect(() => legacyEncode(tokenFor(value))).toThrow()
    })

    it.each(nonLatin1)('now round-trips %s', (_name, value) => {
      const token = tokenFor(value)
      const encoded = encodeCursor(token)

      expect(encoded).toMatch(/^[A-Za-z0-9+/]*={0,2}$/)
      expect(decodeCursor(encoded).values[0]).toBe(value)
    })

    it('round-trips a long mixed-script value', () => {
      const long = '日本語😀José-'.repeat(4000)
      expect(long.length).toBeGreaterThan(0x8000)

      const token = tokenFor(long)
      expect(decodeCursor(encodeCursor(token)).values[0]).toBe(long)
    })

    it('round-trips multi-column cursors with non-ASCII values', () => {
      const token: CursorToken = {
        orderBy: [
          { column: 'name', direction: 'asc' },
          { column: 'id', direction: 'desc' },
        ],
        values: ['日本 🔐', 'ID-Ünïcødé'],
        direction: 'prev',
      }

      expect(decodeCursor(encodeCursor(token))).toEqual(token)
    })

    it('still rejects tampered payloads', () => {
      // The escaping only fires inside JSON string literals, so it must not
      // weaken the structural validation the decoder relies on.
      const smuggled = btoa(
        JSON.stringify({
          orderBy: [{ column: 'name', direction: 'asc' }],
          values: [{ text: 'DROP TABLE users', values: [] }],
          direction: 'next',
        }),
      )

      expect(() => decodeCursor(smuggled)).toThrow(CursorError)
    })
  })
})
