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
