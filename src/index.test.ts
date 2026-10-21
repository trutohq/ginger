import { Database as BunDatabase } from 'bun:sqlite'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { z as z3 } from 'zod/v3'
import { z } from 'zod/v4'
import { fromBunSqlite } from './adapters/bun-sqlite.js'
import {
  decrypt,
  decryptSecrets,
  DefaultKeyProvider,
  encrypt,
  encryptSecrets,
  generateSecretKey,
} from './crypto.js'
import {
  CursorError,
  EncryptionError,
  NotFoundError,
  ValidationError,
} from './errors.js'
import { createService, Service } from './index.js'
import {
  buildCursorConditions,
  decodeCursor,
  encodeCursor,
} from './pagination.js'
import {
  normalizeInclude,
  resolveActiveJoins,
  translateWhereForJoins,
} from './joins.js'
import type { Database } from './types.js'

describe('Ginger Library - Comprehensive Tests', () => {
  let bunDb: BunDatabase
  let db: Database
  let testService: Service<any, any, any, any, any>
  let testSecretKey: string

  // Test schemas - using snake_case for database consistency
  // rowSchema must NOT include secret fields (neither columnName nor logicalName).
  // Secret columns are managed separately by the secrets config.
  const UserRowSchema = z.object({
    id: z.number(),
    name: z.string(),
    email: z.string(),
    tenant_id: z.string(),
    created_at: z.string(),
    updated_at: z.string().nullable().optional(),
  })

  const UserCreateSchema = z.object({
    name: z.string().min(1),
    email: z.string().email(),
    apiKey: z.string().optional(),
    tenant_id: z.string(),
  })

  const UserUpdateSchema = z.object({
    name: z.string().optional(),
    email: z.string().email().optional(),
    apiKey: z.string().optional(),
  })

  const TeamRowSchema = z.object({
    id: z.number(),
    name: z.string(),
    description: z.string(),
  })

  // Join definitions for testing
  const userJoins = {
    teams: {
      kind: 'many' as const,
      localPk: 'id',
      through: {
        table: 'user_teams',
        from: 'user_id',
        to: 'team_id',
      },
      remote: {
        table: 'teams',
        pk: 'id',
        select: ['id', 'name', 'description'],
        alias: 'team',
      },
      schema: TeamRowSchema,
    },
    profile: {
      kind: 'one' as const,
      localPk: 'id',
      remote: {
        table: 'profiles',
        pk: 'user_id',
        select: ['bio', 'avatar'],
      },
      schema: z.object({
        bio: z.string(),
        avatar: z.string(),
      }),
    },
  }

  // Secret field definitions for testing
  const userSecrets = [
    {
      logicalName: 'apiKey',
      columnName: 'api_key_encrypted',
      keyId: 'user-secrets',
    },
  ] as const

  beforeEach(() => {
    // Create in-memory SQLite database and wrap with adapter
    bunDb = new BunDatabase(':memory:')
    db = fromBunSqlite(bunDb)

    // Generate test encryption key
    testSecretKey = 'MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=' // base64 encoded 32-byte key

    // Set up test schema
    bunDb.exec(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT NOT NULL UNIQUE,
        api_key_encrypted TEXT,
        tenant_id TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT
      );

      CREATE TABLE teams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT
      );

      CREATE TABLE user_teams (
        user_id INTEGER NOT NULL,
        team_id INTEGER NOT NULL,
        PRIMARY KEY (user_id, team_id),
        FOREIGN KEY (user_id) REFERENCES users(id),
        FOREIGN KEY (team_id) REFERENCES teams(id)
      );

      CREATE TABLE profiles (
        user_id INTEGER PRIMARY KEY,
        bio TEXT,
        avatar TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
      );
    `)

    // Insert test data
    bunDb.exec(`
      INSERT INTO teams (id, name, description) VALUES 
        (1, 'Team Alpha', 'First team'),
        (2, 'Team Beta', 'Second team');

      INSERT INTO users (id, name, email, tenant_id, created_at) VALUES
        (1, 'John Doe', 'john@test.com', 'tenant-1', '2024-01-01T00:00:00Z'),
        (2, 'Jane Smith', 'jane@test.com', 'tenant-1', '2024-01-02T00:00:00Z'),
        (3, 'Bob Wilson', 'bob@test.com', 'tenant-2', '2024-01-03T00:00:00Z');

      INSERT INTO user_teams (user_id, team_id) VALUES
        (1, 1),
        (2, 1),
        (2, 2);

      INSERT INTO profiles (user_id, bio, avatar) VALUES
        (1, 'Software engineer', 'avatar1.jpg'),
        (2, 'Product manager', 'avatar2.jpg');
    `)

    // Create basic service with encryption keys
    testService = createService({
      table: 'users',
      db,
      rowSchema: UserRowSchema,
      createSchema: UserCreateSchema,
      updateSchema: UserUpdateSchema,
      timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
      encryptionKeys: {
        default: testSecretKey,
        'user-secrets': testSecretKey,
      },
    })
  })

  afterEach(() => {
    bunDb.close()
  })

  describe('Service Creation', () => {
    it('should create a service with basic configuration', () => {
      expect(testService).toBeDefined()
      expect(testService.table).toBe('users')
      expect(testService.primaryKey).toBe('id')
    })

    it('should create a service with custom primary key', () => {
      const service = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        primaryKey: 'uuid',
      })
      expect(service.primaryKey).toBe('uuid')
    })

    it('should create a service with composite primary key', () => {
      const service = createService({
        table: 'user_teams',
        db,
        rowSchema: z.object({ userId: z.number(), teamId: z.number() }),
        createSchema: z.object({ userId: z.number(), teamId: z.number() }),
        updateSchema: z.object({}),
        primaryKey: ['user_id', 'team_id'],
      })
      expect(service.primaryKey).toEqual(['user_id', 'team_id'])
    })

    it('should create a service with joins configuration', () => {
      const serviceWithJoins = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        joins: userJoins,
      })
      expect(serviceWithJoins.joins).toEqual(userJoins)
    })

    it('should create a service with secrets configuration', () => {
      const serviceWithSecrets = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        secrets: userSecrets,
      })
      expect(serviceWithSecrets.secrets).toEqual(userSecrets)
    })
  })

  describe('CRUD Operations', () => {
    describe('list()', () => {
      it('should list all records', async () => {
        const result = await testService.list({ auth: {} })

        expect(result.result).toEqual([
          {
            id: 1,
            name: 'John Doe',
            email: 'john@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-01T00:00:00Z',
            updated_at: null,
          },
          {
            id: 2,
            name: 'Jane Smith',
            email: 'jane@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-02T00:00:00Z',
            updated_at: null,
          },
          {
            id: 3,
            name: 'Bob Wilson',
            email: 'bob@test.com',
            tenant_id: 'tenant-2',
            created_at: '2024-01-03T00:00:00Z',
            updated_at: null,
          },
        ])
        expect(result.nextCursor).toBeUndefined()
        expect(result.prevCursor).toBeUndefined()
      })

      it('should support pagination with limit', async () => {
        const result = await testService.list({ auth: {}, limit: 2 })

        expect(result.result).toEqual([
          {
            id: 1,
            name: 'John Doe',
            email: 'john@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-01T00:00:00Z',
            updated_at: null,
          },
          {
            id: 2,
            name: 'Jane Smith',
            email: 'jane@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-02T00:00:00Z',
            updated_at: null,
          },
        ])
        expect(result.nextCursor).toBeDefined()
        expect(result.prevCursor).toBeUndefined()
      })

      it('should support filtering with where clause', async () => {
        const result = await testService.list({
          auth: {},
          where: { tenant_id: 'tenant-1' },
        })

        expect(result.result).toEqual([
          {
            id: 1,
            name: 'John Doe',
            email: 'john@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-01T00:00:00Z',
            updated_at: null,
          },
          {
            id: 2,
            name: 'Jane Smith',
            email: 'jane@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-02T00:00:00Z',
            updated_at: null,
          },
        ])
        expect(result.nextCursor).toBeUndefined()
        expect(result.prevCursor).toBeUndefined()
      })

      it('should support custom ordering', async () => {
        const result = await testService.list({
          auth: {},
          orderBy: [{ column: 'name', direction: 'desc' }],
        })

        expect(result.result).toEqual([
          {
            id: 1,
            name: 'John Doe',
            email: 'john@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-01T00:00:00Z',
            updated_at: null,
          },
          {
            id: 2,
            name: 'Jane Smith',
            email: 'jane@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-02T00:00:00Z',
            updated_at: null,
          },
          {
            id: 3,
            name: 'Bob Wilson',
            email: 'bob@test.com',
            tenant_id: 'tenant-2',
            created_at: '2024-01-03T00:00:00Z',
            updated_at: null,
          },
        ])
      })

      it('should validate limit parameter', async () => {
        await expect(
          testService.list({ auth: {}, limit: 2000 }),
        ).rejects.toThrow(ValidationError)
      })
    })

    describe('get()', () => {
      it('should get a record by ID', async () => {
        const result = await testService.get(1, { auth: {} })

        expect(result).toEqual({
          id: 1,
          name: 'John Doe',
          email: 'john@test.com',
          tenant_id: 'tenant-1',
          created_at: '2024-01-01T00:00:00Z',
          updated_at: null,
        })
      })

      it('should return null for non-existent record', async () => {
        const result = await testService.get(999, { auth: {} })
        expect(result).toBeNull()
      })
    })

    describe('create()', () => {
      it('should create a new record', async () => {
        const result = await testService.create(
          {
            name: 'Alice Johnson',
            email: 'alice@test.com',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        )

        expect(result).toEqual({
          id: 4,
          name: 'Alice Johnson',
          email: 'alice@test.com',
          tenant_id: 'tenant-1',
          created_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
          updated_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
        })
      })

      it('should validate create data', async () => {
        await expect(
          testService.create(
            {
              name: '',
              email: 'invalid-email',
              tenant_id: 'tenant-1',
            } as any,
            { auth: {} },
          ),
        ).rejects.toThrow()
      })

      it('should enforce unique constraints', async () => {
        await expect(
          testService.create(
            {
              name: 'Duplicate Email',
              email: 'john@test.com', // Already exists
              tenant_id: 'tenant-1',
            },
            { auth: {} },
          ),
        ).rejects.toThrow()
      })
    })

    describe('update()', () => {
      it('should update an existing record', async () => {
        const result = await testService.update(
          1,
          {
            name: 'John Updated',
          },
          { auth: {} },
        )

        expect(result).toEqual({
          id: 1,
          name: 'John Updated',
          email: 'john@test.com',
          tenant_id: 'tenant-1',
          created_at: '2024-01-01T00:00:00Z',
          updated_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
        })
      })

      it('should throw NotFoundError for non-existent record', async () => {
        await expect(
          testService.update(999, { name: 'Updated' }, { auth: {} }),
        ).rejects.toThrow(NotFoundError)
      })

      it('should validate update data', async () => {
        await expect(
          testService.update(1, { email: 'invalid' } as any, { auth: {} }),
        ).rejects.toThrow()
      })
    })

    describe('delete()', () => {
      it('should delete an existing record', async () => {
        const result = await testService.delete(3, { auth: {} })
        expect(result).toBe(true)

        // Verify deletion
        const deleted = await testService.get(3, { auth: {} })
        expect(deleted).toBeNull()
      })

      it('should throw NotFoundError for non-existent record', async () => {
        await expect(testService.delete(999, { auth: {} })).rejects.toThrow(
          NotFoundError,
        )
      })
    })

    describe('count()', () => {
      it('should count all records', async () => {
        const result = await testService.count({ auth: {} })
        expect(result).toBe(3)
      })

      it('should count with where clause', async () => {
        const result = await testService.count({
          auth: {},
          where: { tenant_id: 'tenant-1' },
        })
        expect(result).toBe(2)
      })
    })

    describe('query()', () => {
      it('should execute custom SQL', async () => {
        const result = await testService.query(
          'SELECT * FROM users WHERE tenant_id = ? ORDER BY id',
          { auth: {} },
          'tenant-1',
        )

        expect(result).toEqual([
          {
            id: 1,
            name: 'John Doe',
            email: 'john@test.com',
            api_key_encrypted: null,
            tenant_id: 'tenant-1',
            created_at: '2024-01-01T00:00:00Z',
            updated_at: null,
          },
          {
            id: 2,
            name: 'Jane Smith',
            email: 'jane@test.com',
            api_key_encrypted: null,
            tenant_id: 'tenant-1',
            created_at: '2024-01-02T00:00:00Z',
            updated_at: null,
          },
        ])
      })
    })
  })

  describe('Pagination System', () => {
    describe('Cursor encoding/decoding', () => {
      it('should encode and decode cursor tokens correctly', () => {
        const token = {
          orderBy: [{ column: 'id', direction: 'asc' as const }],
          values: [10],
          direction: 'next' as const,
        }

        const encoded = encodeCursor(token)
        expect(typeof encoded).toBe('string')

        const decoded = decodeCursor(encoded)
        expect(decoded).toEqual(token)
      })

      it('should handle complex ordering in cursors', () => {
        const token = {
          orderBy: [
            { column: 'name', direction: 'desc' as const },
            { column: 'id', direction: 'asc' as const },
          ],
          values: ['John', 5],
          direction: 'prev' as const,
        }

        const encoded = encodeCursor(token)
        const decoded = decodeCursor(encoded)
        expect(decoded).toEqual(token)
      })

      it('should throw CursorError for invalid cursor', () => {
        expect(() => decodeCursor('invalid-cursor')).toThrow(CursorError)
        expect(() => decodeCursor('eyJpbnZhbGlkIjoidG9rZW4ifQ==')).toThrow(
          CursorError,
        )
      })
    })

    describe('Cursor-based pagination', () => {
      it('should paginate forward correctly', async () => {
        // Get first page
        const page1 = await testService.list({ auth: {}, limit: 2 })
        expect(page1.result).toEqual([
          {
            id: 1,
            name: 'John Doe',
            email: 'john@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-01T00:00:00Z',
            updated_at: null,
          },
          {
            id: 2,
            name: 'Jane Smith',
            email: 'jane@test.com',
            tenant_id: 'tenant-1',
            created_at: '2024-01-02T00:00:00Z',
            updated_at: null,
          },
        ])
        expect(page1.nextCursor).toBeDefined()
        expect(page1.prevCursor).toBeUndefined()

        // Get second page
        if (page1.nextCursor) {
          const page2 = await testService.list({
            auth: {},
            cursor: page1.nextCursor,
            limit: 2,
          })
          expect(page2.result).toEqual([
            {
              id: 3,
              name: 'Bob Wilson',
              email: 'bob@test.com',
              tenant_id: 'tenant-2',
              created_at: '2024-01-03T00:00:00Z',
              updated_at: null,
            },
          ])
          expect(page2.nextCursor).toBeUndefined()
          expect(page2.prevCursor).toBeDefined()
        }
      })

      it('should paginate over non-latin1 names without throwing', async () => {
        // Regression: the cursor carries the value of the column being sorted
        // on, so ordering by a name containing CJK/emoji used to throw
        // `InvalidCharacterError` out of btoa and 500 the request on page 2.
        const names = ['Ana José', 'Привет', '张伟', '日本語 🔐', 'émile']
        for (const [i, name] of names.entries()) {
          await testService.create(
            {
              name,
              email: `unicode-${i}@test.com`,
              tenant_id: 'tenant-unicode',
            },
            { auth: {} },
          )
        }

        const orderBy = [{ column: 'name' as const, direction: 'asc' as const }]
        const seen: string[] = []
        let cursor: string | undefined

        // Walk every page; each hop encodes a cursor from the previous row's name.
        for (let page = 0; page < 10; page++) {
          const res = await testService.list({
            auth: {},
            where: { tenant_id: 'tenant-unicode' },
            orderBy,
            limit: 2,
            ...(cursor ? { cursor } : {}),
          })
          seen.push(...res.result.map((r: { name: string }) => r.name))
          if (!res.nextCursor) break
          cursor = res.nextCursor
        }

        expect(seen.sort()).toEqual([...names].sort())
      })

      it('pages through a column the row schema lets hold NULL, both orders', async () => {
        // `updated_at` is nullable in `UserRowSchema`. Ordered by it, a page
        // ending on a NULL stopped the listing (`updated_at = NULL` matches
        // nothing), and on DESC the NULL rows, which SQLite sorts last, were
        // never reached.
        const insert = bunDb.prepare(
          `INSERT INTO users (name, email, tenant_id, created_at, updated_at)
           VALUES (?, ?, 'tenant-null', '2024-01-01T00:00:00Z', ?)`,
        )
        const updatedAt = [
          '2024-02-02',
          null,
          '2024-02-01',
          null,
          '2024-02-03',
          null,
          '2024-02-01',
        ]
        for (const [i, value] of updatedAt.entries()) {
          insert.run(`User ${i}`, `null-${i}@test.com`, value)
        }

        for (const direction of ['asc', 'desc'] as const) {
          const expected = (
            bunDb
              .prepare(
                `SELECT id FROM users WHERE tenant_id = 'tenant-null'
                 ORDER BY updated_at ${direction.toUpperCase()}, id ${direction.toUpperCase()}`,
              )
              .all() as Array<{ id: number }>
          ).map((r) => r.id)

          const seen: number[] = []
          let cursor: string | undefined
          for (let page = 0; page < 10; page++) {
            const res = await testService.list({
              auth: {},
              where: { tenant_id: 'tenant-null' },
              orderBy: [
                { column: 'updated_at', direction },
                { column: 'id', direction },
              ],
              limit: 2,
              ...(cursor ? { cursor } : {}),
            })
            seen.push(...res.result.map((r: { id: number }) => r.id))
            if (!res.nextCursor) break
            cursor = res.nextCursor
          }

          expect(seen, direction).toEqual(expected)
        }
      })

      it('pages exactly on a field the row schema reads from NULL as a value', async () => {
        // The field accepts NULL but reads it as ''. With the cursor built
        // from the parsed row, its DESC condition `(col < '' OR col IS NULL)`
        // met the stored NULL rows on every page and the listing never ended;
        // counted as not nullable instead, the NULL rows after a non-NULL
        // boundary were skipped. The cursor now holds the stored value.
        const blankService = createService({
          table: 'users',
          db,
          rowSchema: UserRowSchema.extend({
            updated_at: z
              .string()
              .nullable()
              .transform((value) => value ?? ''),
          }),
          createSchema: UserCreateSchema,
          updateSchema: UserUpdateSchema,
        })
        const insert = bunDb.prepare(
          `INSERT INTO users (name, email, tenant_id, created_at, updated_at)
           VALUES (?, ?, 'tenant-blank', '2024-01-01T00:00:00Z', ?)`,
        )
        for (const [i, value] of ['2024-02-01', null, null, null].entries()) {
          insert.run(`Blank ${i}`, `blank-${i}@test.com`, value)
        }
        const expected = (
          bunDb
            .prepare(
              `SELECT id FROM users WHERE tenant_id = 'tenant-blank'
               ORDER BY updated_at DESC, id DESC`,
            )
            .all() as Array<{ id: number }>
        ).map((r) => r.id)

        const seen: number[] = []
        let cursor: string | undefined
        for (let page = 0; page < 20; page++) {
          const res = await blankService.list({
            auth: {},
            where: { tenant_id: 'tenant-blank' },
            orderBy: [
              { column: 'updated_at', direction: 'desc' },
              { column: 'id', direction: 'desc' },
            ],
            limit: 1,
            ...(cursor ? { cursor } : {}),
          })
          seen.push(...res.result.map((r: { id: number }) => r.id))
          if (!res.nextCursor) break
          cursor = res.nextCursor
        }

        expect(seen).toEqual(expected)
      })

      const fillUpdatedAt = (row: z.infer<typeof UserRowSchema>) => ({
        ...row,
        updated_at: row.updated_at ?? '',
      })

      it.each([
        ['.transform()', () => UserRowSchema.transform(fillUpdatedAt)],
        [
          '.transform().nullable()',
          () => UserRowSchema.transform(fillUpdatedAt).nullable(),
        ],
        [
          '.transform().readonly()',
          () => UserRowSchema.transform(fillUpdatedAt).readonly(),
        ],
        [
          'z.lazy()',
          () => z.lazy(() => UserRowSchema.transform(fillUpdatedAt)),
        ],
        ['.overwrite()', () => UserRowSchema.overwrite(fillUpdatedAt)],
      ])(
        'ends paging when the row schema (%s) reads NULL as a value',
        async (_label, rowSchema) => {
          // Read off the whole row, not a field, the stored NULLs became '':
          // the same endless DESC listing as a field-level transform.
          const rowService = createService({
            table: 'users',
            db,
            rowSchema: rowSchema() as unknown as typeof UserRowSchema,
            createSchema: UserCreateSchema,
            updateSchema: UserUpdateSchema,
          })
          const tenant = `tenant-row-${_label}`
          const insert = bunDb.prepare(
            `INSERT INTO users (name, email, tenant_id, created_at, updated_at)
             VALUES (?, ?, ?, '2024-01-01T00:00:00Z', ?)`,
          )
          for (const [i, value] of ['2024-02-01', null, null, null].entries()) {
            insert.run(`Row ${i}`, `row-${_label}-${i}@test.com`, tenant, value)
          }

          const seen: number[] = []
          let cursor: string | undefined
          let pages = 0
          for (; pages < 20; pages++) {
            const res = await rowService.list({
              auth: {},
              where: { tenant_id: tenant },
              orderBy: [
                { column: 'updated_at', direction: 'desc' },
                { column: 'id', direction: 'desc' },
              ],
              limit: 1,
              ...(cursor ? { cursor } : {}),
            })
            seen.push(...res.result.map((r: { id: number }) => r.id))
            if (!res.nextCursor) break
            cursor = res.nextCursor
          }

          expect(pages).toBeLessThan(20)
          expect(new Set(seen).size).toBe(seen.length)
          // Every row, the NULL ones included.
          expect(seen).toHaveLength(4)
        },
      )

      // The same row schemas in zod's v3 API, which `import { z } from 'zod'`
      // still gives on zod 3.x. Anything but a plain v3 `z.object` has no
      // `.shape`, so no field is read on its own and every column pages
      // NULL-aware, over the stored values the cursor carries.
      const UserRowSchemaV3 = z3.object({
        id: z3.number(),
        name: z3.string(),
        email: z3.string(),
        tenant_id: z3.string(),
        created_at: z3.string(),
        updated_at: z3.string().nullable().optional(),
      })
      const fillUpdatedAtV3 = (row: z3.infer<typeof UserRowSchemaV3>) => ({
        ...row,
        updated_at: row.updated_at ?? '',
      })

      // Pages a DESC listing of one NULL-dated row after another, as above.
      const pageV3 = async (label: string, rowSchema: unknown) => {
        const rowService = createService({
          table: 'users',
          db,
          rowSchema: rowSchema as unknown as typeof UserRowSchema,
          createSchema: UserCreateSchema,
          updateSchema: UserUpdateSchema,
        })
        const tenant = `tenant-v3-${label}`
        const insert = bunDb.prepare(
          `INSERT INTO users (name, email, tenant_id, created_at, updated_at)
           VALUES (?, ?, ?, '2024-01-01T00:00:00Z', ?)`,
        )
        for (const [i, value] of ['2024-02-01', null, null, null].entries()) {
          insert.run(`V3 ${i}`, `v3-${label}-${i}@test.com`, tenant, value)
        }

        const seen: number[] = []
        let cursor: string | undefined
        let pages = 0
        for (; pages < 20; pages++) {
          const res = await rowService.list({
            auth: {},
            where: { tenant_id: tenant },
            orderBy: [
              { column: 'updated_at', direction: 'desc' },
              { column: 'id', direction: 'desc' },
            ],
            limit: 1,
            ...(cursor ? { cursor } : {}),
          })
          seen.push(...res.result.map((r: { id: number }) => r.id))
          if (!res.nextCursor) break
          cursor = res.nextCursor
        }
        return { pages, seen }
      }

      it.each([
        ['.transform()', () => UserRowSchemaV3.transform(fillUpdatedAtV3)],
        [
          '.transform().nullable()',
          () => UserRowSchemaV3.transform(fillUpdatedAtV3).nullable(),
        ],
        [
          'z.preprocess()',
          () =>
            z3.preprocess(
              (row) => ({
                ...(row as object),
                updated_at:
                  (row as { updated_at?: string | null }).updated_at ?? '',
              }),
              UserRowSchemaV3,
            ),
        ],
      ])(
        'ends paging when a zod v3 row schema (%s) reads NULL as a value',
        async (label, rowSchema) => {
          const { pages, seen } = await pageV3(label, rowSchema())

          expect(pages).toBeLessThan(20)
          expect(new Set(seen).size).toBe(seen.length)
          // Every row, the NULL ones included.
          expect(seen).toHaveLength(4)
        },
      )

      it('ends paging when a field of a zod v3 row schema reads NULL as a value', async () => {
        // A plain v3 object keeps its `.shape`, so its fields are read one by
        // one (`columnAcceptsNull`).
        const { pages, seen } = await pageV3(
          'field',
          UserRowSchemaV3.extend({
            updated_at: z3
              .string()
              .nullable()
              .transform((value) => value ?? ''),
          }),
        )

        expect(pages).toBeLessThan(20)
        expect(new Set(seen).size).toBe(seen.length)
        expect(seen).toHaveLength(4)
      })

      it.each(['asc', 'desc'] as const)(
        'pages every row exactly once, %s, when a field reads a stored value as NULL',
        async (direction) => {
          // Built from the parsed row, a cursor held NULL for a stored value
          // the field reads as NULL (`'garbage'` through `.catch(null)`), and
          // the next page started over at the first non-NULL row, so an ASC
          // listing never ended. The cursor holds the stored value now.
          const rowService = createService({
            table: 'users',
            db,
            rowSchema: UserRowSchema.extend({
              updated_at: z.iso.date().nullable().catch(null),
            }),
            createSchema: UserCreateSchema,
            updateSchema: UserUpdateSchema,
          })
          const insert = bunDb.prepare(
            `INSERT INTO users (name, email, tenant_id, created_at, updated_at)
             VALUES (?, ?, 'tenant-garbage', '2024-01-01T00:00:00Z', ?)`,
          )
          for (let i = 0; i < 20; i++) {
            insert.run(
              `Garbage ${i}`,
              `garbage-${i}@test.com`,
              i % 3 === 0
                ? 'garbage'
                : `2024-01-${String(10 + i).padStart(2, '0')}`,
            )
          }
          const expected = (
            bunDb
              .prepare(
                `SELECT id FROM users WHERE tenant_id = 'tenant-garbage'
                 ORDER BY updated_at ${direction}, id ${direction}`,
              )
              .all() as Array<{ id: number }>
          ).map((r) => r.id)

          const seen: number[] = []
          let cursor: string | undefined
          for (let page = 0; page < 40; page++) {
            const res = await rowService.list({
              auth: {},
              where: { tenant_id: 'tenant-garbage' },
              orderBy: [
                { column: 'updated_at', direction },
                { column: 'id', direction },
              ],
              limit: 2,
              ...(cursor ? { cursor } : {}),
            })
            seen.push(...res.result.map((r: { id: number }) => r.id))
            if (!res.nextCursor) break
            cursor = res.nextCursor
          }

          expect(seen).toEqual(expected)
        },
      )

      it('counts every column NULL-aware under a row schema that rewrites the row, joined and exposed ones too', () => {
        // A cursor carries the stored value, so a column whose NULL the
        // schema may read as a value still pages NULL-aware. Join data and
        // exposed columns are added to the row after the row schema runs.
        const rowService = createService({
          table: 'users',
          db,
          rowSchema: UserRowSchema.transform(
            fillUpdatedAt,
          ) as unknown as typeof UserRowSchema,
          createSchema: UserCreateSchema,
          updateSchema: UserUpdateSchema,
          expose: [{ from: '$profile.bio', as: 'bio' }],
        } as never) as unknown as {
          columnAcceptsNull(column: string): boolean
        }

        expect(rowService.columnAcceptsNull('updated_at')).toBe(true)
        expect(rowService.columnAcceptsNull('$profile.bio')).toBe(true)
        expect(rowService.columnAcceptsNull('bio')).toBe(true)
      })

      it('keeps the caller where clause in force under a hand-built multi-column cursor', async () => {
        // Regression for the cursor-precedence fix. The cursor is caller-held
        // base64 JSON and its `orderBy` replaces the request's, so a client can
        // hand back any two-column cursor over allowlisted columns. Its OR-chain
        // is then ANDed with the caller's `where` in `buildSelect`; left
        // unparenthesised, the branch after the OR carried no `where` at all.
        // `pagination.test.ts` checks the brackets on the fragment — this checks
        // the rows that come back from a real database.
        const cursor = encodeCursor({
          orderBy: [
            { column: 'tenant_id', direction: 'asc' },
            { column: 'id', direction: 'asc' },
          ],
          values: ['tenant-2', 0],
          direction: 'next',
        })

        const scoped = await testService.list({
          auth: {},
          where: { tenant_id: 'tenant-1' },
          cursor,
          limit: 10,
        })
        expect(
          scoped.result.filter(
            (r: { tenant_id: string }) => r.tenant_id !== 'tenant-1',
          ),
        ).toEqual([])

        // Not vacuous: without the where, the same cursor does reach the
        // tenant-2 row, so the assertion above is the where clause at work.
        const unscoped = await testService.list({ auth: {}, cursor, limit: 10 })
        expect(
          unscoped.result.map((r: { tenant_id: string }) => r.tenant_id),
        ).toContain('tenant-2')
      })

      it('pages on a column the row schema parses into a Date, both orders', async () => {
        // Stored as SQLite writes CURRENT_TIMESTAMP (`YYYY-MM-DD HH:MM:SS`),
        // read by the row schema as a Date. The cursor carried the parsed
        // value, which serialises as ISO text (`…T…Z`), and the next page
        // compared that with the stored text. `T` sorts after a space, so DESC
        // served the last row of a page again and ASC skipped the rest of its
        // day.
        const datedService = createService({
          table: 'users',
          db,
          rowSchema: UserRowSchema.extend({ created_at: z.coerce.date() }),
          createSchema: UserCreateSchema,
          updateSchema: UserUpdateSchema,
        })
        const insert = bunDb.prepare(
          `INSERT INTO users (name, email, tenant_id, created_at)
           VALUES (?, ?, 'tenant-dated', ?)`,
        )
        const createdAt = [
          '2024-03-01 10:00:02',
          '2024-03-01 10:00:00',
          '2024-03-02 09:00:00',
          '2024-03-01 10:00:01',
          '2024-02-29 23:59:59',
        ]
        for (const [i, value] of createdAt.entries()) {
          insert.run(`Dated ${i}`, `dated-${i}@test.com`, value)
        }

        for (const direction of ['asc', 'desc'] as const) {
          const expected = (
            bunDb
              .prepare(
                `SELECT id FROM users WHERE tenant_id = 'tenant-dated'
                 ORDER BY created_at ${direction.toUpperCase()}`,
              )
              .all() as Array<{ id: number }>
          ).map((r) => r.id)

          const seen: number[] = []
          let cursor: string | undefined
          for (let page = 0; page < 10; page++) {
            const res = await datedService.list({
              auth: {},
              where: { tenant_id: 'tenant-dated' },
              orderBy: [{ column: 'created_at', direction }],
              limit: 2,
              ...(cursor ? { cursor } : {}),
            })
            seen.push(...res.result.map((r: { id: number }) => r.id))
            if (!res.nextCursor) break
            cursor = res.nextCursor
          }

          expect(seen, direction).toEqual(expected)
        }

        // The rows are still read through the schema.
        const first = await datedService.list({
          auth: {},
          where: { tenant_id: 'tenant-dated' },
          orderBy: [{ column: 'created_at', direction: 'desc' }],
          limit: 1,
        })
        expect(first.result[0].created_at).toBeInstanceOf(Date)
      })

      it('falls back to the parsed value for a stored value a cursor cannot carry', async () => {
        // bun:sqlite with `safeIntegers` returns INTEGER columns as bigint,
        // which JSON can't hold; the row schema reads it as a number.
        const bigDb = new BunDatabase(':memory:', { safeIntegers: true })
        bigDb.exec(
          `CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)`,
        )
        for (const name of ['a', 'b', 'c']) {
          bigDb.prepare('INSERT INTO items (name) VALUES (?)').run(name)
        }
        const itemService = createService({
          table: 'items',
          db: fromBunSqlite(bigDb),
          rowSchema: z.object({ id: z.coerce.number(), name: z.string() }),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
        })

        const seen: number[] = []
        let cursor: string | undefined
        for (let page = 0; page < 10; page++) {
          const res = await itemService.list({
            auth: {},
            limit: 1,
            ...(cursor ? { cursor } : {}),
          })
          seen.push(...res.result.map((r: { id: number }) => r.id))
          if (!res.nextCursor) break
          cursor = res.nextCursor
        }
        bigDb.close()

        expect(seen).toEqual([1, 2, 3])
      })

      it('builds no cursor over a column the response leaves out', async () => {
        // A cursor is readable, and its own orderBy orders the next page, so
        // one over a column the response drops would read that column out.
        const secretDb = new BunDatabase(':memory:')
        secretDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL, password_hash TEXT NOT NULL);
           INSERT INTO accounts (name, password_hash) VALUES ('a', 'hash-a'), ('b', 'hash-b');`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(secretDb),
          // A whole-object transform: no shape, and no `password_hash` out.
          rowSchema: z
            .object({
              id: z.number(),
              name: z.string(),
              password_hash: z.string(),
            })
            .transform(({ password_hash: _hash, ...rest }) => rest),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
        })

        await expect(
          accountService.list({
            auth: {},
            orderBy: [{ column: 'password_hash', direction: 'asc' }],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        secretDb.close()
      })

      it('builds no cursor over a column of a join the response leaves out', async () => {
        const noteDb = new BunDatabase(':memory:')
        noteDb.exec(
          `CREATE TABLE accounts (id TEXT PRIMARY KEY, team_id TEXT NOT NULL);
           CREATE TABLE teams (id TEXT PRIMARY KEY, region TEXT NOT NULL, note TEXT);
           INSERT INTO teams VALUES ('t-1', 'eu', 'note-1'), ('t-2', 'us', 'note-2');
           INSERT INTO accounts VALUES ('a-1', 't-1'), ('a-2', 't-2');`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(noteDb),
          rowSchema: z.object({ id: z.string(), team_id: z.string() }),
          createSchema: z.object({ id: z.string() }),
          updateSchema: z.object({ team_id: z.string().optional() }),
          joins: {
            team: {
              kind: 'one' as const,
              localColumn: 'team_id',
              remote: {
                table: 'teams',
                pk: 'id',
                select: ['id', 'region', 'note'],
              },
              schema: z.object({ id: z.string() }),
            },
          },
          // The join is resolved for this alone; `note` is never returned.
          expose: [{ from: '$team.region', as: 'region' }],
        })
        // A hand-made cursor that orders by the join's `note`.
        const cursor = btoa(
          JSON.stringify({
            orderBy: [
              { column: '$team.note', direction: 'asc' },
              { column: 'id', direction: 'asc' },
            ],
            values: ['', ''],
            direction: 'next',
          }),
        )

        await expect(
          accountService.list({ auth: {}, limit: 1, cursor }),
        ).rejects.toThrow('not found in row for cursor creation')
        noteDb.close()
      })

      it('builds no cursor over a column the row schema sets to undefined', async () => {
        // The key stays on the parsed row, holding `undefined`, which the
        // JSON response drops.
        const secretDb = new BunDatabase(':memory:')
        secretDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL, password_hash TEXT NOT NULL);
           INSERT INTO accounts (name, password_hash) VALUES ('a', 'hash-a'), ('b', 'hash-b');`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(secretDb),
          rowSchema: z
            .object({
              id: z.number(),
              name: z.string(),
              password_hash: z.string(),
            })
            .transform((row) => ({ ...row, password_hash: undefined })),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
        })

        await expect(
          accountService.list({
            auth: {},
            orderBy: [{ column: 'password_hash', direction: 'asc' }],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        secretDb.close()
      })

      it('builds no cursor over a column the row schema drops whose name every object inherits', async () => {
        // `'constructor' in row` is true of any object, through its
        // prototype, though the JSON response has no such key.
        const carDb = new BunDatabase(':memory:')
        carDb.exec(
          `CREATE TABLE cars (id INTEGER PRIMARY KEY, name TEXT NOT NULL, "constructor" TEXT NOT NULL);
           INSERT INTO cars VALUES (1, 'a', 'maker-b'), (2, 'b', 'maker-a');`,
        )
        const carService = createService({
          table: 'cars',
          db: fromBunSqlite(carDb),
          rowSchema: z
            .object({
              id: z.number(),
              name: z.string(),
              constructor: z.string(),
            })
            .transform(({ constructor: _maker, ...rest }) => rest),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
        })

        await expect(
          carService.list({
            auth: {},
            orderBy: [{ column: 'constructor', direction: 'asc' }],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        carDb.close()
      })

      it.each(['asc', 'desc'] as const)(
        'pages %s past the NULLs of a field the row schema reads as undefined',
        async (direction) => {
          // The field leaves NULL out of the JSON response. The key holds
          // `undefined` over a stored NULL, which says no more than NULL.
          const omitService = createService({
            table: 'users',
            db,
            rowSchema: UserRowSchema.extend({
              updated_at: z
                .string()
                .nullable()
                .transform((value) => value ?? undefined),
            }),
            createSchema: UserCreateSchema,
            updateSchema: UserUpdateSchema,
          })
          const insert = bunDb.prepare(
            `INSERT INTO users (name, email, tenant_id, created_at, updated_at)
             VALUES (?, ?, 'tenant-omit', '2024-01-01T00:00:00Z', ?)`,
          )
          const updatedAt = [
            '2024-02-02',
            null,
            '2024-02-01',
            null,
            '2024-02-03',
          ]
          for (const [i, value] of updatedAt.entries()) {
            insert.run(`Omit ${i}`, `omit-${i}@test.com`, value)
          }
          const expected = (
            bunDb
              .prepare(
                `SELECT id FROM users WHERE tenant_id = 'tenant-omit'
                 ORDER BY updated_at ${direction}, id ${direction}`,
              )
              .all() as Array<{ id: number }>
          ).map((r) => r.id)

          const seen: number[] = []
          let cursor: string | undefined
          for (let page = 0; page < 10; page++) {
            const res = await omitService.list({
              auth: {},
              where: { tenant_id: 'tenant-omit' },
              orderBy: [
                { column: 'updated_at', direction },
                { column: 'id', direction },
              ],
              limit: 1,
              ...(cursor ? { cursor } : {}),
            })
            seen.push(...res.result.map((r: { id: number }) => r.id))
            if (!res.nextCursor) break
            cursor = res.nextCursor
          }

          expect(seen).toEqual(expected)
        },
      )

      it('builds no cursor from another column under the flat name of a joined column the query leaves out', async () => {
        // The join doesn't select `secret`, so the query returns
        // `$profile.secret` under no key. Its flat name, `profile_secret`, is
        // a column of the base table, one the row schema drops.
        const profileDb = new BunDatabase(':memory:')
        profileDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL, profile_secret TEXT NOT NULL);
           CREATE TABLE profiles (account_id INTEGER PRIMARY KEY, bio TEXT, secret TEXT);
           INSERT INTO accounts VALUES (1, 'a', 'hidden-1'), (2, 'b', 'hidden-2');
           INSERT INTO profiles VALUES (1, 'bio-1', 's-2'), (2, 'bio-2', 's-1');`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(profileDb),
          rowSchema: z
            .object({
              id: z.number(),
              name: z.string(),
              profile_secret: z.string(),
            })
            .transform(({ profile_secret: _secret, ...rest }) => rest),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
          joins: {
            profile: {
              kind: 'one' as const,
              localPk: 'id',
              remote: { table: 'profiles', pk: 'account_id', select: ['bio'] },
              schema: z.object({ bio: z.string() }),
            },
          },
        })

        await expect(
          accountService.list({
            auth: {},
            include: { profile: true },
            orderBy: [
              { column: '$profile.secret', direction: 'asc' },
              { column: 'id', direction: 'asc' },
            ],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        profileDb.close()
      })

      it('builds no cursor over a column the row schema drops under the name of an included join', async () => {
        // The response's `profile` is the join's data, not the base column
        // of that name, which the row schema drops.
        const profileDb = new BunDatabase(':memory:')
        profileDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL, profile TEXT NOT NULL);
           CREATE TABLE profiles (account_id INTEGER PRIMARY KEY, bio TEXT);
           INSERT INTO accounts VALUES (1, 'a', 'hidden-y'), (2, 'b', 'hidden-z');
           INSERT INTO profiles VALUES (1, 'bio1'), (2, 'bio2');`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(profileDb),
          rowSchema: z
            .object({ id: z.number(), name: z.string() })
            .transform((row) => row),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
          joins: {
            profile: {
              kind: 'one' as const,
              localPk: 'id',
              remote: { table: 'profiles', pk: 'account_id', select: ['bio'] },
              schema: z.object({ bio: z.string() }),
            },
          },
        })

        await expect(
          accountService.list({
            auth: {},
            include: { profile: true },
            orderBy: [{ column: 'profile', direction: 'asc' }],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        // Selected by name too: the join's data still sits under it.
        await expect(
          accountService.list({
            auth: {},
            include: { profile: true },
            select: ['id', 'profile', '$profile.bio'],
            orderBy: [{ column: 'profile', direction: 'asc' }],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        profileDb.close()
      })

      it('builds no cursor over a column the row schema declares under the name of an included join', async () => {
        // The row schema takes the join's data under `profile`, so the
        // response holds it there, and the base column of that name stays
        // out of the response.
        const profileDb = new BunDatabase(':memory:')
        profileDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT NOT NULL, profile TEXT NOT NULL);
           CREATE TABLE profiles (account_id INTEGER PRIMARY KEY, bio TEXT);
           INSERT INTO accounts VALUES (1, 'a', 'hidden-y'), (2, 'b', 'hidden-z');
           INSERT INTO profiles VALUES (1, 'bio1'), (2, 'bio2');`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(profileDb),
          rowSchema: z.object({
            id: z.number(),
            name: z.string(),
            profile: z.object({ bio: z.string() }).nullable(),
          }),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
          joins: {
            profile: {
              kind: 'one' as const,
              localPk: 'id',
              remote: { table: 'profiles', pk: 'account_id', select: ['bio'] },
              schema: z.object({ bio: z.string() }),
            },
          },
        })

        await expect(
          accountService.list({
            auth: {},
            include: { profile: true },
            orderBy: [{ column: 'profile', direction: 'asc' }],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        profileDb.close()
      })

      it('answers a base column that shares a nested join’s name as the row schema reads it', async () => {
        // The response took nested joins' names from the top-level row, so
        // with the nested join included, the base column's stored value
        // replaced the one the row schema gives (here, a masked one).
        const nestedDb = new BunDatabase(':memory:')
        nestedDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, ssn TEXT NOT NULL, grp_id INTEGER);
           CREATE TABLE groups (id INTEGER PRIMARY KEY, title TEXT, kid_id INTEGER);
           CREATE TABLE kids (id INTEGER PRIMARY KEY, label TEXT);
           INSERT INTO kids VALUES (1, 'kid-1');
           INSERT INTO groups VALUES (1, 'g1', 1);
           INSERT INTO accounts VALUES (1, '123-45-6789', 1);`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(nestedDb),
          rowSchema: z.object({
            id: z.number(),
            ssn: z.string().transform((value) => `XXX-XX-${value.slice(-4)}`),
            grp_id: z.number().nullable(),
          }),
          createSchema: z.object({ ssn: z.string() }),
          updateSchema: z.object({ ssn: z.string().optional() }),
          joins: {
            grp: {
              kind: 'one' as const,
              localColumn: 'grp_id',
              remote: {
                table: 'groups',
                pk: 'id',
                select: ['title', 'kid_id'],
              },
              schema: z.object({ title: z.string(), kid_id: z.number() }),
              joins: {
                ssn: {
                  kind: 'one' as const,
                  localColumn: 'kid_id',
                  remote: { table: 'kids', pk: 'id', select: ['label'] },
                  schema: z.object({ label: z.string() }),
                },
              },
            },
          },
        })
        const expected = {
          id: 1,
          ssn: 'XXX-XX-6789',
          grp_id: 1,
          grp: { title: 'g1', kid_id: 1, ssn: { label: 'kid-1' } },
        }

        const page = await accountService.list({
          auth: {},
          include: { grp: { ssn: true } },
        })
        expect(page.result).toEqual([expected])
        expect(
          await accountService.get(1, {
            auth: {},
            include: { grp: { ssn: true } },
          }),
        ).toEqual(expected)
        nestedDb.close()
      })

      it('leaves out a base column a shapeless row schema drops, though a nested join shares its name', async () => {
        // 2.x answered the dropped column anyway, at the top level with its
        // stored value, and so paged by it.
        const nestedDb = new BunDatabase(':memory:')
        nestedDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, kid TEXT NOT NULL, grp_id INTEGER);
           CREATE TABLE groups (id INTEGER PRIMARY KEY, title TEXT, kid_id INTEGER);
           CREATE TABLE kids (id INTEGER PRIMARY KEY, label TEXT);
           INSERT INTO kids VALUES (1, 'kid-1');
           INSERT INTO groups VALUES (1, 'g1', 1);
           INSERT INTO accounts VALUES (1, 'K1', 1), (2, 'K2', 1);`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(nestedDb),
          rowSchema: z
            .object({ id: z.number(), grp_id: z.number().nullable() })
            .transform((row) => row),
          createSchema: z.object({ kid: z.string() }),
          updateSchema: z.object({ kid: z.string().optional() }),
          joins: {
            grp: {
              kind: 'one' as const,
              localColumn: 'grp_id',
              remote: {
                table: 'groups',
                pk: 'id',
                select: ['title', 'kid_id'],
              },
              schema: z.object({ title: z.string(), kid_id: z.number() }),
              joins: {
                kid: {
                  kind: 'one' as const,
                  localColumn: 'kid_id',
                  remote: { table: 'kids', pk: 'id', select: ['label'] },
                  schema: z.object({ label: z.string() }),
                },
              },
            },
          },
        })
        const include = { grp: { kid: true } }
        const expected = {
          id: 1,
          grp_id: 1,
          grp: { title: 'g1', kid_id: 1, kid: { label: 'kid-1' } },
        }

        const page = await accountService.list({
          auth: {},
          include,
          orderBy: [{ column: 'id', direction: 'asc' }],
          limit: 1,
        })
        expect(page.result).toEqual([expected])
        expect(await accountService.get(1, { auth: {}, include })).toEqual(
          expected,
        )
        await expect(
          accountService.list({
            auth: {},
            include,
            orderBy: [{ column: 'kid', direction: 'asc' }],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        nestedDb.close()
      })

      it('pages by a base column that shares a nested join’s name', async () => {
        // A nested join's data sits inside its parent's, so the response's
        // `child` is the base column, and pages by it.
        const nestedDb = new BunDatabase(':memory:')
        nestedDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, child TEXT NOT NULL, grp_id INTEGER);
           CREATE TABLE groups (id INTEGER PRIMARY KEY, title TEXT, kid_id INTEGER);
           CREATE TABLE kids (id INTEGER PRIMARY KEY, label TEXT);
           INSERT INTO kids VALUES (1, 'kid-1');
           INSERT INTO groups VALUES (1, 'g1', 1);
           INSERT INTO accounts VALUES (1, 'c-b', 1), (2, 'c-a', 1), (3, 'c-c', 1);`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(nestedDb),
          rowSchema: z.object({
            id: z.number(),
            child: z.string(),
            grp_id: z.number().nullable(),
          }),
          createSchema: z.object({ child: z.string() }),
          updateSchema: z.object({ child: z.string().optional() }),
          joins: {
            grp: {
              kind: 'one' as const,
              localColumn: 'grp_id',
              remote: {
                table: 'groups',
                pk: 'id',
                select: ['title', 'kid_id'],
              },
              schema: z.object({ title: z.string(), kid_id: z.number() }),
              joins: {
                child: {
                  kind: 'one' as const,
                  localColumn: 'kid_id',
                  remote: { table: 'kids', pk: 'id', select: ['label'] },
                  schema: z.object({ label: z.string() }),
                },
              },
            },
          },
        })

        for (const select of [undefined, ['id', 'child']]) {
          const ids: number[] = []
          let cursor: string | undefined
          do {
            const page = await accountService.list({
              auth: {},
              include: { grp: { child: true } },
              orderBy: [{ column: 'child', direction: 'asc' }],
              ...(select ? { select } : {}),
              limit: 1,
              cursor,
            })
            ids.push(...page.result.map((row) => Number(row.id)))
            cursor = page.nextCursor
          } while (cursor)
          expect(ids).toEqual([2, 1, 3])
        }
        nestedDb.close()
      })

      it('pages on a runtime without Object.hasOwn', async () => {
        const pageDb = new BunDatabase(':memory:')
        pageDb.exec(
          `CREATE TABLE notes (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
           INSERT INTO notes VALUES (1, 'a'), (2, 'b');`,
        )
        const noteService = createService({
          table: 'notes',
          db: fromBunSqlite(pageDb),
          rowSchema: z.object({ id: z.number(), title: z.string() }),
          createSchema: z.object({ title: z.string() }),
          updateSchema: z.object({ title: z.string().optional() }),
        })
        const { hasOwn } = Object
        delete (Object as { hasOwn?: unknown }).hasOwn
        try {
          const page = await noteService.list({
            auth: {},
            orderBy: [{ column: 'title', direction: 'asc' }],
            limit: 1,
          })
          const next = await noteService.list({
            auth: {},
            cursor: page.nextCursor,
            limit: 1,
          })
          expect(next.result.map((row) => row.id)).toEqual([2])
        } finally {
          Object.hasOwn = hasOwn
          pageDb.close()
        }
      })

      it('builds no cursor over a column of a many join, which list returns empty', async () => {
        const teamService = createService({
          table: 'users',
          db,
          rowSchema: UserRowSchema,
          createSchema: UserCreateSchema,
          updateSchema: UserUpdateSchema,
          joins: userJoins,
        })

        // The row comes back with `teams: []`, so a team's name in a cursor
        // would be a value the response leaves out.
        await expect(
          teamService.list({
            auth: {},
            include: { teams: true },
            orderBy: [
              { column: '$teams.name', direction: 'desc' },
              { column: 'id', direction: 'desc' },
            ],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
      })

      it('builds no cursor over a column of a one join under a many join', async () => {
        const leadDb = new BunDatabase(':memory:')
        leadDb.exec(
          `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
           CREATE TABLE memberships (user_id INTEGER NOT NULL, lead_id INTEGER);
           CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT);
           INSERT INTO users VALUES (1, 'a'), (2, 'b');
           INSERT INTO people VALUES (1, 'Lead-B'), (2, 'Lead-A');
           INSERT INTO memberships VALUES (1, 1), (2, 2);`,
        )
        const userService = createService({
          table: 'users',
          db: fromBunSqlite(leadDb),
          rowSchema: z.object({ id: z.number(), name: z.string() }),
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
          joins: {
            memberships: {
              kind: 'many' as const,
              localPk: 'id',
              remote: {
                table: 'memberships',
                pk: 'user_id',
                select: ['lead_id'],
              },
              schema: z.object({ lead_id: z.number() }),
              joins: {
                lead: {
                  kind: 'one' as const,
                  localColumn: 'lead_id',
                  remote: { table: 'people', pk: 'id', select: ['name'] },
                  schema: z.object({ name: z.string() }),
                },
              },
            },
          },
        })

        // The row comes back with `memberships: []`, the lead's name nowhere.
        await expect(
          userService.list({
            auth: {},
            include: { memberships: { lead: true } },
            orderBy: [
              { column: '$memberships.lead.name', direction: 'asc' },
              { column: 'id', direction: 'asc' },
            ],
            limit: 1,
          }),
        ).rejects.toThrow('not found in row for cursor creation')
        leadDb.close()
      })

      it('should handle cursor conditions correctly', () => {
        const token = {
          orderBy: [{ column: 'id', direction: 'asc' as const }],
          values: [2],
          direction: 'next' as const,
        }

        const conditions = buildCursorConditions(token, 'users')
        expect(conditions.text).toBe('"users"."id" > ?')
        expect(conditions.values).toEqual([2])
      })
    })
  })

  describe('Encryption System', () => {
    let keyProvider: DefaultKeyProvider

    beforeEach(() => {
      keyProvider = new DefaultKeyProvider({
        default: testSecretKey,
        'user-secrets': testSecretKey,
        'custom-key': testSecretKey,
      })
    })

    describe('Basic encryption/decryption', () => {
      it('should encrypt and decrypt strings correctly', async () => {
        const plaintext = 'sk_ex_abcdef1234567890abcdef1234567890'

        const encrypted = await encrypt(plaintext, keyProvider)
        expect(encrypted).toMatch(
          /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
        )

        const decrypted = await decrypt(encrypted, keyProvider)
        expect(decrypted).toBe(plaintext)
      })

      it('should generate different ciphertext for same plaintext', async () => {
        const plaintext = 'test-secret'

        const encrypted1 = await encrypt(plaintext, keyProvider)
        const encrypted2 = await encrypt(plaintext, keyProvider)

        expect(encrypted1).not.toBe(encrypted2)

        const decrypted1 = await decrypt(encrypted1, keyProvider)
        const decrypted2 = await decrypt(encrypted2, keyProvider)

        expect(decrypted1).toBe(plaintext)
        expect(decrypted2).toBe(plaintext)
      })

      it('should handle different key IDs', async () => {
        const plaintext = 'test-secret'

        const encrypted = await encrypt(plaintext, keyProvider, 'custom-key')
        const decrypted = await decrypt(encrypted, keyProvider)

        expect(decrypted).toBe(plaintext)
      })

      it('should throw EncryptionError for invalid ciphertext', async () => {
        await expect(decrypt('invalid-cipher', keyProvider)).rejects.toThrow(
          EncryptionError,
        )

        await expect(
          decrypt('invalid:format:here', keyProvider),
        ).rejects.toThrow(EncryptionError)
      })
    })

    describe('Secret field encryption in data objects', () => {
      it('should encrypt secrets in data objects', async () => {
        const data = {
          name: 'John',
          apiKey: 'sk_ex_secret123',
          email: 'john@test.com',
        }

        const encrypted = await encryptSecrets(data, userSecrets, keyProvider)

        expect(encrypted.name).toBe('John')
        expect(encrypted.email).toBe('john@test.com')
        expect(encrypted.apiKey).toBeUndefined()
        expect(typeof encrypted.api_key_encrypted).toBe('string')
        expect(encrypted.api_key_encrypted).toMatch(
          /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
        )

        const decrypted = await decrypt(
          encrypted.api_key_encrypted as string,
          keyProvider,
        )
        expect(decrypted).toBe('sk_ex_secret123')
      })

      it('should decrypt secrets in data objects', async () => {
        const encrypted = await encrypt('sk_ex_secret123', keyProvider)
        const data = {
          name: 'John',
          api_key_encrypted: encrypted,
          email: 'john@test.com',
        }

        const decrypted = await decryptSecrets(data, userSecrets, keyProvider)

        expect(decrypted).toEqual({
          name: 'John',
          apiKey: 'sk_ex_secret123',
          email: 'john@test.com',
        })
      })

      it('should handle missing secret fields gracefully', async () => {
        const data = { name: 'John', email: 'john@test.com' }

        const encrypted = await encryptSecrets(data, userSecrets, keyProvider)
        expect(encrypted).toEqual({ name: 'John', email: 'john@test.com' })

        const decrypted = await decryptSecrets(data, userSecrets, keyProvider)
        expect(decrypted).toEqual({ name: 'John', email: 'john@test.com' })
      })

      it('should throw EncryptionError for non-string secret values', async () => {
        const data = { apiKey: 123 as any }

        await expect(
          encryptSecrets(data, userSecrets, keyProvider),
        ).rejects.toThrow(EncryptionError)
      })
    })

    describe('Key generation and management', () => {
      it('should generate valid secret keys', async () => {
        const key = await generateSecretKey()
        expect(typeof key).toBe('string')
        expect(key).toMatch(/^[A-Za-z0-9+/]+=*$/) // Valid base64
        expect(atob(key)).toHaveLength(32) // 256-bit key when decoded
      })

      it('should cache keys in DefaultKeyProvider', async () => {
        const provider = new DefaultKeyProvider({ 'test-key': testSecretKey })

        const key1 = await provider.getKey('test-key')
        const key2 = await provider.getKey('test-key')

        expect(key1).toBe(key2) // Should be same instance (cached)
        expect(key1).toBeInstanceOf(CryptoKey)
        expect(key1.type).toBe('secret')
      })

      it('should throw EncryptionError when encryption key not provided', async () => {
        const provider = new DefaultKeyProvider()

        await expect(provider.getKey('missing-key')).rejects.toThrow(
          EncryptionError,
        )
      })

      it('should NOT fall back to process.env (security: no implicit ambient keys)', async () => {
        // Set environment variable for this test
        process.env.SECRET_KEY = testSecretKey

        // The provider no longer silently adopts process.env.SECRET_KEY; with
        // no keys passed, the default key is simply not found.
        const provider = new DefaultKeyProvider()
        await expect(provider.getKey('default')).rejects.toThrow(
          'Encryption key not found for keyId: default',
        )

        // Clean up
        delete process.env.SECRET_KEY
      })
    })

    describe('Integration with service operations', () => {
      let serviceWithSecrets: Service<any, any, any, any, any>

      beforeEach(() => {
        serviceWithSecrets = createService({
          table: 'users',
          db,
          rowSchema: UserRowSchema,
          createSchema: UserCreateSchema,
          updateSchema: UserUpdateSchema,
          secrets: userSecrets,
          timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
          encryptionKeys: {
            default: testSecretKey,
            'user-secrets': testSecretKey,
          },
        })
      })

      it('should encrypt secrets on create', async () => {
        const user = await serviceWithSecrets.create(
          {
            name: 'Secret User',
            email: 'secret@test.com',
            apiKey: 'sk_ex_secret123',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        )

        expect(user).toEqual({
          id: 4,
          name: 'Secret User',
          email: 'secret@test.com',
          tenant_id: 'tenant-1',
          created_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
          updated_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
        })

        // API key should be encrypted in the database column
        const rawResult = bunDb
          .prepare('SELECT * FROM users WHERE id = ?')
          .get(user.id) as any
        expect(rawResult.api_key_encrypted).toBeDefined()
        expect(rawResult.api_key_encrypted).not.toBe('sk_ex_secret123')
        expect(rawResult.api_key_encrypted).toMatch(
          /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
        )
      })

      it('should decrypt secrets when includeSecrets=true', async () => {
        const created = await serviceWithSecrets.create(
          {
            name: 'Secret User',
            email: 'secret@test.com',
            apiKey: 'sk_ex_secret123',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        )

        const withSecrets = await serviceWithSecrets.get(created.id, {
          auth: {},
          includeSecrets: true,
        })

        expect(withSecrets).toEqual({
          id: created.id,
          name: 'Secret User',
          email: 'secret@test.com',
          apiKey: 'sk_ex_secret123',
          tenant_id: 'tenant-1',
          created_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
          updated_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
        })
      })

      it('should not include secrets when includeSecrets=false', async () => {
        const created = await serviceWithSecrets.create(
          {
            name: 'Secret User',
            email: 'secret2@test.com',
            apiKey: 'sk_ex_secret456',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        )

        const withoutSecrets = await serviceWithSecrets.get(created.id, {
          auth: {},
        })

        expect(withoutSecrets).toEqual({
          id: created.id,
          name: 'Secret User',
          email: 'secret2@test.com',
          tenant_id: 'tenant-1',
          created_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
          updated_at: expect.stringMatching(
            /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
          ),
        })
        expect(withoutSecrets?.apiKey).toBeUndefined()
      })
    })
  })

  describe('Joins System', () => {
    let serviceWithJoins: Service<any, any, any, any, any>

    beforeEach(() => {
      serviceWithJoins = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        joins: userJoins,
        encryptionKeys: {
          default: testSecretKey,
        },
      })
    })

    it('should include one-to-one joins when requested', async () => {
      const user = await serviceWithJoins.get(1, {
        auth: {},
        include: { profile: true },
      })

      expect(user).toEqual({
        id: 1,
        name: 'John Doe',
        email: 'john@test.com',
        tenant_id: 'tenant-1',
        created_at: '2024-01-01T00:00:00Z',
        updated_at: null,
        profile: {
          bio: 'Software engineer',
          avatar: 'avatar1.jpg',
        },
      })
    })

    it('should include one-to-many joins when requested', async () => {
      const user = await serviceWithJoins.get(2, {
        auth: {},
        include: { teams: true },
      })

      expect(user).toEqual({
        id: 2,
        name: 'Jane Smith',
        email: 'jane@test.com',
        tenant_id: 'tenant-1',
        created_at: '2024-01-02T00:00:00Z',
        updated_at: null,
        teams: [
          {
            id: 1,
            name: 'Team Alpha',
            description: 'First team',
          },
          {
            id: 2,
            name: 'Team Beta',
            description: 'Second team',
          },
        ],
      })
    })

    it('should not include joins when not requested', async () => {
      const user = await serviceWithJoins.get(1, { auth: {} })

      expect(user).toEqual({
        id: 1,
        name: 'John Doe',
        email: 'john@test.com',
        tenant_id: 'tenant-1',
        created_at: '2024-01-01T00:00:00Z',
        updated_at: null,
      })
      expect(user?.profile).toBeUndefined()
      expect(user?.teams).toBeUndefined()
    })

    it('should handle multiple joins simultaneously', async () => {
      const user = await serviceWithJoins.get(1, {
        auth: {},
        include: { profile: true, teams: true },
      })

      expect(user).toEqual({
        id: 1,
        name: 'John Doe',
        email: 'john@test.com',
        tenant_id: 'tenant-1',
        created_at: '2024-01-01T00:00:00Z',
        updated_at: null,
        profile: {
          bio: 'Software engineer',
          avatar: 'avatar1.jpg',
        },
        teams: [
          {
            id: 1,
            name: 'Team Alpha',
            description: 'First team',
          },
        ],
      })
    })

    it('should return null for one-to-one joins with no data', async () => {
      const user = await serviceWithJoins.get(3, {
        auth: {},
        include: { profile: true },
      })

      expect(user).toEqual({
        id: 3,
        name: 'Bob Wilson',
        email: 'bob@test.com',
        tenant_id: 'tenant-2',
        created_at: '2024-01-03T00:00:00Z',
        updated_at: null,
        profile: null,
      })
    })

    it('should return empty array for one-to-many joins with no data', async () => {
      const user = await serviceWithJoins.get(3, {
        auth: {},
        include: { teams: true },
      })

      expect(user).toEqual({
        id: 3,
        name: 'Bob Wilson',
        email: 'bob@test.com',
        tenant_id: 'tenant-2',
        created_at: '2024-01-03T00:00:00Z',
        updated_at: null,
        teams: [],
      })
    })
  })

  describe('Field selection (select)', () => {
    let serviceWithJoins: Service<any, any, any, any, any>
    let serviceWithSecrets: Service<any, any, any, any, any>

    beforeEach(() => {
      serviceWithJoins = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        joins: userJoins,
        timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
        encryptionKeys: { default: testSecretKey },
      })

      serviceWithSecrets = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        secrets: userSecrets,
        timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
        encryptionKeys: {
          default: testSecretKey,
          'user-secrets': testSecretKey,
        },
      })
    })

    describe('main row select', () => {
      it('returns only requested columns from get()', async () => {
        const user = await serviceWithJoins.get(1, {
          auth: {},
          select: ['id', 'name'],
        })
        expect(user).toEqual({ id: 1, name: 'John Doe' })
      })

      it('returns only requested columns from list()', async () => {
        const page = await serviceWithJoins.list({
          auth: {},
          select: ['id', 'email'],
          orderBy: [{ column: 'id', direction: 'asc' }],
        })
        expect(page.result).toEqual([
          { id: 1, email: 'john@test.com' },
          { id: 2, email: 'jane@test.com' },
          { id: 3, email: 'bob@test.com' },
        ])
      })

      it('always silently includes the primary key even when omitted from select', async () => {
        const user = await serviceWithJoins.get(1, {
          auth: {},
          select: ['name'],
        })
        expect(user).toEqual({ id: 1, name: 'John Doe' })
      })

      it('always silently includes orderBy columns so cursor pagination keeps working', async () => {
        const page1 = await serviceWithJoins.list({
          auth: {},
          select: ['name'],
          orderBy: [{ column: 'created_at', direction: 'asc' }],
          limit: 1,
        })
        expect(page1.result).toEqual([
          {
            id: 1,
            name: 'John Doe',
            created_at: '2024-01-01T00:00:00Z',
          },
        ])
        expect(page1.nextCursor).toBeDefined()

        const page2 = await serviceWithJoins.list({
          auth: {},
          select: ['name'],
          cursor: page1.nextCursor,
          limit: 1,
        })
        expect(page2.result).toEqual([
          {
            id: 2,
            name: 'Jane Smith',
            created_at: '2024-01-02T00:00:00Z',
          },
        ])
      })

      it('throws ValidationError for unknown columns', async () => {
        await expect(
          serviceWithJoins.get(1, {
            auth: {},
            select: ['id', 'nope'],
          }),
        ).rejects.toThrow(ValidationError)
      })

      it('returns full row when select is omitted (backwards compatible)', async () => {
        const user = await serviceWithJoins.get(1, { auth: {} })
        expect(user).toMatchObject({
          id: 1,
          name: 'John Doe',
          email: 'john@test.com',
          tenant_id: 'tenant-1',
        })
      })

      it('treats empty select array as undefined (full row)', async () => {
        const user = await serviceWithJoins.get(1, {
          auth: {},
          select: [],
        })
        expect(user).toMatchObject({
          id: 1,
          name: 'John Doe',
          email: 'john@test.com',
        })
      })

      it('deduplicates repeated select tokens', async () => {
        const user = await serviceWithJoins.get(1, {
          auth: {},
          select: ['id', 'name', 'name', 'id'],
        })
        expect(user).toEqual({ id: 1, name: 'John Doe' })
      })
    })

    describe('$alias join column selection', () => {
      it('selects specific columns from a one-to-one join', async () => {
        const user = await serviceWithJoins.get(1, {
          auth: {},
          select: ['id', 'name', '$profile.bio'],
          include: { profile: true },
        })
        expect(user).toEqual({
          id: 1,
          name: 'John Doe',
          profile: {
            bio: 'Software engineer',
            user_id: 1, // PK silently included
          },
        })
      })

      it('selects specific columns from a one-to-many join (through table)', async () => {
        const user = await serviceWithJoins.get(2, {
          auth: {},
          select: ['id', '$teams.name'],
          include: { teams: true },
        })
        expect(user).toEqual({
          id: 2,
          teams: [
            { id: 1, name: 'Team Alpha' },
            { id: 2, name: 'Team Beta' },
          ],
        })
      })

      it('expands a bare $alias to all configured join columns', async () => {
        const user = await serviceWithJoins.get(1, {
          auth: {},
          select: ['id', '$teams'],
          include: { teams: true },
        })
        expect(user).toEqual({
          id: 1,
          teams: [{ id: 1, name: 'Team Alpha', description: 'First team' }],
        })
      })

      it('throws when $alias.column is used without include[alias] = true', async () => {
        await expect(
          serviceWithJoins.get(1, {
            auth: {},
            select: ['id', '$teams.name'],
          }),
        ).rejects.toThrow(/include\.teams = true/)
      })

      it('throws ValidationError for unknown join alias', async () => {
        await expect(
          serviceWithJoins.get(1, {
            auth: {},
            select: ['id', '$nope.foo'],
            include: { teams: true },
          }),
        ).rejects.toThrow(ValidationError)
      })

      it('works with list() + per-call join column selection', async () => {
        const page = await serviceWithJoins.list({
          auth: {},
          select: ['id', '$profile.avatar'],
          include: { profile: true },
          orderBy: [{ column: 'id', direction: 'asc' }],
          limit: 2,
        })
        expect(page.result).toEqual([
          {
            id: 1,
            profile: { user_id: 1, avatar: 'avatar1.jpg' },
          },
          {
            id: 2,
            profile: { user_id: 2, avatar: 'avatar2.jpg' },
          },
        ])
      })

      it("pages list() by a join column that select adds to the join's configured columns", async () => {
        // `user_id` isn't in the join's `remote.select`. The query returns it
        // for this call, so the response carries it, and so does a cursor.
        const ids: number[] = []
        let cursor: string | undefined
        for (let page = 0; page < 5; page++) {
          const res = await serviceWithJoins.list({
            auth: {},
            select: ['id', '$profile.user_id'],
            include: { profile: true },
            orderBy: [
              { column: '$profile.user_id', direction: 'desc' },
              { column: 'id', direction: 'desc' },
            ],
            limit: 1,
            ...(cursor ? { cursor } : {}),
          })
          ids.push(...res.result.map((r: { id: number }) => r.id))
          if (!res.nextCursor) break
          cursor = res.nextCursor
        }

        // User 3 has no profile, and NULL sorts last on DESC.
        expect(ids).toEqual([2, 1, 3])
      })

      it('pages list() by a join column whose flat alias select changes', async () => {
        // `$team_lead.name` has the flat alias `team_lead_name` unless an
        // earlier join column takes it. With its configured columns, the
        // `team` join's `lead_name` does, and `$team_lead.name` becomes
        // `team_lead__name`; `select` narrows `team` to `id`, which leaves
        // the name free.
        const leadDb = new BunDatabase(':memory:')
        leadDb.exec(
          `CREATE TABLE accounts (id INTEGER PRIMARY KEY, team_id INTEGER, lead_id INTEGER);
           CREATE TABLE teams (id INTEGER PRIMARY KEY, lead_name TEXT);
           CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT);
           INSERT INTO teams VALUES (1, 'x'), (2, 'y');
           INSERT INTO people VALUES (1, 'Carol'), (2, 'Alice'), (3, 'Bob');
           INSERT INTO accounts VALUES (1, 1, 1), (2, 2, 2), (3, 1, 3);`,
        )
        const accountService = createService({
          table: 'accounts',
          db: fromBunSqlite(leadDb),
          rowSchema: z.object({
            id: z.number(),
            team_id: z.number(),
            lead_id: z.number(),
          }),
          createSchema: z.object({ team_id: z.number() }),
          updateSchema: z.object({ team_id: z.number().optional() }),
          joins: {
            team: {
              kind: 'one' as const,
              localColumn: 'team_id',
              remote: { table: 'teams', pk: 'id', select: ['id', 'lead_name'] },
              schema: z.object({ id: z.number(), lead_name: z.string() }),
            },
            team_lead: {
              kind: 'one' as const,
              localColumn: 'lead_id',
              remote: { table: 'people', pk: 'id', select: ['name'] },
              schema: z.object({ name: z.string() }),
            },
          },
        })

        const ids: number[] = []
        let cursor: string | undefined
        for (let page = 0; page < 5; page++) {
          const res = await accountService.list({
            auth: {},
            select: ['id', '$team.id'],
            include: { team: true, team_lead: true },
            orderBy: [
              { column: '$team_lead.name', direction: 'asc' },
              { column: 'id', direction: 'asc' },
            ],
            limit: 1,
            ...(cursor ? { cursor } : {}),
          })
          ids.push(...res.result.map((r: { id: number }) => r.id))
          if (!res.nextCursor) break
          cursor = res.nextCursor
        }
        leadDb.close()

        expect(ids).toEqual([2, 3, 1])
      })

      it('does not strip main columns whose names share the join alias prefix', async () => {
        // Regression: a previous projectRow implementation removed any key in
        // the row whose name started with `${alias}_` to drop the SQL-aliased
        // join columns. That accidentally also matched legitimate main
        // columns. With the collision-free implementation, `team_size` must
        // survive even though the join alias is `team` (so the SQL aliased
        // join keys look like `team_id`, `team_name`, … (or `team__id` only on collision).
        bunDb.exec(`ALTER TABLE users ADD COLUMN team_size INTEGER DEFAULT 0`)
        bunDb.exec(`UPDATE users SET team_size = 7 WHERE id = 1`)

        const SchemaWithCollidingColumn = z.object({
          id: z.number(),
          name: z.string(),
          email: z.string(),
          tenant_id: z.string(),
          team_size: z.number(),
          created_at: z.string(),
          updated_at: z.string().nullable().optional(),
        })

        const svc = createService({
          table: 'users',
          db,
          rowSchema: SchemaWithCollidingColumn,
          createSchema: SchemaWithCollidingColumn.partial(),
          updateSchema: SchemaWithCollidingColumn.partial(),
          joins: userJoins, // join alias is `team`
          timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
        })

        const user = await svc.get(1, {
          auth: {},
          select: ['id', 'team_size', '$teams.name'],
          include: { teams: true },
        })

        expect(user).toEqual({
          id: 1,
          team_size: 7,
          teams: [{ id: 1, name: 'Team Alpha' }],
        })
      })
    })

    describe('interaction with secrets', () => {
      beforeEach(async () => {
        await serviceWithSecrets.create(
          {
            name: 'Secret User',
            email: 'secret@test.com',
            tenant_id: 'tenant-1',
            apiKey: 'fake-test-secret-abcdefghijklmnopqrstuvwxyz',
          },
          { auth: {} },
        )
      })

      it('returns a select-restricted row WITHOUT secrets when includeSecrets is false', async () => {
        const user = await serviceWithSecrets.get(4, {
          auth: {},
          select: ['id', 'name'],
        })
        expect(user).toEqual({ id: 4, name: 'Secret User' })
        expect((user as any).apiKey).toBeUndefined()
      })

      it('returns a select-restricted row WITH decrypted secret when includeSecrets is true', async () => {
        const user = await serviceWithSecrets.get(4, {
          auth: {},
          select: ['id'],
          includeSecrets: true,
        })
        expect(user).toEqual({
          id: 4,
          apiKey: 'fake-test-secret-abcdefghijklmnopqrstuvwxyz',
        })
      })

      it('select does not allow listing secret column names directly', async () => {
        await expect(
          serviceWithSecrets.get(4, {
            auth: {},
            select: ['apiKey'],
          }),
        ).rejects.toThrow(ValidationError)
      })
    })

    describe('create() and update() honour select', () => {
      it('create() returns a projected record', async () => {
        const created = await serviceWithJoins.create(
          {
            name: 'Carol Jones',
            email: 'carol@test.com',
            tenant_id: 'tenant-1',
          },
          { auth: {}, select: ['id', 'name'] },
        )
        expect(created).toEqual({ id: 4, name: 'Carol Jones' })
      })

      it('update() returns a projected record', async () => {
        const updated = await serviceWithJoins.update(
          1,
          { name: 'John Renamed' },
          { auth: {}, select: ['id', 'name'] },
        )
        expect(updated).toEqual({ id: 1, name: 'John Renamed' })
      })
    })
  })

  describe('Hooks System', () => {
    it('should execute before hooks', async () => {
      const hookCalls: string[] = []

      const serviceWithHooks = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        encryptionKeys: { default: testSecretKey },
        hooks: {
          list: {
            before: async () => {
              hookCalls.push('before-list')
            },
          },
        },
      })

      const result = await serviceWithHooks.list({ auth: {} })
      expect(hookCalls).toEqual(['before-list'])
      expect(result.result).toHaveLength(3) // Verify the operation completed
    })

    it('should execute after hooks', async () => {
      const hookCalls: string[] = []

      const serviceWithHooks = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        encryptionKeys: { default: testSecretKey },
        hooks: {
          list: {
            after: async () => {
              hookCalls.push('after-list')
            },
          },
        },
      })

      const result = await serviceWithHooks.list({ auth: {} })
      expect(hookCalls).toEqual(['after-list'])
      expect(result.result).toHaveLength(3) // Verify the operation completed
    })

    it('should execute multiple hooks in order', async () => {
      const hookCalls: string[] = []

      const serviceWithHooks = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        encryptionKeys: { default: testSecretKey },
        hooks: {
          list: {
            before: [
              async () => {
                hookCalls.push('hook-1')
              },
              async () => {
                hookCalls.push('hook-2')
              },
            ],
          },
        },
      })

      const result = await serviceWithHooks.list({ auth: {} })
      expect(hookCalls).toEqual(['hook-1', 'hook-2'])
      expect(result.result).toHaveLength(3) // Verify the operation completed
    })

    it('should execute error hooks when operations fail', async () => {
      const hookCalls: string[] = []

      const serviceWithHooks = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        encryptionKeys: { default: testSecretKey },
        hooks: {
          create: {
            before: async () => {
              throw new Error('Simulated error')
            },
            error: async () => {
              hookCalls.push('error-hook')
            },
          },
        },
      })

      await expect(
        serviceWithHooks.create(
          {
            name: 'Test',
            email: 'test@test.com',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        ),
      ).rejects.toThrow('Simulated error')

      expect(hookCalls).toEqual(['error-hook'])
    })

    it('should provide correct context to hooks', async () => {
      let capturedContext: any

      const serviceWithHooks = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        encryptionKeys: { default: testSecretKey },
        hooks: {
          list: {
            before: async (ctx) => {
              capturedContext = ctx
            },
          },
        },
      })

      await serviceWithHooks.list({
        auth: { user: { id: 'test-user', roles: ['admin'] } },
      })

      expect(capturedContext.auth).toEqual({
        user: { id: 'test-user', roles: ['admin'] },
      })
      expect(capturedContext.method).toBe('list')
      expect(capturedContext.db).toBe(db)
      expect(capturedContext.deps).toBeDefined()
      expect(typeof capturedContext.deps).toBe('object')
    })

    it('should allow hooks to modify context', async () => {
      const serviceWithHooks = createService({
        table: 'users',
        db,
        rowSchema: UserRowSchema,
        createSchema: UserCreateSchema,
        updateSchema: UserUpdateSchema,
        encryptionKeys: { default: testSecretKey },
        hooks: {
          list: {
            before: async (ctx) => {
              // Add tenant filtering
              ;(ctx as any).params.where = {
                ...(ctx as any).params.where,
                tenant_id: 'tenant-1',
              }
            },
          },
        },
      })

      const result = await serviceWithHooks.list({ auth: {} })

      // Should only return users from tenant-1
      expect(result.result).toEqual([
        {
          id: 1,
          name: 'John Doe',
          email: 'john@test.com',
          tenant_id: 'tenant-1',
          created_at: '2024-01-01T00:00:00Z',
          updated_at: null,
        },
        {
          id: 2,
          name: 'Jane Smith',
          email: 'jane@test.com',
          tenant_id: 'tenant-1',
          created_at: '2024-01-02T00:00:00Z',
          updated_at: null,
        },
      ])
    })
  })

  describe('create() with non-integer primary keys', () => {
    it('should create a record with a TEXT primary key', async () => {
      bunDb.exec(`
        CREATE TABLE documents (
          doc_id TEXT PRIMARY KEY,
          title TEXT NOT NULL,
          body TEXT
        );
      `)

      const docService = createService({
        table: 'documents',
        db,
        primaryKey: 'doc_id',
        rowSchema: z.object({
          doc_id: z.string(),
          title: z.string(),
          body: z.string().nullable(),
        }),
        createSchema: z.object({
          doc_id: z.string(),
          title: z.string(),
          body: z.string().optional(),
        }),
        updateSchema: z.object({
          title: z.string().optional(),
          body: z.string().optional(),
        }),
      })

      const doc = await docService.create(
        { doc_id: 'doc-abc-123', title: 'Hello', body: 'World' },
        { auth: {} },
      )

      expect(doc).toEqual({
        doc_id: 'doc-abc-123',
        title: 'Hello',
        body: 'World',
      })

      const fetched = await docService.get('doc-abc-123', { auth: {} })
      expect(fetched).toEqual(doc)
    })

    it('should create a record with a UUID primary key', async () => {
      bunDb.exec(`
        CREATE TABLE tokens (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          value TEXT NOT NULL
        );
      `)

      const tokenService = createService({
        table: 'tokens',
        db,
        primaryKey: 'id',
        rowSchema: z.object({
          id: z.string(),
          name: z.string(),
          value: z.string(),
        }),
        createSchema: z.object({
          id: z.string(),
          name: z.string(),
          value: z.string(),
        }),
        updateSchema: z.object({
          name: z.string().optional(),
          value: z.string().optional(),
        }),
      })

      const uuid = 'f47ac10b-58cc-4372-a567-0e02b2c3d479'
      const token = await tokenService.create(
        { id: uuid, name: 'api-key', value: 'secret' },
        { auth: {} },
      )

      expect(token).toEqual({
        id: uuid,
        name: 'api-key',
        value: 'secret',
      })

      const fetched = await tokenService.get(uuid, { auth: {} })
      expect(fetched).toEqual(token)
    })

    it('should create a record with a composite primary key', async () => {
      bunDb.exec(`
        CREATE TABLE settings (
          tenant_id TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT,
          PRIMARY KEY (tenant_id, key)
        );
      `)

      const settingsService = createService({
        table: 'settings',
        db,
        primaryKey: ['tenant_id', 'key'],
        rowSchema: z.object({
          tenant_id: z.string(),
          key: z.string(),
          value: z.string().nullable(),
        }),
        createSchema: z.object({
          tenant_id: z.string(),
          key: z.string(),
          value: z.string().optional(),
        }),
        updateSchema: z.object({
          value: z.string().optional(),
        }),
      })

      const setting = await settingsService.create(
        { tenant_id: 'org-1', key: 'theme', value: 'dark' },
        { auth: {} },
      )

      expect(setting).toEqual({
        tenant_id: 'org-1',
        key: 'theme',
        value: 'dark',
      })

      const fetched = await settingsService.get(
        { tenant_id: 'org-1', key: 'theme' } as any,
        { auth: {} },
      )
      expect(fetched).toEqual(setting)
    })

    it('should create a record with an auto-generated DEFAULT primary key', async () => {
      bunDb.exec(`
        CREATE TABLE events (
          id TEXT PRIMARY KEY DEFAULT ('evt_' || lower(hex(randomblob(8)))),
          type TEXT NOT NULL,
          payload TEXT
        );
      `)

      const eventService = createService({
        table: 'events',
        db,
        primaryKey: 'id',
        rowSchema: z.object({
          id: z.string(),
          type: z.string(),
          payload: z.string().nullable(),
        }),
        createSchema: z.object({
          type: z.string(),
          payload: z.string().optional(),
        }),
        updateSchema: z.object({
          payload: z.string().optional(),
        }),
      })

      const event = await eventService.create(
        { type: 'user.created', payload: '{"userId":"u1"}' },
        { auth: {} },
      )

      expect(event.type).toBe('user.created')
      expect(event.payload).toBe('{"userId":"u1"}')
      expect(event.id).toMatch(/^evt_[0-9a-f]{16}$/)

      const fetched = await eventService.get(event.id, { auth: {} })
      expect(fetched).toEqual(event)
    })

    it('should create multiple records with TEXT primary keys', async () => {
      bunDb.exec(`
        CREATE TABLE slugs (
          slug TEXT PRIMARY KEY,
          url TEXT NOT NULL
        );
      `)

      const slugService = createService({
        table: 'slugs',
        db,
        primaryKey: 'slug',
        rowSchema: z.object({
          slug: z.string(),
          url: z.string(),
        }),
        createSchema: z.object({
          slug: z.string(),
          url: z.string(),
        }),
        updateSchema: z.object({
          url: z.string().optional(),
        }),
      })

      const s1 = await slugService.create(
        { slug: 'hello-world', url: '/posts/1' },
        { auth: {} },
      )
      const s2 = await slugService.create(
        { slug: 'about-us', url: '/pages/about' },
        { auth: {} },
      )

      expect(s1.slug).toBe('hello-world')
      expect(s2.slug).toBe('about-us')

      const list = await slugService.list({ auth: {} })
      expect(list.result).toHaveLength(2)
    })
  })

  describe('Secrets + rowSchema validation', () => {
    it('should reject rowSchema containing secret columnName', () => {
      const BadRowSchema = z.object({
        id: z.number(),
        name: z.string(),
        api_key_encrypted: z.string().nullable(),
      })

      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: BadRowSchema,
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
          secrets: [
            {
              logicalName: 'apiKey',
              columnName: 'api_key_encrypted',
              keyId: 'default',
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).toThrow(ValidationError)
    })

    it('should reject rowSchema containing secret logicalName', () => {
      const BadRowSchema = z.object({
        id: z.number(),
        name: z.string(),
        apiKey: z.string().nullable(),
      })

      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: BadRowSchema,
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
          secrets: [
            {
              logicalName: 'apiKey',
              columnName: 'api_key_encrypted',
              keyId: 'default',
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).toThrow(ValidationError)
    })

    it('should provide a clear error message for columnName in rowSchema', () => {
      const BadRowSchema = z.object({
        id: z.number(),
        totp_secret_encrypted: z.string().nullable(),
      })

      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: BadRowSchema,
          createSchema: z.object({}),
          updateSchema: z.object({}),
          secrets: [
            {
              logicalName: 'totpSecret',
              columnName: 'totp_secret_encrypted',
              keyId: 'default',
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).toThrow(/totp_secret_encrypted.*columnName.*should not be in rowSchema/)
    })

    it('should provide a clear error message for logicalName in rowSchema', () => {
      const BadRowSchema = z.object({
        id: z.number(),
        totpSecret: z.string().nullable(),
      })

      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: BadRowSchema,
          createSchema: z.object({}),
          updateSchema: z.object({}),
          secrets: [
            {
              logicalName: 'totpSecret',
              columnName: 'totp_secret_encrypted',
              keyId: 'default',
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).toThrow(/totpSecret.*logicalName.*should not be in rowSchema/)
    })

    it('should allow services with secrets when rowSchema is correct', () => {
      const GoodRowSchema = z.object({
        id: z.number(),
        name: z.string(),
      })

      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: GoodRowSchema,
          createSchema: z.object({ name: z.string() }),
          updateSchema: z.object({ name: z.string().optional() }),
          secrets: [
            {
              logicalName: 'apiKey',
              columnName: 'api_key_encrypted',
              keyId: 'default',
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).not.toThrow()
    })
  })

  describe('Secret serialize/deserialize codec', () => {
    const CodecRowSchema = z.object({
      id: z.number(),
      name: z.string(),
      email: z.string(),
      tenant_id: z.string(),
      created_at: z.string(),
      updated_at: z.string().nullable().optional(),
    })
    const CodecCreateSchema = z.object({
      name: z.string(),
      email: z.string().email(),
      tenant_id: z.string(),
      config: z.any().optional(),
    })
    const CodecUpdateSchema = z.object({
      name: z.string().optional(),
      config: z.any().optional(),
    })

    const codecSecrets = [
      {
        logicalName: 'config',
        columnName: 'api_key_encrypted',
        keyId: 'default',
        serialize: JSON.stringify,
        deserialize: JSON.parse,
      },
    ] as const

    let codecService: Service<any, any, any, any, any>

    beforeEach(() => {
      codecService = createService({
        table: 'users',
        db,
        rowSchema: CodecRowSchema,
        createSchema: CodecCreateSchema,
        updateSchema: CodecUpdateSchema,
        secrets: codecSecrets,
        timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
        encryptionKeys: { default: testSecretKey },
      })
    })

    const sampleConfig = {
      client_secret: 'super-secret',
      sp_signing_key: '-----BEGIN PRIVATE KEY-----',
    }

    it('round-trips a structured secret through create -> get', async () => {
      const created = await codecService.create(
        {
          name: 'SSO One',
          email: 'sso1@test.com',
          tenant_id: 'tenant-1',
          config: sampleConfig,
        },
        { auth: {} },
      )

      // create() returns the row without secrets by default.
      expect(created.config).toBeUndefined()

      const withSecrets = await codecService.get(created.id, {
        auth: {},
        includeSecrets: true,
      })
      expect(withSecrets?.config).toEqual(sampleConfig)

      // The DB column stores an encrypted packed string, not raw JSON.
      const raw = bunDb
        .prepare('SELECT api_key_encrypted FROM users WHERE id = ?')
        .get(created.id) as any
      expect(raw.api_key_encrypted).toMatch(
        /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
      )
    })

    it('round-trips a structured secret through update -> get', async () => {
      const created = await codecService.create(
        {
          name: 'SSO Two',
          email: 'sso2@test.com',
          tenant_id: 'tenant-1',
          config: sampleConfig,
        },
        { auth: {} },
      )

      const newConfig = { client_secret: 'rotated', extra: [1, 2, 3] }
      await codecService.update(created.id, { config: newConfig }, { auth: {} })

      const updated = await codecService.get(created.id, {
        auth: {},
        includeSecrets: true,
      })
      expect(updated?.config).toEqual(newConfig)
    })

    it('round-trips structured secrets through list (includeSecrets: true)', async () => {
      await codecService.create(
        {
          name: 'SSO List',
          email: 'ssolist@test.com',
          tenant_id: 'tenant-codec',
          config: sampleConfig,
        },
        { auth: {} },
      )

      const listed = await codecService.list({
        auth: {},
        where: { tenant_id: 'tenant-codec' },
        includeSecrets: true,
      })

      expect(listed.result).toHaveLength(1)
      expect(listed.result[0].config).toEqual(sampleConfig)
    })

    it('round-trips structured secrets through query (includeSecrets: true)', async () => {
      const created = await codecService.create(
        {
          name: 'SSO Query',
          email: 'ssoquery@test.com',
          tenant_id: 'tenant-1',
          config: sampleConfig,
        },
        { auth: {} },
      )

      const rows = await codecService.query(
        'SELECT * FROM users WHERE id = ?',
        { auth: {}, includeSecrets: true },
        created.id,
      )

      expect(rows).toHaveLength(1)
      expect((rows[0] as any).config).toEqual(sampleConfig)
    })

    it('does not run the codec or expose the field when includeSecrets is false', async () => {
      const created = await codecService.create(
        {
          name: 'SSO Hidden',
          email: 'ssohidden@test.com',
          tenant_id: 'tenant-1',
          config: sampleConfig,
        },
        { auth: {} },
      )

      const fetched = await codecService.get(created.id, { auth: {} })
      expect(fetched).not.toBeNull()
      expect('config' in (fetched as object)).toBe(false)
      expect((fetched as any).api_key_encrypted).toBeUndefined()
    })

    it('skips the codec when no secret is provided and omits the field on read', async () => {
      const created = await codecService.create(
        {
          name: 'SSO Null',
          email: 'ssonull@test.com',
          tenant_id: 'tenant-1',
          // config intentionally omitted
        },
        { auth: {} },
      )

      const raw = bunDb
        .prepare('SELECT api_key_encrypted FROM users WHERE id = ?')
        .get(created.id) as any
      expect(raw.api_key_encrypted).toBeNull()

      const withSecrets = await codecService.get(created.id, {
        auth: {},
        includeSecrets: true,
      })
      expect('config' in (withSecrets as object)).toBe(false)
    })

    it('throws ValidationError when only serialize is provided', () => {
      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: CodecRowSchema,
          createSchema: CodecCreateSchema,
          updateSchema: CodecUpdateSchema,
          secrets: [
            {
              logicalName: 'config',
              columnName: 'api_key_encrypted',
              keyId: 'default',
              serialize: JSON.stringify,
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).toThrow(ValidationError)
    })

    it('throws ValidationError when only deserialize is provided', () => {
      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: CodecRowSchema,
          createSchema: CodecCreateSchema,
          updateSchema: CodecUpdateSchema,
          secrets: [
            {
              logicalName: 'config',
              columnName: 'api_key_encrypted',
              keyId: 'default',
              deserialize: JSON.parse,
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).toThrow(/must define both 'serialize' and 'deserialize'/)
    })

    it('allows a secret with neither serialize nor deserialize', () => {
      expect(() =>
        createService({
          table: 'users',
          db,
          rowSchema: CodecRowSchema,
          createSchema: CodecCreateSchema,
          updateSchema: CodecUpdateSchema,
          secrets: [
            {
              logicalName: 'config',
              columnName: 'api_key_encrypted',
              keyId: 'default',
            },
          ],
          encryptionKeys: { default: testSecretKey },
        }),
      ).not.toThrow()
    })
  })

  describe('Error Handling', () => {
    it('should throw appropriate error types', () => {
      expect(() => {
        throw new NotFoundError('users', 1)
      }).toThrow(NotFoundError)

      expect(() => {
        throw new ValidationError('Invalid data')
      }).toThrow(ValidationError)

      expect(() => {
        throw new EncryptionError('Encryption failed')
      }).toThrow(EncryptionError)
    })

    it('should include error details', () => {
      const error = new NotFoundError('users', 123, { extra: 'info' })

      expect(error.code).toBe('NOT_FOUND')
      expect(error.statusCode).toBe(404)
      expect(error.details).toEqual({ extra: 'info' })
    })

    it('should handle database constraint violations', async () => {
      // Try to create duplicate email
      await expect(
        testService.create(
          {
            name: 'Duplicate',
            email: 'john@test.com', // Already exists
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        ),
      ).rejects.toThrow()
    })
  })

  describe('Performance and Edge Cases', () => {
    it('should handle large result sets efficiently', async () => {
      // Insert many test records
      const stmt = bunDb.prepare(`
        INSERT INTO users (name, email, tenant_id, created_at) 
        VALUES (?, ?, ?, ?)
      `)

      for (let i = 0; i < 100; i++) {
        stmt.run(
          `User ${i}`,
          `user${i}@test.com`,
          'tenant-test',
          '2024-01-01T00:00:00Z',
        )
      }

      const result = await testService.list({
        auth: {},
        where: { tenant_id: 'tenant-test' },
        limit: 50,
      })

      expect(result.result).toHaveLength(50)
      expect(result.nextCursor).toBeDefined()
      expect(result.prevCursor).toBeUndefined()

      // Verify first few records have expected structure
      expect(result.result[0]).toEqual({
        id: 4,
        name: 'User 0',
        email: 'user0@test.com',
        tenant_id: 'tenant-test',
        created_at: '2024-01-01T00:00:00Z',
        updated_at: null,
      })
      expect(result.result[1]).toEqual({
        id: 5,
        name: 'User 1',
        email: 'user1@test.com',
        tenant_id: 'tenant-test',
        created_at: '2024-01-01T00:00:00Z',
        updated_at: null,
      })
    })

    it('should handle empty result sets', async () => {
      const result = await testService.list({
        auth: {},
        where: { tenant_id: 'non-existent' },
      })

      expect(result).toEqual({
        result: [],
        nextCursor: undefined,
        prevCursor: undefined,
      })
    })

    it('should handle malformed cursors gracefully', async () => {
      await expect(
        testService.list({
          auth: {},
          cursor: 'definitely-not-a-valid-cursor',
        }),
      ).rejects.toThrow(ValidationError)
    })

    it('should validate schema strictly', async () => {
      // Test with invalid email
      await expect(
        testService.create(
          {
            name: 'Test User',
            email: 'not-an-email',
            tenant_id: 'tenant-1',
          } as any,
          { auth: {} },
        ),
      ).rejects.toThrow()

      // Test with missing required field
      await expect(
        testService.create(
          {
            name: 'Test User',
            // email missing
            tenant_id: 'tenant-1',
          } as any,
          { auth: {} },
        ),
      ).rejects.toThrow()
    })
  })

  describe('Joins v2 (FK, chained, expose, filters)', () => {
    const IntegratedAccountRowSchema = z.object({
      id: z.string(),
      environment_integration_id: z.string(),
      status: z.string(),
      tenant_id: z.string(),
    })

    const EnvironmentIntegrationRowSchema = z.object({
      id: z.string(),
      environment_id: z.string(),
      integration_id: z.string(),
      is_beta: z.number(),
    })

    const IntegrationRowSchema = z.object({
      id: z.string(),
      name: z.string(),
      config: z.string(),
    })

    const integratedAccountJoins = {
      environment_integration: {
        kind: 'one' as const,
        localColumn: 'environment_integration_id',
        remote: {
          table: 'environment_integration',
          pk: 'id',
          select: ['id', 'environment_id', 'integration_id', 'is_beta'],
        },
        schema: EnvironmentIntegrationRowSchema,
        joins: {
          integration: {
            kind: 'one' as const,
            localColumn: 'integration_id',
            remote: {
              table: 'integration',
              pk: 'id',
              select: ['id', 'name', 'config'],
            },
            schema: IntegrationRowSchema,
          },
        },
      },
    }

    let iaService: Service<any, any, any, any, any>

    beforeEach(() => {
      bunDb.exec(`
        CREATE TABLE integrated_account (
          id TEXT PRIMARY KEY,
          environment_integration_id TEXT NOT NULL,
          status TEXT NOT NULL,
          tenant_id TEXT NOT NULL
        );
        CREATE TABLE environment_integration (
          id TEXT PRIMARY KEY,
          environment_id TEXT NOT NULL,
          integration_id TEXT NOT NULL,
          is_beta INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE integration (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          config TEXT
        );
        INSERT INTO integration VALUES
          ('int-1', 'Salesforce', '{"label":"SF"}'),
          ('int-2', 'HubSpot', '{"label":"HS"}');
        INSERT INTO environment_integration VALUES
          ('ei-1', 'env-1', 'int-1', 0),
          ('ei-2', 'env-2', 'int-2', 1);
        INSERT INTO integrated_account VALUES
          ('ia-1', 'ei-1', 'active', 'tnt-1'),
          ('ia-2', 'ei-2', 'active', 'tnt-1'),
          ('ia-3', 'ei-1', 'inactive', 'tnt-2');
      `)

      iaService = createService({
        table: 'integrated_account',
        db,
        rowSchema: IntegratedAccountRowSchema,
        createSchema: z.object({
          id: z.string(),
          environment_integration_id: z.string(),
          status: z.string(),
          tenant_id: z.string(),
        }),
        updateSchema: z.object({
          status: z.string().optional(),
        }),
        joins: integratedAccountJoins,
        expose: [
          {
            from: '$environment_integration.environment_id',
            as: 'environment_id',
          },
        ],
      })
    })

    it('FK join via localColumn returns nested 2-hop data on get', async () => {
      const row = await iaService.get('ia-1', {
        auth: {},
        include: {
          environment_integration: {
            integration: true,
          },
        },
      })

      expect(row).toEqual({
        id: 'ia-1',
        environment_integration_id: 'ei-1',
        status: 'active',
        tenant_id: 'tnt-1',
        environment_id: 'env-1',
        environment_integration: {
          id: 'ei-1',
          environment_id: 'env-1',
          integration_id: 'int-1',
          is_beta: 0,
          integration: {
            id: 'int-1',
            name: 'Salesforce',
            config: '{"label":"SF"}',
          },
        },
      })
    })

    it('expose projects environment_id at top level', async () => {
      const row = await iaService.get('ia-1', {
        auth: {},
        include: { environment_integration: true },
      })

      expect(row?.environment_id).toBe('env-1')
      expect(row?.environment_integration?.environment_id).toBe('env-1')
    })

    it('filters on exposed environment_id via list where', async () => {
      const page = await iaService.list({
        auth: {},
        where: { environment_id: { in: ['env-1'] } },
        include: { environment_integration: { integration: true } },
      })

      expect(page.result.map((r) => r.id).sort()).toEqual(['ia-1', 'ia-3'])
    })

    it('count matches list filter on exposed column', async () => {
      const count = await iaService.count({
        auth: {},
        where: { environment_id: 'env-2' },
        include: { environment_integration: true },
      })
      const page = await iaService.list({
        auth: {},
        where: { environment_id: 'env-2' },
        include: { environment_integration: true },
      })

      expect(count).toBe(page.result.length)
      expect(count).toBe(1)
    })

    it('scoped get returns null when environment_id not in scope', async () => {
      const row = await iaService.get('ia-1', {
        auth: {},
        where: { environment_id: { in: ['env-2'] } },
        include: { environment_integration: true },
      })

      expect(row).toBeNull()
    })

    it('include select overrides strip integration on list', async () => {
      const page = await iaService.list({
        auth: {},
        include: {
          environment_integration: {
            select: ['id', 'environment_id'],
            integration: {
              select: ['id', 'name'],
            },
          },
        },
      })

      const ia1 = page.result.find((r) => r.id === 'ia-1')
      expect(ia1?.environment_integration).toEqual({
        id: 'ei-1',
        environment_id: 'env-1',
        integration: {
          id: 'int-1',
          name: 'Salesforce',
        },
      })
      expect(ia1?.environment_integration?.integration?.config).toBeUndefined()
    })

    it('orders by joined column $integration.name', async () => {
      const page = await iaService.list({
        auth: {},
        orderBy: [{ column: '$integration.name', direction: 'asc' }],
        include: {
          environment_integration: { integration: true },
        },
      })

      expect(page.result.map((r) => r.id)).toEqual(['ia-2', 'ia-1', 'ia-3'])
    })

    it('pages by joined column $integration.name', async () => {
      // The cursor reads the joined value from the row as the query returned
      // it, under its flat key; from the nested parsed row it threw
      // "not found in row for cursor creation" on any page with a next one.
      const ids: string[] = []
      let cursor: string | undefined
      for (let page = 0; page < 5; page++) {
        const res = await iaService.list({
          auth: {},
          orderBy: [
            { column: '$integration.name', direction: 'asc' },
            { column: 'id', direction: 'asc' },
          ],
          include: {
            environment_integration: { integration: true },
          },
          limit: 1,
          ...(cursor ? { cursor } : {}),
        })
        ids.push(...res.result.map((r) => r.id))
        if (!res.nextCursor) break
        cursor = res.nextCursor
      }

      expect(ids).toEqual(['ia-2', 'ia-1', 'ia-3'])
    })

    it('orders by exposed environment_id column', async () => {
      const page = await iaService.list({
        auth: {},
        orderBy: [{ column: 'environment_id', direction: 'asc' }],
      })

      expect(page.result.map((r) => r.id)).toEqual(['ia-1', 'ia-3', 'ia-2'])
    })

    it('count with expose filter works without explicit include', async () => {
      const count = await iaService.count({
        auth: {},
        where: { environment_id: { in: ['env-1'] } },
      })

      expect(count).toBe(2)
    })

    it('paginates with join sort cursor round-trip', async () => {
      const page1 = await iaService.list({
        auth: {},
        limit: 2,
        orderBy: [{ column: 'id', direction: 'asc' }],
        include: { environment_integration: true },
      })

      expect(page1.result).toHaveLength(2)
      expect(page1.nextCursor).toBeDefined()

      const page2 = await iaService.list({
        auth: {},
        limit: 2,
        cursor: page1.nextCursor,
        orderBy: [{ column: 'id', direction: 'asc' }],
        include: { environment_integration: true },
      })

      expect(page2.result).toHaveLength(1)
      expect(page2.prevCursor).toBeDefined()
    })
  })

  describe('join column alias compatibility', () => {
    it('keeps legacy prefix_col for standard joins', () => {
      const resolved = resolveActiveJoins(
        userJoins,
        normalizeInclude({ profile: true }),
        undefined,
        'users',
        undefined,
        ['id', 'name', 'email', 'tenant_id', 'created_at', 'updated_at'],
      )
      const profile = resolved.find((r) => r.path === 'profile')!
      expect(profile.columnAliases.bio).toBe('profile_bio')
      expect(profile.columnAliases.avatar).toBe('profile_avatar')
    })

    it('uses prefix__col only when legacy alias collides with a base column', () => {
      const resolved = resolveActiveJoins(
        {
          environment_integration: {
            kind: 'one' as const,
            localColumn: 'environment_integration_id',
            remote: {
              table: 'environment_integration',
              pk: 'id',
              select: ['id', 'environment_id'],
            },
            schema: z.object({ id: z.string(), environment_id: z.string() }),
          },
        },
        normalizeInclude({ environment_integration: true }),
        undefined,
        'integrated_account',
        undefined,
        ['id', 'environment_integration_id', 'status'],
      )
      const ei = resolved.find((r) => r.path === 'environment_integration')!
      expect(ei.columnAliases.id).toBe('environment_integration__id')
      expect(ei.columnAliases.environment_id).toBe(
        'environment_integration_environment_id',
      )
    })

    it('drops prototype-polluting where keys in translateWhereForJoins', () => {
      const protoPayload = JSON.parse(
        '{"__proto__":{"polluted":true}}',
      ) as Record<string, unknown>
      const result = translateWhereForJoins(
        { ...protoPayload, status: 'active' },
        'integrated_account',
        [],
        undefined,
      )

      expect(result).toEqual({ status: 'active' })
      expect(
        Object.prototype.hasOwnProperty.call(Object.prototype, 'polluted'),
      ).toBe(false)
    })
  })
})
