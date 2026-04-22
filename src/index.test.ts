import { Database as BunDatabase } from 'bun:sqlite'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
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

      it('should fallback to process.env when no keys provided', async () => {
        // Set environment variable for this test
        process.env.SECRET_KEY = testSecretKey

        const provider = new DefaultKeyProvider()
        const key = await provider.getKey('default')
        expect(key).toBeInstanceOf(CryptoKey)
        expect(key.type).toBe('secret')

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

      it('does not strip main columns whose names share the join alias prefix', async () => {
        // Regression: a previous projectRow implementation removed any key in
        // the row whose name started with `${alias}_` to drop the SQL-aliased
        // join columns. That accidentally also matched legitimate main
        // columns. With the collision-free implementation, `team_size` must
        // survive even though the join alias is `team` (so the SQL aliased
        // join keys look like `team_id`, `team_name`, …).
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
})
