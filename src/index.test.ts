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
  const UserRowSchema = z.object({
    id: z.number(),
    name: z.string(),
    email: z.string(),
    api_key_encrypted: z.string().optional(),
    tenant_id: z.string(),
    created_at: z.string(),
    updated_at: z.string().nullable().optional(),
  })

  const UserCreateSchema = z.object({
    name: z.string().min(1),
    email: z.string().email(),
    api_key_encrypted: z.string().optional(),
    tenant_id: z.string(),
  })

  const UserUpdateSchema = z.object({
    name: z.string().optional(),
    email: z.string().email().optional(),
    api_key_encrypted: z.string().optional(),
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
      logicalName: 'api_key_encrypted',
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
          id: 4, // Should be the next auto-increment ID
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
          api_key_encrypted: 'sk_ex_secret123',
          email: 'john@test.com',
        }

        const encrypted = await encryptSecrets(data, userSecrets, keyProvider)

        expect(encrypted.name).toBe('John')
        expect(encrypted.email).toBe('john@test.com')
        expect(typeof encrypted.api_key_encrypted).toBe('string')
        expect(encrypted.api_key_encrypted).not.toBe('sk_ex_secret123') // Should be encrypted
        expect(encrypted.api_key_encrypted).toMatch(
          /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
        )

        // Verify it can be decrypted back to original
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
          api_key_encrypted: 'sk_ex_secret123',
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
        const data = { api_key_encrypted: 123 as any }

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
            api_key_encrypted: 'sk_ex_secret123',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        )

        expect(user).toEqual({
          id: 4, // Should be next auto-increment
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

        // API key should be encrypted in database
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
        // First create a user with encrypted secret
        const created = await serviceWithSecrets.create(
          {
            name: 'Secret User',
            email: 'secret@test.com',
            api_key_encrypted: 'sk_ex_secret123',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        )

        // Get with secrets
        const withSecrets = await serviceWithSecrets.get(created.id, {
          auth: {},
          includeSecrets: true,
        })

        expect(withSecrets).toEqual({
          id: created.id,
          name: 'Secret User',
          email: 'secret@test.com',
          api_key_encrypted: 'sk_ex_secret123',
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
        // First create a user with encrypted secret
        const created = await serviceWithSecrets.create(
          {
            name: 'Secret User',
            email: 'secret2@test.com',
            api_key_encrypted: 'sk_ex_secret456',
            tenant_id: 'tenant-1',
          },
          { auth: {} },
        )

        // Get without secrets (default)
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
        expect(withoutSecrets?.api_key_encrypted).toBeUndefined()
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
        id: 4, // Should start after existing test data
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
