/**
 * Complete example of using the Ginger library with:
 * - Secret apiKey field
 * - Join to teams table
 * - Custom method withMembership
 * - Hook that enforces tenant filtering via auth.user
 */

import { createService, z, type JoinDef, type SecretFieldDef } from './index.js'

// Schema definitions
// Note: secret fields (apiKey) must NOT be in rowSchema — they are managed
// separately by the secrets config and injected only when includeSecrets: true.
const UserRowSchema = z.object({
  id: z.number(),
  name: z.string(),
  email: z.string(),
  tenantId: z.string(),
  createdAt: z.string(),
  updatedAt: z.string(),
})

const UserCreateSchema = z.object({
  name: z.string().min(1).max(255),
  email: z.string().email().max(255),
  apiKey: z.string().min(32).max(255),
  tenantId: z.string().uuid(),
})

const UserUpdateSchema = z.object({
  name: z.string().min(1).max(255).optional(),
  email: z.string().email().max(255).optional(),
  apiKey: z.string().min(32).max(255).optional(),
})

const TeamRowSchema = z.object({
  id: z.number(),
  name: z.string(),
  description: z.string(),
})

// Join definitions
// Note: Auth-based filtering (like tenant_id checks) should NOT be embedded in join definitions.
// Instead, handle them through:
// 1. Hooks that modify where clauses before queries
// 2. Proper database design with foreign keys
// 3. Service-level filtering logic
// 4. Row-level security in the database
const userJoins = {
  teams: {
    kind: 'many',
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
    where: { $teams: { active: 1 } },
    schema: TeamRowSchema,
  },
} satisfies Record<string, JoinDef>

// Secret field definitions
const userSecrets = [
  {
    logicalName: 'apiKey',
    columnName: 'api_key_encrypted',
    keyId: 'user-secrets',
  },
] as const satisfies readonly SecretFieldDef[]

/**
 * Factory function to create users service with tenant hooks and encryption keys
 */
export function createUsersService(
  db: any,
  encryptionKeys: Record<string, string>,
) {
  return createService({
    table: 'users',
    db,
    rowSchema: UserRowSchema,
    createSchema: UserCreateSchema,
    updateSchema: UserUpdateSchema,
    joins: userJoins,
    secrets: userSecrets,
    primaryKey: 'id',
    defaultOrderBy: { column: 'created_at', direction: 'desc' as const },
    encryptionKeys,
    timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
    hooks: {
      list: {
        before: async (ctx: any) => {
          if (!ctx.auth.user?.tenantId) {
            throw new Error('User must have a tenant ID')
          }
          if (!ctx.params.where) {
            ctx.params.where = {}
          }
          ctx.params.where.tenantId = ctx.auth.user.tenantId
        },
      },
      get: {
        before: async (ctx: any) => {
          if (!ctx.auth.user?.tenantId) {
            throw new Error('User must have a tenant ID')
          }
        },
      },
      create: {
        before: async (ctx: any) => {
          if (!ctx.auth.user?.tenantId) {
            throw new Error('User must have a tenant ID')
          }
          ctx.data.tenantId = ctx.auth.user.tenantId
        },
      },
    },
  })
}

/**
 * Example usage in a Cloudflare Worker
 */
export default {
  async fetch(_request: Request, env: any, _ctx: any): Promise<Response> {
    // Get encryption keys from Cloudflare environment variables/secrets
    const encryptionKeys = {
      default: env.ENCRYPTION_KEY_DEFAULT, // Store as base64 encoded key
      'user-secrets':
        env.ENCRYPTION_KEY_USER_SECRETS || env.ENCRYPTION_KEY_DEFAULT,
    }

    // Initialize the service with the database binding and encryption keys
    const usersService = createService({
      table: 'users',
      db: env.DB,
      rowSchema: UserRowSchema,
      createSchema: UserCreateSchema,
      updateSchema: UserUpdateSchema,
      joins: userJoins,
      secrets: userSecrets,
      encryptionKeys,
      timestamps: { createdAt: 'created_at', updatedAt: 'updated_at' },
      hooks: {
        list: {
          before: async (ctx: any) => {
            if (!ctx.auth.user?.tenantId) {
              throw new Error('User must have a tenant ID')
            }
            if (!ctx.params.where) {
              ctx.params.where = {}
            }
            ctx.params.where.tenantId = ctx.auth.user.tenantId
          },
        },
        create: {
          before: async (ctx: any) => {
            if (!ctx.auth.user?.tenantId) {
              throw new Error('User must have a tenant ID')
            }
            ctx.data.tenantId = ctx.auth.user.tenantId
          },
        },
      },
    })

    try {
      // Example: Create a new user
      const newUser = await usersService.create(
        {
          name: 'John Doe',
          email: 'john@example.com',
          apiKey: 'sk_ex_abcdef1234567890abcdef1234567890',
          tenantId: 'tenant-123',
        },
        {
          auth: {
            user: {
              id: 'current-user-id',
              tenantId: 'tenant-123',
              roles: ['admin'],
            },
          },
        },
      )

      // Example: List users with pagination
      const users = await usersService.list({
        auth: {
          user: {
            id: 'current-user-id',
            tenantId: 'tenant-123',
            roles: ['admin'],
          },
        },
        limit: 10,
        include: { teams: true },
        orderBy: [{ column: 'created_at', direction: 'desc' }],
      })

      // Example: Get a user with secrets included
      const userWithSecrets = await usersService.get(newUser.id, {
        auth: {
          user: {
            id: 'current-user-id',
            tenantId: 'tenant-123',
            roles: ['admin'],
          },
        },
        includeSecrets: true, // This will decrypt the API key
      })

      return new Response(
        JSON.stringify({
          newUser,
          users,
          userWithSecrets,
        }),
        {
          headers: { 'content-type': 'application/json' },
        },
      )
    } catch (error) {
      return new Response(
        JSON.stringify({
          error: error instanceof Error ? error.message : 'Unknown error',
        }),
        {
          status: 500,
          headers: { 'content-type': 'application/json' },
        },
      )
    }
  },
}

/**
 * Cloudflare Worker Environment Setup:
 *
 * 1. Add these environment variables to your Cloudflare Worker:
 *    - ENCRYPTION_KEY_DEFAULT: A base64-encoded 256-bit encryption key
 *    - ENCRYPTION_KEY_USER_SECRETS: (Optional) A separate key for user secrets
 *
 * 2. Generate encryption keys using the library:
 *    ```typescript
 *    import { generateSecretKey } from './crypto.js'
 *    const key = await generateSecretKey()
 *    console.log('ENCRYPTION_KEY_DEFAULT=' + key)
 *    ```
 *
 * 3. Bind your D1 database as "DB" in wrangler.toml:
 *    ```toml
 *    [[d1_databases]]
 *    binding = "DB"
 *    database_name = "your-database"
 *    database_id = "your-database-id"
 *    ```
 */

/**
 * SQL schema for this example:
 *
 * ```sql
 * CREATE TABLE users (
 *   id INTEGER PRIMARY KEY AUTOINCREMENT,
 *   name TEXT NOT NULL,
 *   email TEXT NOT NULL UNIQUE,
 *   api_key_encrypted TEXT NOT NULL,
 *   tenant_id TEXT NOT NULL,
 *   created_at TEXT NOT NULL,
 *   updated_at TEXT NOT NULL
 * );
 *
 * CREATE TABLE teams (
 *   id INTEGER PRIMARY KEY AUTOINCREMENT,
 *   name TEXT NOT NULL,
 *   description TEXT,
 *   tenant_id TEXT NOT NULL,
 *   active INTEGER DEFAULT 1
 * );
 *
 * CREATE TABLE user_teams (
 *   user_id INTEGER NOT NULL,
 *   team_id INTEGER NOT NULL,
 *   PRIMARY KEY (user_id, team_id),
 *   FOREIGN KEY (user_id) REFERENCES users(id),
 *   FOREIGN KEY (team_id) REFERENCES teams(id)
 * );
 *
 * CREATE INDEX idx_users_tenant_id ON users(tenant_id);
 * CREATE INDEX idx_teams_tenant_id ON teams(tenant_id);
 * ```
 */
