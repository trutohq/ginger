/**
 * Complete example of using the D1 library with:
 * - Secret apiKey field
 * - Join to teams table
 * - Custom method withMembership
 * - Hook that enforces tenant filtering via auth.user
 */

import { createService, z, type JoinDef, type SecretFieldDef } from './index.js'

// Schema definitions
const UserRowSchema = z.object({
  id: z.number(),
  name: z.string(),
  email: z.string(),
  apiKey: z.string(), // This will be encrypted
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
    where: (ctx) =>
      `teams.active = 1 AND teams.tenant_id = '${ctx.auth.user?.tenantId || ''}'`,
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
 * Factory function to create users service with tenant hooks
 */
function createUsersService(db: any) {
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
    hooks: {
      // Enforce tenant filtering on all operations
      list: {
        before: async (ctx: any) => {
          if (!ctx.auth.user?.tenantId) {
            throw new Error('User must have a tenant ID')
          }
          // Add tenant filter to where clause
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
          // This would be handled by ensuring the ID belongs to the tenant
          // In practice, you'd modify the query to include tenant check
        },
      },
      create: {
        before: async (ctx: any) => {
          if (!ctx.auth.user?.tenantId) {
            throw new Error('User must have a tenant ID')
          }
          // Ensure created records belong to the user's tenant
          ctx.data.tenantId = ctx.auth.user.tenantId
          ctx.data.createdAt = new Date().toISOString()
          ctx.data.updatedAt = new Date().toISOString()
        },
      },
      update: {
        before: async (ctx: any) => {
          ctx.data.updatedAt = new Date().toISOString()
        },
      },
    },
  })
}

/**
 * Extended users service with custom methods
 */
class UsersService {
  private baseService: ReturnType<typeof createUsersService>

  constructor(db: any) {
    this.baseService = createUsersService(db)
  }

  // Delegate all base methods
  async list(...args: Parameters<typeof this.baseService.list>) {
    return this.baseService.list(...args)
  }

  async get(...args: Parameters<typeof this.baseService.get>) {
    return this.baseService.get(...args)
  }

  async create(...args: Parameters<typeof this.baseService.create>) {
    return this.baseService.create(...args)
  }

  async update(...args: Parameters<typeof this.baseService.update>) {
    return this.baseService.update(...args)
  }

  async delete(...args: Parameters<typeof this.baseService.delete>) {
    return this.baseService.delete(...args)
  }

  async count(...args: Parameters<typeof this.baseService.count>) {
    return this.baseService.count(...args)
  }

  async query(...args: Parameters<typeof this.baseService.query>) {
    return this.baseService.query(...args)
  }
  /**
   * Custom method: Get users with their team memberships
   */
  async withMembership(auth: { user?: { id: string; tenantId: string } } = {}) {
    const ctx = {
      auth,
      db: this.db,
      deps: this.deps,
      method: 'withMembership',
      params: { auth },
    }

    try {
      await this.runHooks('before', 'withMembership', ctx)

      // List users with teams included
      const users = await this.list({
        auth,
        include: { teams: true },
        orderBy: [{ column: 'name', direction: 'asc' }],
      })

      // Transform the data to include membership info
      const usersWithMembership = users.result.map((user) => ({
        ...user,
        membershipCount: Array.isArray(user.teams) ? user.teams.length : 0,
        isTeamMember: Array.isArray(user.teams) && user.teams.length > 0,
      }))

      const result = {
        ...users,
        result: usersWithMembership,
      }

      ctx.result = result
      await this.runHooks('after', 'withMembership', ctx)

      return result
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'withMembership', ctx)
      throw error
    }
  }

  /**
   * Custom method: Get user by API key (encrypted lookup)
   */
  async findByApiKey(
    apiKey: string,
    auth: { user?: { tenantId: string } } = {},
  ) {
    const ctx = {
      auth,
      db: this.db,
      deps: this.deps,
      method: 'findByApiKey',
      params: { apiKey, auth },
    }

    try {
      await this.runHooks('before', 'findByApiKey', ctx)

      // In practice, you'd need to implement encrypted search
      // This is a simplified version that lists all users and filters
      const users = await this.list({
        auth,
        includeSecrets: true,
        limit: 1000, // Adjust based on your needs
      })

      const user = users.result.find((u) => u.apiKey === apiKey)

      ctx.result = user || null
      await this.runHooks('after', 'findByApiKey', ctx)

      return user || null
    } catch (error) {
      ctx.error = error as Error
      await this.runHooks('error', 'findByApiKey', ctx)
      throw error
    }
  }
}

/**
 * Example usage in a Cloudflare Worker
 */
export default {
  async fetch(request: Request, env: any, ctx: any): Promise<Response> {
    // Initialize the service with the D1 binding
    const usersService = new UsersService({
      table: 'users',
      db: env.DB, // Cloudflare D1 binding
      rowSchema: UserRowSchema,
      createSchema: UserCreateSchema,
      updateSchema: UserUpdateSchema,
      joins: userJoins,
      secrets: userSecrets,
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

      // Example: Get users with membership info
      const usersWithMembership = await usersService.withMembership({
        user: {
          id: 'current-user-id',
          tenantId: 'tenant-123',
        },
      })

      // Example: Find user by API key
      const userByApiKey = await usersService.findByApiKey(
        'sk_ex_abcdef1234567890abcdef1234567890',
        {
          user: { tenantId: 'tenant-123' },
        },
      )

      return new Response(
        JSON.stringify({
          newUser,
          users,
          usersWithMembership,
          userByApiKey,
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
