# Cloudflare D1 TypeScript Library

A comprehensive, type-safe data access layer for Cloudflare D1 with advanced features including cursor-based pagination, join support, field encryption, and a powerful hook system.

## Features

✨ **Fully Type-Safe**: Built with TypeScript and Zod v4 for complete type safety
🔐 **Field Encryption**: AES-256-GCM encryption for sensitive data  
🔗 **Advanced Joins**: Type-safe joins with conditional inclusion
📄 **Cursor Pagination**: Opaque cursor-based pagination for better performance
🎣 **Hook System**: Comprehensive before/after/error hooks inspired by Feathers.js
🧩 **Dependency Injection**: Built-in service dependency management
🛡️ **SQL Injection Protection**: All queries are safely parameterized using @truto/sqlite-builder

## Quick Start

```bash
npm install zod@^3.25.0 @truto/sqlite-builder
```

### Basic Example

```typescript
import { createService, z } from 'your-d1-library'

// Define your schemas
const UserSchema = z.object({
  id: z.number(),
  name: z.string(),
  email: z.string(),
  apiKey: z.string(), // Will be encrypted
  createdAt: z.string(),
})

const CreateUserSchema = z.object({
  name: z.string().min(1),
  email: z.string().email(),
  apiKey: z.string().min(32),
})

const UpdateUserSchema = z.object({
  name: z.string().optional(),
  email: z.string().email().optional(),
})

// Define joins
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
    },
    where: (ctx) =>
      `teams.active = 1 AND teams.tenant_id = '${ctx.auth.user?.tenantId}'`,
    schema: z.object({
      id: z.number(),
      name: z.string(),
      description: z.string(),
    }),
  },
}

// Define secret fields for encryption
const userSecrets = [
  {
    logicalName: 'apiKey',
    columnName: 'api_key_encrypted',
    keyId: 'user-secrets',
  },
] as const

// Create service with hooks for tenant filtering
const usersService = createService({
  table: 'users',
  db: env.DB, // Cloudflare D1 binding
  rowSchema: UserSchema,
  createSchema: CreateUserSchema,
  updateSchema: UpdateUserSchema,
  joins: userJoins,
  secrets: userSecrets,
  hooks: {
    // Enforce tenant filtering on all operations
    list: {
      before: async (ctx) => {
        if (!ctx.auth.user?.tenantId) {
          throw new Error('User must have a tenant ID')
        }
        // Add tenant filter
        ctx.params.where = {
          ...ctx.params.where,
          tenantId: ctx.auth.user.tenantId,
        }
      },
    },
    create: {
      before: async (ctx) => {
        // Auto-populate tenant and timestamps
        ctx.data.tenantId = ctx.auth.user.tenantId
        ctx.data.createdAt = new Date().toISOString()
      },
    },
  },
})

// Usage in Cloudflare Worker
export default {
  async fetch(request: Request, env: any): Promise<Response> {
    const auth = {
      user: {
        id: 'user-123',
        tenantId: 'tenant-456',
        roles: ['admin'],
      },
    }

    try {
      // Create a user (apiKey will be automatically encrypted)
      const newUser = await usersService.create(
        {
          name: 'John Doe',
          email: 'john@example.com',
          apiKey: 'sk_ex_abcdef1234567890abcdef1234567890',
        },
        { auth },
      )

      // List users with pagination and joins
      const users = await usersService.list({
        auth,
        limit: 10,
        include: { teams: true }, // Include team data
        orderBy: [{ column: 'createdAt', direction: 'desc' }],
      })

      // Get a specific user by ID (with decrypted secrets)
      const user = await usersService.get(newUser.id, {
        auth,
        include: { teams: true },
        includeSecrets: true, // Decrypt the apiKey
      })

      // Update a user
      const updatedUser = await usersService.update(
        newUser.id,
        { name: 'John Smith' },
        { auth },
      )

      // Count users
      const userCount = await usersService.count({ auth })

      return new Response(
        JSON.stringify({
          newUser,
          users,
          user,
          updatedUser,
          userCount,
        }),
      )
    } catch (error) {
      return new Response(
        JSON.stringify({
          error: error.message,
        }),
        { status: 500 },
      )
    }
  },
}
```

## Core Concepts

### Service Configuration

```typescript
interface ServiceOptions<TRow, TCreate, TUpdate, TJoins, TSecrets> {
  table: string // Table name
  db: D1Database // Cloudflare D1 binding
  rowSchema: TRow // Schema for row validation
  createSchema: TCreate // Schema for create validation
  updateSchema: TUpdate // Schema for update validation
  joins?: TJoins // Join definitions
  secrets?: TSecrets // Secret field definitions
  hooks?: HookMap // Hook functions
  deps?: ServiceDeps // Service dependencies
  primaryKey?: string | string[] // Primary key (default: 'id')
  defaultOrderBy?: OrderBy // Default sorting
  keyProvider?: KeyProvider // Encryption key provider
}
```

### Pagination

The library uses opaque cursor-based pagination for better performance:

```typescript
const result = await usersService.list({
  auth,
  limit: 20,
  cursor:
    'eyJvcmRlckJ5IjpbeyJjb2x1bW4iOiJpZCIsImRpcmVjdGlvbiI6ImFzYyJ9XSwiaWQiOjEwfQ==',
  orderBy: [{ column: 'createdAt', direction: 'desc' }],
})

// result.nextCursor - for next page
// result.prevCursor - for previous page
// result.result - array of records
```

### Joins

Define type-safe joins with conditional inclusion:

```typescript
const joins = {
  profile: {
    kind: 'one' as const,
    localPk: 'id',
    remote: {
      table: 'profiles',
      pk: 'user_id',
      select: ['bio', 'avatar', 'verified'],
    },
    schema: ProfileSchema,
  },
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
      select: ['id', 'name'],
    },
    where: 'teams.active = 1',
    schema: TeamSchema,
  },
}

// Use joins conditionally
const user = await usersService.get(id, {
  auth,
  include: {
    profile: true, // Include profile data
    teams: true, // Include teams array
  },
})
```

### Field Encryption

Automatically encrypt sensitive fields using AES-256-GCM:

```typescript
const secrets = [
  {
    logicalName: 'apiKey', // Field name in your schema
    columnName: 'api_key_enc', // Actual column name in DB
    keyId: 'api-keys', // Key identifier
  },
  {
    logicalName: 'ssn',
    columnName: 'ssn_encrypted',
    keyId: 'pii-data',
  },
] as const

// Set encryption key via environment variable
process.env.SECRET_KEY = 'base64-encoded-256-bit-key'

// Or provide custom key provider
const customKeyProvider = {
  async getKey(keyId: string): Promise<CryptoKey> {
    // Your custom key retrieval logic
  },
}
```

### Hooks System

Implement cross-cutting concerns with hooks:

```typescript
const hooks = {
  // Method-specific hooks
  list: {
    before: [authHook, tenantFilterHook],
    after: [auditLogHook],
    error: [errorReportingHook],
  },
  create: {
    before: async (ctx) => {
      ctx.data.createdAt = new Date().toISOString()
      ctx.data.createdBy = ctx.auth.user.id
    },
    after: async (ctx) => {
      await sendWelcomeEmail(ctx.result.email)
    },
  },

  // Global hooks (applied to all methods)
  '*': {
    before: [validateAuthHook],
    error: [globalErrorHandler],
  },
}
```

### Error Handling

The library provides a comprehensive error hierarchy:

```typescript
import {
  ServiceError, // Base error class
  NotFoundError, // Resource not found
  ValidationError, // Schema validation failed
  AuthError, // Authorization failed
  DatabaseError, // Database operation failed
  EncryptionError, // Encryption/decryption failed
  HookError, // Hook execution failed
  CursorError, // Pagination cursor invalid
} from 'your-d1-library'

try {
  await usersService.get(id, { auth })
} catch (error) {
  if (error instanceof NotFoundError) {
    return new Response('User not found', { status: 404 })
  }
  if (error instanceof ValidationError) {
    return new Response(error.message, { status: 400 })
  }
  // Handle other errors...
}
```

## API Reference

### Core Methods

All services automatically get these CRUD methods:

- `list(params)` - Paginated list with filtering and joins
- `get(id, opts)` - Get single record by ID
- `create(data, opts)` - Create new record
- `update(id, data, opts)` - Update existing record
- `delete(id, opts)` - Delete record
- `count(params)` - Count records matching criteria
- `query(sql, params)` - Execute custom SQL

### Custom Methods

Extend services with custom methods that automatically get hook support:

```typescript
class UsersService extends Service {
  async findByEmail(email: string, auth: AuthContext) {
    return this.query('SELECT * FROM users WHERE email = ? AND tenant_id = ?', {
      auth,
    }).then((rows) => rows[0] || null)
  }

  async activateUser(id: number, auth: AuthContext) {
    return this.update(id, { active: true }, { auth })
  }
}
```

## Requirements

- Cloudflare Workers environment
- D1 database binding
- TypeScript 5.0+
- Zod 3.25+ (includes v4)
- @truto/sqlite-builder 1.0+

## SQL Schema

Your D1 tables should follow these conventions:

````sql
-- Example users table
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  api_key_encrypted TEXT,  -- For encrypted fields
  tenant_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- Example junction table for many-to-many
CREATE TABLE user_teams (
```bash
# Development
bun run dev          # Start development mode with watch
bun run build        # Build the library
bun run typecheck    # Run TypeScript type checking

# Testing
bun test             # Run tests
bun run test:coverage # Run tests with coverage
bun run test:ui      # Run tests with UI

# Linting and Formatting
bun run lint         # Run ESLint
bun run lint:fix     # Fix ESLint issues
bun run format       # Format code with Prettier
bun run format:check # Check formatting

# Publishing
bun run prepublishOnly # Pre-publish checks (typecheck, lint, test, build)
````

### Project Structure

```
├── src/
│   └── index.ts          # Main entry point
├── dist/                 # Built files (auto-generated)
├── tests/                # Test files
├── package.json          # Package configuration
├── tsconfig.json         # TypeScript configuration
├── vitest.config.ts      # Vitest configuration
├── eslint.config.js      # ESLint configuration
└── README.md             # This file
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with [TypeScript](https://www.typescriptlang.org/)
- Tested with [Vitest](https://vitest.dev/)
- Bundled with [Bun](https://bun.sh/)
