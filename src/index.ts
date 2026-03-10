/**
 * Ginger — A type-safe SQLite data access layer
 *
 * Works with Cloudflare D1, Bun SQLite, and Durable Object SqlStorage.
 *
 * Features:
 * - Cursor-based pagination
 * - Type-safe joins with conditional inclusion
 * - AES-256-GCM encryption for secret fields
 * - Comprehensive hook system
 * - Dependency injection
 * - Safe SQL generation
 */

// Main Service class
export { Service } from './service.js'
import { Service } from './service.js'

// Type definitions
export type {
  AuthContext,
  BaseCtx,
  BaseService,
  ComputeJoins,
  CountParams,
  CreateParams,
  CursorToken,
  // Core types
  Database,
  ExecResult,
  PreparedStatement,
  QueryResult,
  DeleteParams,
  ErrorCtx,
  GetParams,
  Hook,
  HookMap,
  HookPhase,
  // Configuration
  JoinDef,
  JoinKind,
  KeyProvider,
  // Parameters
  ListParams,
  ListResult,
  MethodName,
  MethodOptions,
  OrderBy,
  OrderDirection,
  QueryParams,
  SecretFieldDef,
  ServiceCreate,
  ServiceDeps,
  ServiceOptions,
  // Helper types
  ServiceRow,
  ServiceUpdate,
  UpdateParams,
} from './types.js'

// Error classes
export {
  AuthError,
  CursorError,
  DatabaseError,
  DependencyError,
  EncryptionError,
  HookError,
  NotFoundError,
  ServiceError,
  SqlBuilderError,
  ValidationError,
} from './errors.js'

// Encryption utilities
export {
  decrypt,
  decryptSecrets,
  DefaultKeyProvider,
  encrypt,
  encryptSecrets,
  generateSecretKey,
  getSecretColumns,
  getSecretLogicalNames,
} from './crypto.js'

// Pagination utilities
export {
  buildCursorConditions,
  createCursor,
  decodeCursor,
  encodeCursor,
  getDefaultOrderBy,
  reverseOrderBy,
  validateOrderBy,
} from './pagination.js'

// SQL builder utilities
export {
  buildCount,
  buildDelete,
  buildInsert,
  buildSelect,
  buildSelectById,
  buildUpdate,
  escapeColumn,
  escapeTable,
} from './sql-builder.js'

// Database adapters
export { fromBunSqlite, fromDurableObjectStorage } from './adapters/index.js'
export type {
  BunSqliteDatabase,
  BunSqliteStatement,
  DurableObjectSqlStorage,
  SqlStorageCursor,
} from './adapters/index.js'

// Re-export zod v4 for convenience
export * as z from 'zod/v4'

/**
 * Create a new service instance with the provided configuration
 *
 * @example
 * ```typescript
 * import { createService, z } from 'ginger'
 *
 * const userService = createService({
 *   table: 'users',
 *   db, // Database instance (D1 binding, fromBunSqlite, or fromDurableObjectStorage)
 *   rowSchema: z.object({
 *     id: z.number(),
 *     name: z.string(),
 *     email: z.string(),
 *   }),
 *   createSchema: z.object({
 *     name: z.string(),
 *     email: z.string(),
 *   }),
 *   updateSchema: z.object({
 *     name: z.string().optional(),
 *     email: z.string().optional(),
 *   }),
 * })
 * ```
 */
export function createService<
  TRow extends import('zod/v4').ZodTypeAny,
  TCreate extends import('zod/v4').ZodTypeAny,
  TUpdate extends import('zod/v4').ZodTypeAny,
  TJoins extends Record<string, import('./types.js').JoinDef> = Record<
    string,
    never
  >,
  TSecrets extends
    | readonly import('./types.js').SecretFieldDef[]
    | undefined = undefined,
>(
  options: import('./types.js').ServiceOptions<
    TRow,
    TCreate,
    TUpdate,
    TJoins,
    TSecrets
  >,
): import('./service.js').Service<TRow, TCreate, TUpdate, TJoins, TSecrets> {
  return new Service(options)
}

// Default export for convenience
export default {
  createService,
}
