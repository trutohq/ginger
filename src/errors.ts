/**
 * Base service error class
 */
export class ServiceError extends Error {
  public readonly code: string
  public readonly statusCode: number
  public readonly details?: unknown

  constructor(
    message: string,
    code: string,
    statusCode: number = 500,
    details?: unknown,
  ) {
    super(message)
    this.name = this.constructor.name
    this.code = code
    this.statusCode = statusCode
    this.details = details

    // Restore prototype chain for instanceof checks
    Object.setPrototypeOf(this, new.target.prototype)
  }
}

/**
 * Resource not found error
 */
export class NotFoundError extends ServiceError {
  constructor(resource: string, id: string | number, details?: unknown) {
    super(`${resource} with id "${id}" not found`, 'NOT_FOUND', 404, details)
  }
}

/**
 * Validation error for schema validation failures
 */
export class ValidationError extends ServiceError {
  constructor(message: string, details?: unknown) {
    super(message, 'VALIDATION_ERROR', 400, details)
  }
}

/**
 * Authorization error for access control failures
 */
export class AuthError extends ServiceError {
  constructor(message: string = 'Access denied', details?: unknown) {
    super(message, 'AUTH_ERROR', 403, details)
  }
}

/**
 * Database operation error
 */
export class DatabaseError extends ServiceError {
  constructor(message: string, details?: unknown) {
    super(message, 'DATABASE_ERROR', 500, details)
  }
}

/**
 * Encryption/decryption error
 */
export class EncryptionError extends ServiceError {
  constructor(message: string, details?: unknown) {
    super(message, 'ENCRYPTION_ERROR', 500, details)
  }
}

/**
 * Hook execution error
 */
export class HookError extends ServiceError {
  constructor(
    hookPhase: string,
    methodName: string,
    originalError: Error,
    details?: unknown,
  ) {
    super(
      `Hook error in ${hookPhase} phase of ${methodName}: ${originalError.message}`,
      'HOOK_ERROR',
      500,
      details
        ? { ...(details as Record<string, unknown>), originalError }
        : { originalError },
    )
  }
}

/**
 * Pagination cursor error
 */
export class CursorError extends ServiceError {
  constructor(message: string, details?: unknown) {
    super(message, 'CURSOR_ERROR', 400, details)
  }
}

/**
 * SQL builder error
 */
export class SqlBuilderError extends ServiceError {
  constructor(message: string, details?: unknown) {
    super(message, 'SQL_BUILDER_ERROR', 500, details)
  }
}

/**
 * Dependency injection error
 */
export class DependencyError extends ServiceError {
  constructor(message: string, details?: unknown) {
    super(message, 'DEPENDENCY_ERROR', 500, details)
  }
}
