import { EncryptionError } from './errors.js'
import type { KeyProvider, SecretFieldDef } from './types.js'

/**
 * Default key provider that accepts keys as constructor parameters
 */
export class DefaultKeyProvider implements KeyProvider {
  private keyCache = new Map<string, CryptoKey>()
  private keys: Map<string, string>

  constructor(keys: Record<string, string> = {}) {
    this.keys = new Map(Object.entries(keys))
    // If no keys provided but a default key exists, use it
    if (
      this.keys.size === 0 &&
      typeof process !== 'undefined' &&
      process.env?.SECRET_KEY
    ) {
      this.keys.set('default', process.env.SECRET_KEY)
    }
  }

  async getKey(keyId: string = 'default'): Promise<CryptoKey> {
    const cached = this.keyCache.get(keyId)
    if (cached) {
      return cached
    }

    const secretKey = this.keys.get(keyId)
    if (!secretKey) {
      throw new EncryptionError(`Encryption key not found for keyId: ${keyId}`)
    }

    try {
      // Convert base64 secret to ArrayBuffer
      const keyData = new Uint8Array(
        atob(secretKey)
          .split('')
          .map((char) => char.charCodeAt(0)),
      )

      const cryptoKey = await crypto.subtle.importKey(
        'raw',
        keyData,
        { name: 'AES-GCM' },
        false,
        ['encrypt', 'decrypt'],
      )

      this.keyCache.set(keyId, cryptoKey)
      return cryptoKey
    } catch (error) {
      throw new EncryptionError(
        `Failed to import encryption key: ${error instanceof Error ? error.message : 'Unknown error'}`,
        { keyId, error },
      )
    }
  }
}

/**
 * Generate a new encryption key (utility function for setup)
 */
export async function generateSecretKey(): Promise<string> {
  const key = await crypto.subtle.generateKey(
    { name: 'AES-GCM', length: 256 },
    true,
    ['encrypt', 'decrypt'],
  )

  const exported = await crypto.subtle.exportKey('raw', key)
  return btoa(String.fromCharCode(...new Uint8Array(exported)))
}

/**
 * Encrypt a plaintext value using AES-256-GCM
 * Returns a packed string in format "kid:iv:cipher" (base64 segments)
 */
export async function encrypt(
  plaintext: string,
  keyProvider: KeyProvider,
  keyId: string = 'default',
): Promise<string> {
  try {
    const key = await keyProvider.getKey(keyId)
    const iv = crypto.getRandomValues(new Uint8Array(12)) // 96-bit IV for GCM
    const encoder = new TextEncoder()
    const data = encoder.encode(plaintext)

    const ciphertext = await crypto.subtle.encrypt(
      { name: 'AES-GCM', iv },
      key,
      data,
    )

    // Pack as "kid:iv:cipher" with base64 encoding
    const kidB64 = btoa(keyId)
    const ivB64 = btoa(String.fromCharCode(...iv))
    const cipherB64 = btoa(String.fromCharCode(...new Uint8Array(ciphertext)))

    return `${kidB64}:${ivB64}:${cipherB64}`
  } catch (error) {
    throw new EncryptionError(
      `Failed to encrypt data: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { keyId, error },
    )
  }
}

/**
 * Decrypt a packed ciphertext string
 */
export async function decrypt(
  packed: string,
  keyProvider: KeyProvider,
): Promise<string> {
  try {
    const parts = packed.split(':')
    if (parts.length !== 3) {
      throw new EncryptionError('Invalid packed ciphertext format')
    }

    const [kidB64, ivB64, cipherB64] = parts
    if (!kidB64 || !ivB64 || !cipherB64) {
      throw new EncryptionError(
        'Invalid packed ciphertext format: missing parts',
      )
    }

    const keyId = atob(kidB64)
    const iv = new Uint8Array(
      atob(ivB64)
        .split('')
        .map((char) => char.charCodeAt(0)),
    )
    const ciphertext = new Uint8Array(
      atob(cipherB64)
        .split('')
        .map((char) => char.charCodeAt(0)),
    )

    const key = await keyProvider.getKey(keyId)
    const decrypted = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv },
      key,
      ciphertext,
    )

    const decoder = new TextDecoder()
    return decoder.decode(decrypted)
  } catch (error) {
    throw new EncryptionError(
      `Failed to decrypt data: ${error instanceof Error ? error.message : 'Unknown error'}`,
      { error },
    )
  }
}

/**
 * Encrypt secret fields in a data object
 */
export async function encryptSecrets<T extends Record<string, unknown>>(
  data: T,
  secrets: readonly SecretFieldDef[],
  keyProvider: KeyProvider,
): Promise<Record<string, unknown>> {
  const result: Record<string, unknown> = { ...data }

  for (const secret of secrets) {
    const { logicalName, columnName, keyId = 'default' } = secret
    const value = data[logicalName]

    if (value !== undefined && value !== null) {
      if (typeof value !== 'string') {
        throw new EncryptionError(
          `Secret field "${logicalName}" must be a string`,
          { logicalName, value },
        )
      }
      // Replace logical field with encrypted column
      delete result[logicalName]
      result[columnName] = await encrypt(value, keyProvider, keyId)
    }
  }

  return result
}

/**
 * Decrypt secret fields in a data object
 */
export async function decryptSecrets<T extends Record<string, unknown>>(
  data: T,
  secrets: readonly SecretFieldDef[],
  keyProvider: KeyProvider,
): Promise<Record<string, unknown>> {
  const result: Record<string, unknown> = { ...data }

  for (const secret of secrets) {
    const { logicalName, columnName } = secret
    const encryptedValue = data[columnName]

    if (encryptedValue !== undefined && encryptedValue !== null) {
      if (typeof encryptedValue !== 'string') {
        throw new EncryptionError(
          `Encrypted field "${columnName}" must be a string`,
          { columnName, encryptedValue },
        )
      }
      // Replace encrypted column with decrypted logical field
      delete result[columnName]
      result[logicalName] = await decrypt(encryptedValue, keyProvider)
    }
  }

  return result
}

/**
 * Get secret column names for SQL queries
 */
export function getSecretColumns(secrets: readonly SecretFieldDef[]): string[] {
  return secrets.map((s) => s.columnName)
}

/**
 * Get secret logical names for schema validation
 */
export function getSecretLogicalNames(
  secrets: readonly SecretFieldDef[],
): string[] {
  return secrets.map((s) => s.logicalName)
}
