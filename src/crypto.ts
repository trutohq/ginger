import { EncryptionError } from './errors.js'
import type { KeyProvider, SecretFieldDef } from './types.js'

/**
 * Number of bytes converted per `String.fromCharCode` call.
 *
 * `String.fromCharCode(...bytes)` spreads every byte into the argument list,
 * so a large payload blows the engine's argument limit and throws
 * "Maximum call stack size exceeded" (observed between ~256KB and 1MB, which
 * is customer-reachable through any large secret field). Chunking keeps the
 * argument count bounded and is byte-for-byte identical: `fromCharCode` maps
 * each code unit independently, so concatenating the per-chunk strings yields
 * exactly the string the single spread call produced.
 */
const BASE64_CHUNK_SIZE = 0x8000

/**
 * Encode bytes as base64 with no argument-count ceiling.
 *
 * Produces the same output as `btoa(String.fromCharCode(...bytes))` for every
 * input, including the sizes where that expression throws.
 */
export function bytesToBase64(bytes: Uint8Array): string {
  let binary = ''
  for (let i = 0; i < bytes.length; i += BASE64_CHUNK_SIZE) {
    binary += String.fromCharCode(...bytes.subarray(i, i + BASE64_CHUNK_SIZE))
  }
  return btoa(binary)
}

/**
 * Decode base64 to bytes.
 *
 * Same result as `new Uint8Array(atob(b64).split('').map(c => c.charCodeAt(0)))`
 * — `atob` only ever yields latin1 code units — without materialising a
 * one-character-string array per byte.
 */
export function base64ToBytes(b64: string): Uint8Array {
  const binary = atob(b64)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

/**
 * Default key provider that accepts keys as constructor parameters
 */
export class DefaultKeyProvider implements KeyProvider {
  private keyCache = new Map<string, CryptoKey>()
  private keys: Map<string, string>

  constructor(keys: Record<string, string> = {}) {
    // SECURITY: keys must be passed explicitly. We intentionally do NOT fall
    // back to process.env.SECRET_KEY — implicitly adopting an ambient env var
    // as the default encryption key leads to surprising key provenance in
    // shared/CI environments. Callers that want env-based keys should read
    // them explicitly and pass them here (or supply a custom keyProvider).
    this.keys = new Map(Object.entries(keys))
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
      const keyData = base64ToBytes(secretKey)

      // SECURITY: enforce a 256-bit key. The library advertises AES-256-GCM;
      // silently accepting a shorter (e.g. 128-bit) key would weaken the
      // cipher without any signal to the caller.
      if (keyData.length !== 32) {
        throw new EncryptionError(
          `Encryption key for keyId "${keyId}" must be 32 bytes (256-bit) when base64-decoded, but got ${keyData.length} bytes`,
          { keyId },
        )
      }

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
      // Preserve our own typed errors (e.g. the key-length check above).
      if (error instanceof EncryptionError) {
        throw error
      }
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
  return bytesToBase64(new Uint8Array(exported))
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
    const ivB64 = bytesToBase64(iv)
    const cipherB64 = bytesToBase64(new Uint8Array(ciphertext))

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
    const iv = base64ToBytes(ivB64)
    const ciphertext = base64ToBytes(cipherB64)

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
    const { logicalName, columnName, keyId = 'default', serialize } = secret
    const value = data[logicalName]

    if (value !== undefined && value !== null) {
      // Run the optional codec first; default is identity (string passthrough).
      let plaintext: unknown = value
      if (serialize) {
        try {
          plaintext = serialize(value)
        } catch (error) {
          throw new EncryptionError(
            `Failed to serialize secret field "${logicalName}"`,
            { logicalName, error },
          )
        }
      }

      // Guard after serialize: the value to encrypt must be a string.
      if (typeof plaintext !== 'string') {
        throw new EncryptionError(
          `Secret field "${logicalName}" must be a string`,
          { logicalName, value },
        )
      }
      // Replace logical field with encrypted column
      delete result[logicalName]
      result[columnName] = await encrypt(plaintext, keyProvider, keyId)
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
    const { logicalName, columnName, deserialize } = secret
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
      const raw = await decrypt(encryptedValue, keyProvider)

      // Run the optional codec last; default is identity (string passthrough).
      if (deserialize) {
        try {
          result[logicalName] = deserialize(raw)
        } catch (error) {
          throw new EncryptionError(
            `Failed to deserialize secret field "${columnName}"`,
            { columnName, error },
          )
        }
      } else {
        result[logicalName] = raw
      }
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
