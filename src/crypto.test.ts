import { beforeEach, describe, expect, it } from 'bun:test'
import {
  decrypt,
  decryptSecrets,
  DefaultKeyProvider,
  encrypt,
  encryptSecrets,
  generateSecretKey,
  getSecretColumns,
  getSecretLogicalNames,
} from './crypto.js'
import { EncryptionError } from './errors.js'
import type { SecretFieldDef } from './types.js'

describe('crypto.ts', () => {
  let testKey: string
  let altKey: string

  beforeEach(async () => {
    // Generate fresh keys for each test
    testKey = await generateSecretKey()
    altKey = await generateSecretKey()
  })

  describe('generateSecretKey', () => {
    it('should generate a valid base64 encoded key', async () => {
      const key = await generateSecretKey()

      expect(typeof key).toBe('string')
      expect(key).toMatch(/^[A-Za-z0-9+/]+=*$/) // Valid base64 pattern
      expect(key.length).toBeGreaterThan(0)

      // Should decode to exactly 32 bytes (256 bits)
      const decoded = atob(key)
      expect(decoded.length).toBe(32)
    })

    it('should generate different keys on each call', async () => {
      const key1 = await generateSecretKey()
      const key2 = await generateSecretKey()
      const key3 = await generateSecretKey()

      expect(key1).not.toBe(key2)
      expect(key2).not.toBe(key3)
      expect(key1).not.toBe(key3)
    })

    it('should generate keys that work for encryption/decryption', async () => {
      const key = await generateSecretKey()
      const keyProvider = new DefaultKeyProvider({ default: key })

      const plaintext = 'test-message'
      const encrypted = await encrypt(plaintext, keyProvider)
      const decrypted = await decrypt(encrypted, keyProvider)

      expect(decrypted).toBe(plaintext)
    })
  })

  describe('DefaultKeyProvider', () => {
    describe('constructor', () => {
      it('should accept keys as constructor parameter', () => {
        const keys = {
          default: testKey,
          'custom-key': altKey,
        }
        const provider = new DefaultKeyProvider(keys)

        expect(provider).toBeInstanceOf(DefaultKeyProvider)
      })

      it('should work with empty keys object', () => {
        const provider = new DefaultKeyProvider({})

        expect(provider).toBeInstanceOf(DefaultKeyProvider)
      })

      it('should fallback to environment variable when no keys provided', () => {
        const originalEnv = process.env.SECRET_KEY
        process.env.SECRET_KEY = testKey

        try {
          const provider = new DefaultKeyProvider()
          expect(provider).toBeInstanceOf(DefaultKeyProvider)
        } finally {
          if (originalEnv !== undefined) {
            process.env.SECRET_KEY = originalEnv
          } else {
            delete process.env.SECRET_KEY
          }
        }
      })

      it('should not use environment variable when keys are provided', () => {
        const originalEnv = process.env.SECRET_KEY
        process.env.SECRET_KEY = 'should-not-use-this'

        try {
          const provider = new DefaultKeyProvider({ default: testKey })
          expect(provider).toBeInstanceOf(DefaultKeyProvider)
        } finally {
          if (originalEnv !== undefined) {
            process.env.SECRET_KEY = originalEnv
          } else {
            delete process.env.SECRET_KEY
          }
        }
      })
    })

    describe('getKey', () => {
      let provider: DefaultKeyProvider

      beforeEach(() => {
        provider = new DefaultKeyProvider({
          default: testKey,
          'alt-key': altKey,
          'custom-key': testKey,
        })
      })

      it('should return a CryptoKey for valid keyId', async () => {
        const key = await provider.getKey('default')

        expect(key).toBeInstanceOf(CryptoKey)
        expect(key.type).toBe('secret')
        expect(key.algorithm.name).toBe('AES-GCM')
        expect((key.algorithm as any).length).toBe(256)
      })

      it('should return different keys for different keyIds', async () => {
        const key1 = await provider.getKey('default')
        const key2 = await provider.getKey('alt-key')

        expect(key1).not.toBe(key2)
        expect(key1).toBeInstanceOf(CryptoKey)
        expect(key2).toBeInstanceOf(CryptoKey)
      })

      it('should cache keys and return same instance on repeated calls', async () => {
        const key1 = await provider.getKey('default')
        const key2 = await provider.getKey('default')
        const key3 = await provider.getKey('default')

        expect(key1).toBe(key2)
        expect(key2).toBe(key3)
      })

      it('should use "default" as default keyId when not specified', async () => {
        const key1 = await provider.getKey()
        const key2 = await provider.getKey('default')

        expect(key1).toBe(key2)
      })

      it('should throw EncryptionError for missing keyId', async () => {
        await expect(provider.getKey('non-existent-key')).rejects.toThrow(
          EncryptionError,
        )

        await expect(provider.getKey('non-existent-key')).rejects.toThrow(
          'Encryption key not found for keyId: non-existent-key',
        )
      })

      it('should throw EncryptionError for invalid base64 key', async () => {
        const providerWithInvalidKey = new DefaultKeyProvider({
          invalid: 'not-valid-base64-!@#$%',
        })

        await expect(providerWithInvalidKey.getKey('invalid')).rejects.toThrow(
          EncryptionError,
        )

        await expect(providerWithInvalidKey.getKey('invalid')).rejects.toThrow(
          'Failed to import encryption key',
        )
      })

      it('should handle keys of different lengths', async () => {
        // Create a base64 string that's shorter (16 bytes instead of 32)
        const shortKey = btoa('1234567890123456')
        const providerWithShortKey = new DefaultKeyProvider({
          short: shortKey,
        })

        // Web Crypto API may accept different key lengths for AES-GCM
        const key = await providerWithShortKey.getKey('short')
        expect(key).toBeInstanceOf(CryptoKey)
        expect(key.type).toBe('secret')
      })
    })
  })

  describe('encrypt', () => {
    let keyProvider: DefaultKeyProvider

    beforeEach(() => {
      keyProvider = new DefaultKeyProvider({
        default: testKey,
        'custom-key': altKey,
      })
    })

    it('should encrypt plaintext to valid packed format', async () => {
      const plaintext = 'Hello, World!'
      const encrypted = await encrypt(plaintext, keyProvider)

      expect(typeof encrypted).toBe('string')
      expect(encrypted).toMatch(
        /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
      )

      // Should have exactly 3 parts separated by colons
      const parts = encrypted.split(':')
      expect(parts).toHaveLength(3)

      // Each part should be valid base64
      parts.forEach((part) => {
        expect(part).toMatch(/^[A-Za-z0-9+/]+=*$/)
        expect(() => atob(part)).not.toThrow()
      })
    })

    it('should produce different ciphertext for same plaintext', async () => {
      const plaintext = 'test-message'

      const encrypted1 = await encrypt(plaintext, keyProvider)
      const encrypted2 = await encrypt(plaintext, keyProvider)
      const encrypted3 = await encrypt(plaintext, keyProvider)

      expect(encrypted1).not.toBe(encrypted2)
      expect(encrypted2).not.toBe(encrypted3)
      expect(encrypted1).not.toBe(encrypted3)

      // But all should decrypt to same plaintext
      expect(await decrypt(encrypted1, keyProvider)).toBe(plaintext)
      expect(await decrypt(encrypted2, keyProvider)).toBe(plaintext)
      expect(await decrypt(encrypted3, keyProvider)).toBe(plaintext)
    })

    it('should handle different keyIds', async () => {
      const plaintext = 'secret-data'

      const encrypted1 = await encrypt(plaintext, keyProvider, 'default')
      const encrypted2 = await encrypt(plaintext, keyProvider, 'custom-key')

      expect(encrypted1).not.toBe(encrypted2)

      // Both should decrypt correctly
      expect(await decrypt(encrypted1, keyProvider)).toBe(plaintext)
      expect(await decrypt(encrypted2, keyProvider)).toBe(plaintext)

      // KeyId should be embedded in the ciphertext
      const keyId1 = atob(encrypted1.split(':')[0])
      const keyId2 = atob(encrypted2.split(':')[0])
      expect(keyId1).toBe('default')
      expect(keyId2).toBe('custom-key')
    })

    it('should handle empty string', async () => {
      const encrypted = await encrypt('', keyProvider)
      const decrypted = await decrypt(encrypted, keyProvider)

      expect(decrypted).toBe('')
    })

    it('should handle special characters and unicode', async () => {
      const testCases = [
        'Hello 世界',
        '🚀 🌟 💫',
        'Special chars: !@#$%^&*()_+-=[]{}|;:,.<>?',
        'Newlines\nand\ttabs',
        'JSON: {"key": "value", "number": 123}',
        'null\x00bytes\x00included',
      ]

      for (const plaintext of testCases) {
        const encrypted = await encrypt(plaintext, keyProvider)
        const decrypted = await decrypt(encrypted, keyProvider)
        expect(decrypted).toBe(plaintext)
      }
    })

    it('should handle very long strings', async () => {
      const longString = 'x'.repeat(100000) // 100KB string

      const encrypted = await encrypt(longString, keyProvider)
      const decrypted = await decrypt(encrypted, keyProvider)

      expect(decrypted).toBe(longString)
    })

    it('should throw EncryptionError for invalid keyId', async () => {
      await expect(
        encrypt('test', keyProvider, 'non-existent'),
      ).rejects.toThrow(EncryptionError)
    })
  })

  describe('decrypt', () => {
    let keyProvider: DefaultKeyProvider

    beforeEach(() => {
      keyProvider = new DefaultKeyProvider({
        default: testKey,
        'custom-key': altKey,
      })
    })

    it('should decrypt valid ciphertext correctly', async () => {
      const plaintext = 'Test message for decryption'
      const encrypted = await encrypt(plaintext, keyProvider)
      const decrypted = await decrypt(encrypted, keyProvider)

      expect(decrypted).toBe(plaintext)
    })

    it('should throw EncryptionError for invalid format - no colons', async () => {
      await expect(decrypt('invalid-cipher-text', keyProvider)).rejects.toThrow(
        EncryptionError,
      )

      await expect(decrypt('invalid-cipher-text', keyProvider)).rejects.toThrow(
        'Invalid packed ciphertext format',
      )
    })

    it('should throw EncryptionError for invalid format - wrong number of parts', async () => {
      await expect(decrypt('one:two', keyProvider)).rejects.toThrow(
        EncryptionError,
      )

      await expect(decrypt('one:two:three:four', keyProvider)).rejects.toThrow(
        EncryptionError,
      )
    })

    it('should throw EncryptionError for empty parts', async () => {
      await expect(decrypt(':part2:part3', keyProvider)).rejects.toThrow(
        EncryptionError,
      )

      await expect(decrypt('part1::part3', keyProvider)).rejects.toThrow(
        EncryptionError,
      )

      await expect(decrypt('part1:part2:', keyProvider)).rejects.toThrow(
        EncryptionError,
      )
    })

    it('should throw EncryptionError for invalid base64 parts', async () => {
      await expect(decrypt('invalid!:Zm9v:YmFy', keyProvider)).rejects.toThrow(
        EncryptionError,
      )

      await expect(decrypt('Zm9v:invalid!:YmFy', keyProvider)).rejects.toThrow(
        EncryptionError,
      )

      await expect(decrypt('Zm9v:YmFy:invalid!', keyProvider)).rejects.toThrow(
        EncryptionError,
      )
    })

    it('should throw EncryptionError for non-existent keyId', async () => {
      // Create a valid-looking ciphertext with non-existent keyId
      const fakeKeyId = btoa('non-existent-key')
      const fakeIv = btoa('123456789012') // 12 bytes
      const fakeCipher = btoa('fake-cipher-data')

      await expect(
        decrypt(`${fakeKeyId}:${fakeIv}:${fakeCipher}`, keyProvider),
      ).rejects.toThrow(EncryptionError)
    })

    it('should throw EncryptionError for tampered ciphertext', async () => {
      const plaintext = 'original-message'
      const encrypted = await encrypt(plaintext, keyProvider)

      // Tamper with different parts
      const parts = encrypted.split(':')

      // Tamper with IV
      const tamperedIv =
        parts[0] + ':' + btoa('tampered-iv-12') + ':' + parts[2]
      await expect(decrypt(tamperedIv, keyProvider)).rejects.toThrow(
        EncryptionError,
      )

      // Tamper with ciphertext
      const tamperedCipher =
        parts[0] + ':' + parts[1] + ':' + btoa('tampered-cipher')
      await expect(decrypt(tamperedCipher, keyProvider)).rejects.toThrow(
        EncryptionError,
      )
    })

    it('should handle decryption with correct keyId embedded in ciphertext', async () => {
      const plaintext = 'message-for-custom-key'
      const encrypted = await encrypt(plaintext, keyProvider, 'custom-key')
      const decrypted = await decrypt(encrypted, keyProvider)

      expect(decrypted).toBe(plaintext)
    })
  })

  describe('encryptSecrets', () => {
    let keyProvider: DefaultKeyProvider
    let secretDefs: SecretFieldDef[]

    beforeEach(() => {
      keyProvider = new DefaultKeyProvider({
        default: testKey,
        'api-keys': altKey,
      })

      secretDefs = [
        {
          logicalName: 'apiKey',
          columnName: 'api_key_encrypted',
          keyId: 'api-keys',
        },
        {
          logicalName: 'secretToken',
          columnName: 'secret_token_enc',
          keyId: 'default',
        },
        {
          logicalName: 'password',
          columnName: 'password_hash', // No keyId, should use default
        },
      ]
    })

    it('should encrypt specified secret fields', async () => {
      const data = {
        name: 'John Doe',
        email: 'john@example.com',
        apiKey: 'sk_ex_123456789',
        secretToken: 'token_abc123',
        password: 'supersecret',
        publicField: 'not-secret',
      }

      const encrypted = await encryptSecrets(data, secretDefs, keyProvider)

      // Non-secret fields should be unchanged
      expect(encrypted.name).toBe('John Doe')
      expect(encrypted.email).toBe('john@example.com')
      expect(encrypted.publicField).toBe('not-secret')

      // Secret logical fields should be removed
      expect(encrypted.apiKey).toBeUndefined()
      expect(encrypted.secretToken).toBeUndefined()
      expect(encrypted.password).toBeUndefined()

      // Encrypted columns should be present and encrypted
      expect(typeof encrypted.api_key_encrypted).toBe('string')
      expect(typeof encrypted.secret_token_enc).toBe('string')
      expect(typeof encrypted.password_hash).toBe('string')

      expect(encrypted.api_key_encrypted).not.toBe('sk_ex_123456789')
      expect(encrypted.secret_token_enc).not.toBe('token_abc123')
      expect(encrypted.password_hash).not.toBe('supersecret')

      // Should be valid encrypted format
      expect(encrypted.api_key_encrypted).toMatch(
        /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
      )
      expect(encrypted.secret_token_enc).toMatch(
        /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
      )
      expect(encrypted.password_hash).toMatch(
        /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
      )

      // Verify decryption works
      expect(
        await decrypt(encrypted.api_key_encrypted as string, keyProvider),
      ).toBe('sk_ex_123456789')
      expect(
        await decrypt(encrypted.secret_token_enc as string, keyProvider),
      ).toBe('token_abc123')
      expect(
        await decrypt(encrypted.password_hash as string, keyProvider),
      ).toBe('supersecret')
    })

    it('should handle missing secret fields gracefully', async () => {
      const data = {
        name: 'John Doe',
        email: 'john@example.com',
        // Missing all secret fields
      }

      const encrypted = await encryptSecrets(data, secretDefs, keyProvider)

      expect(encrypted).toEqual({
        name: 'John Doe',
        email: 'john@example.com',
      })
    })

    it('should handle null secret fields gracefully', async () => {
      const data = {
        name: 'John Doe',
        apiKey: null,
        secretToken: null,
        password: null,
      }

      const encrypted = await encryptSecrets(data, secretDefs, keyProvider)

      expect(encrypted).toEqual({
        name: 'John Doe',
        apiKey: null,
        secretToken: null,
        password: null,
      })
    })

    it('should handle undefined secret fields gracefully', async () => {
      const data = {
        name: 'John Doe',
        apiKey: undefined,
        secretToken: undefined,
        password: undefined,
      }

      const encrypted = await encryptSecrets(data, secretDefs, keyProvider)

      expect(encrypted).toEqual({
        name: 'John Doe',
      })
    })

    it('should throw EncryptionError for non-string secret values', async () => {
      const testCases = [
        { apiKey: 123 },
        { secretToken: true },
        { password: { nested: 'object' } },
        { apiKey: ['array', 'value'] },
      ]

      for (const data of testCases) {
        await expect(
          encryptSecrets(data, secretDefs, keyProvider),
        ).rejects.toThrow(EncryptionError)

        await expect(
          encryptSecrets(data, secretDefs, keyProvider),
        ).rejects.toThrow('must be a string')
      }
    })

    it('should handle empty secret definitions', async () => {
      const data = {
        name: 'John Doe',
        apiKey: 'should-not-be-encrypted',
      }

      const encrypted = await encryptSecrets(data, [], keyProvider)

      expect(encrypted).toEqual(data)
    })

    it('should use correct keyId for each secret field', async () => {
      const data = {
        apiKey: 'api-secret',
        secretToken: 'token-secret',
      }

      const encrypted = await encryptSecrets(data, secretDefs, keyProvider)

      // Verify that correct keyIds are embedded
      const apiKeyId = atob(
        (encrypted.api_key_encrypted as string).split(':')[0],
      )
      const tokenKeyId = atob(
        (encrypted.secret_token_enc as string).split(':')[0],
      )

      expect(apiKeyId).toBe('api-keys')
      expect(tokenKeyId).toBe('default')
    })

    it('should not mutate original data object', async () => {
      const originalData = {
        name: 'John Doe',
        apiKey: 'secret-key',
      }
      const dataCopy = { ...originalData }

      const encrypted = await encryptSecrets(
        originalData,
        secretDefs,
        keyProvider,
      )

      expect(originalData).toEqual(dataCopy)
      expect(encrypted).not.toBe(originalData)
    })
  })

  describe('decryptSecrets', () => {
    let keyProvider: DefaultKeyProvider
    let secretDefs: SecretFieldDef[]

    beforeEach(() => {
      keyProvider = new DefaultKeyProvider({
        default: testKey,
        'api-keys': altKey,
      })

      secretDefs = [
        {
          logicalName: 'apiKey',
          columnName: 'api_key_encrypted',
          keyId: 'api-keys',
        },
        {
          logicalName: 'secretToken',
          columnName: 'secret_token_enc',
          keyId: 'default',
        },
        {
          logicalName: 'password',
          columnName: 'password_hash',
        },
      ]
    })

    it('should decrypt encrypted fields back to logical names', async () => {
      // First encrypt some data
      const originalData = {
        name: 'John Doe',
        email: 'john@example.com',
        apiKey: 'sk_ex_123456789',
        secretToken: 'token_abc123',
        password: 'supersecret',
      }

      const encrypted = await encryptSecrets(
        originalData,
        secretDefs,
        keyProvider,
      )

      // Now decrypt it back
      const decrypted = await decryptSecrets(encrypted, secretDefs, keyProvider)

      expect(decrypted).toEqual({
        name: 'John Doe',
        email: 'john@example.com',
        apiKey: 'sk_ex_123456789',
        secretToken: 'token_abc123',
        password: 'supersecret',
      })
    })

    it('should handle missing encrypted fields gracefully', async () => {
      const data = {
        name: 'John Doe',
        email: 'john@example.com',
        // Missing all encrypted columns
      }

      const decrypted = await decryptSecrets(data, secretDefs, keyProvider)

      expect(decrypted).toEqual({
        name: 'John Doe',
        email: 'john@example.com',
      })
    })

    it('should handle null encrypted fields gracefully', async () => {
      const data = {
        name: 'John Doe',
        api_key_encrypted: null,
        secret_token_enc: null,
        password_hash: null,
      }

      const decrypted = await decryptSecrets(data, secretDefs, keyProvider)

      expect(decrypted).toEqual({
        name: 'John Doe',
        api_key_encrypted: null,
        secret_token_enc: null,
        password_hash: null,
      })
    })

    it('should handle undefined encrypted fields gracefully', async () => {
      const data = {
        name: 'John Doe',
        api_key_encrypted: undefined,
        secret_token_enc: undefined,
        password_hash: undefined,
      }

      const decrypted = await decryptSecrets(data, secretDefs, keyProvider)

      expect(decrypted).toEqual({
        name: 'John Doe',
      })
    })

    it('should throw EncryptionError for non-string encrypted values', async () => {
      const testCases = [
        { api_key_encrypted: 123 },
        { secret_token_enc: true },
        { password_hash: { nested: 'object' } },
        { api_key_encrypted: ['array', 'value'] },
      ]

      for (const data of testCases) {
        await expect(
          decryptSecrets(data, secretDefs, keyProvider),
        ).rejects.toThrow(EncryptionError)

        await expect(
          decryptSecrets(data, secretDefs, keyProvider),
        ).rejects.toThrow('must be a string')
      }
    })

    it('should handle empty secret definitions', async () => {
      const data = {
        name: 'John Doe',
        api_key_encrypted: 'should-not-be-decrypted',
      }

      const decrypted = await decryptSecrets(data, [], keyProvider)

      expect(decrypted).toEqual(data)
    })

    it('should throw EncryptionError for invalid encrypted data', async () => {
      const data = {
        api_key_encrypted: 'invalid-encrypted-data',
      }

      await expect(
        decryptSecrets(data, secretDefs, keyProvider),
      ).rejects.toThrow(EncryptionError)
    })

    it('should not mutate original data object', async () => {
      const apiKeyEncrypted = await encrypt('secret-key', keyProvider)
      const originalData = {
        name: 'John Doe',
        api_key_encrypted: apiKeyEncrypted,
      }
      const dataCopy = { ...originalData }

      const decrypted = await decryptSecrets(
        originalData,
        secretDefs,
        keyProvider,
      )

      expect(originalData).toEqual(dataCopy)
      expect(decrypted).not.toBe(originalData)
    })

    it('should handle partial encrypted data correctly', async () => {
      const apiKeyEncrypted = await encrypt(
        'api-secret',
        keyProvider,
        'api-keys',
      )

      const data = {
        name: 'John Doe',
        api_key_encrypted: apiKeyEncrypted,
        // Missing other encrypted fields
      }

      const decrypted = await decryptSecrets(data, secretDefs, keyProvider)

      expect(decrypted).toEqual({
        name: 'John Doe',
        apiKey: 'api-secret',
      })
    })
  })

  describe('getSecretColumns', () => {
    it('should return array of column names', () => {
      const secretDefs: SecretFieldDef[] = [
        { logicalName: 'apiKey', columnName: 'api_key_encrypted' },
        { logicalName: 'password', columnName: 'password_hash' },
        { logicalName: 'token', columnName: 'token_enc' },
      ]

      const columns = getSecretColumns(secretDefs)

      expect(columns).toEqual([
        'api_key_encrypted',
        'password_hash',
        'token_enc',
      ])
    })

    it('should return empty array for empty input', () => {
      const columns = getSecretColumns([])

      expect(columns).toEqual([])
    })

    it('should preserve order of definitions', () => {
      const secretDefs: SecretFieldDef[] = [
        { logicalName: 'third', columnName: 'col_c' },
        { logicalName: 'first', columnName: 'col_a' },
        { logicalName: 'second', columnName: 'col_b' },
      ]

      const columns = getSecretColumns(secretDefs)

      expect(columns).toEqual(['col_c', 'col_a', 'col_b'])
    })

    it('should handle duplicate column names', () => {
      const secretDefs: SecretFieldDef[] = [
        { logicalName: 'key1', columnName: 'shared_column' },
        { logicalName: 'key2', columnName: 'shared_column' },
      ]

      const columns = getSecretColumns(secretDefs)

      expect(columns).toEqual(['shared_column', 'shared_column'])
    })
  })

  describe('getSecretLogicalNames', () => {
    it('should return array of logical names', () => {
      const secretDefs: SecretFieldDef[] = [
        { logicalName: 'apiKey', columnName: 'api_key_encrypted' },
        { logicalName: 'password', columnName: 'password_hash' },
        { logicalName: 'token', columnName: 'token_enc' },
      ]

      const names = getSecretLogicalNames(secretDefs)

      expect(names).toEqual(['apiKey', 'password', 'token'])
    })

    it('should return empty array for empty input', () => {
      const names = getSecretLogicalNames([])

      expect(names).toEqual([])
    })

    it('should preserve order of definitions', () => {
      const secretDefs: SecretFieldDef[] = [
        { logicalName: 'third', columnName: 'col_c' },
        { logicalName: 'first', columnName: 'col_a' },
        { logicalName: 'second', columnName: 'col_b' },
      ]

      const names = getSecretLogicalNames(secretDefs)

      expect(names).toEqual(['third', 'first', 'second'])
    })

    it('should handle duplicate logical names', () => {
      const secretDefs: SecretFieldDef[] = [
        { logicalName: 'duplicated', columnName: 'col_a' },
        { logicalName: 'duplicated', columnName: 'col_b' },
      ]

      const names = getSecretLogicalNames(secretDefs)

      expect(names).toEqual(['duplicated', 'duplicated'])
    })
  })

  describe('Integration tests', () => {
    let keyProvider: DefaultKeyProvider
    let secretDefs: SecretFieldDef[]

    beforeEach(() => {
      keyProvider = new DefaultKeyProvider({
        default: testKey,
        'api-keys': altKey,
      })

      secretDefs = [
        {
          logicalName: 'apiKey',
          columnName: 'api_key_encrypted',
          keyId: 'api-keys',
        },
        { logicalName: 'secret', columnName: 'secret_enc', keyId: 'default' },
      ]
    })

    it('should handle full encrypt/decrypt cycle', async () => {
      const originalData = {
        id: 1,
        name: 'Test User',
        apiKey: 'sk_ex_abcdef123456',
        secret: 'very-secret-token',
        publicData: 'not-encrypted',
      }

      // Encrypt secrets
      const encrypted = await encryptSecrets(
        originalData,
        secretDefs,
        keyProvider,
      )

      expect(encrypted).toEqual({
        id: 1,
        name: 'Test User',
        publicData: 'not-encrypted',
        api_key_encrypted: expect.stringMatching(
          /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
        ),
        secret_enc: expect.stringMatching(
          /^[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*:[A-Za-z0-9+/]+=*$/,
        ),
      })

      // Decrypt secrets
      const decrypted = await decryptSecrets(encrypted, secretDefs, keyProvider)

      expect(decrypted).toEqual(originalData)
    })

    it('should handle mixed scenarios with some fields missing', async () => {
      const partialData = {
        name: 'Partial User',
        apiKey: 'sk_ex_partial123',
        // secret field missing
        otherField: 'preserved',
      }

      const encrypted = await encryptSecrets(
        partialData,
        secretDefs,
        keyProvider,
      )
      const decrypted = await decryptSecrets(encrypted, secretDefs, keyProvider)

      expect(decrypted).toEqual(partialData)
    })

    it('should maintain data integrity across multiple operations', async () => {
      const testData = {
        apiKey: 'sk_ex_integrity_test',
        secret: 'super-secret-value',
      }

      // Multiple encrypt/decrypt cycles
      let current: any = testData
      for (let i = 0; i < 5; i++) {
        const encrypted = await encryptSecrets(current, secretDefs, keyProvider)
        current = await decryptSecrets(encrypted, secretDefs, keyProvider)
      }

      expect(current).toEqual(testData)
    })
  })
})
