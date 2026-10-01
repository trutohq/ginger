/* global Bun */
import { describe, expect, it } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import * as zodV4 from 'zod/v4'
import { z } from './index.js'

describe('z (re-exported zod v4)', () => {
  it('is the real zod v4 classic API', () => {
    const schema = z.object({ id: z.number(), name: z.string().optional() })
    expect(schema.parse({ id: 1 })).toEqual({ id: 1 })
    expect(schema.safeParse({ id: 'x' }).success).toBe(false)
    // Same classes as `zod/v4`, so `instanceof` and cross-imports agree.
    expect(z.ZodString).toBe(zodV4.ZodString)
    expect(z.toJSONSchema).toBe(zodV4.toJSONSchema)
    expect(z.iso).toBe(zodV4.iso)
    expect(z.coerce).toBe(zodV4.coerce)
  })

  it('keeps every zod export except the namespaces that carry the locale tables', () => {
    const shadowed = ['locales', 'z']
    const missing = Object.keys(zodV4).filter(
      (name) => name !== 'default' && !shadowed.includes(name) && !(name in z),
    )
    expect(missing).toEqual([])
    expect(z.locales).toBeUndefined()
    expect(z.z).toBeUndefined()
  })

  it('keeps z.core, minus its own locales', () => {
    expect(z.core.$ZodType).toBe(zodV4.core.$ZodType)
    expect(z.core.parse).toBe(zodV4.core.parse)
    expect(
      Object.keys(zodV4.core).filter(
        (name) => name !== 'locales' && !(name in z.core),
      ),
    ).toEqual([])
    expect(z.core.locales).toBeUndefined()
  })

  it('still reports errors in English', () => {
    const result = z.string().safeParse(1)
    expect(result.success).toBe(false)
    expect(result.error?.issues[0]?.message).toBe(
      'Invalid input: expected string, received number',
    )
  })

  // The reason zod.ts exists. A Worker that does `z.object(...)` must not pull
  // zod's ~50 locale tables onto its cold-start path. Russian is the marker:
  // its table is a long one, and nothing else in zod mentions this phrase.
  it('does not drag zod locale tables into a bundle that uses z', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'ginger-z-'))
    try {
      const entry = join(dir, 'entry.ts')
      writeFileSync(
        entry,
        `import { z } from ${JSON.stringify(join(import.meta.dir, 'index.ts'))}\n` +
          `console.log(z.object({ a: z.string() }).parse({ a: 'x' }))\n`,
      )
      const out = await Bun.build({
        entrypoints: [entry],
        target: 'browser',
        minify: false,
      })
      expect(out.success).toBe(true)
      const text = await out.outputs[0]!.text()
      expect(text).toContain('Invalid input') // English is still there
      expect(text).not.toContain('Недопустимый ввод') // ru
      expect(text).not.toContain('Entrée invalide') // fr
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })
})
