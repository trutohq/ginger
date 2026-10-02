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
  // zod's ~50 locale tables onto its cold-start path. The markers are each
  // table's own "invalid input" phrase, taken from the shipped locale files;
  // ASCII ones survive a bundler that escapes non-ASCII (esbuild's default).
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
      expect(text).not.toContain('Ongeldige invoer') // nl
      expect(text).not.toContain('Entrée invalide') // fr
      expect(text).not.toContain('Неверный ввод') // ru
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  // The subpath is the other half of the same fix: a namespace import of
  // `./zod.js` lets the bundler drop members the entry never touches, which the
  // index's `export * as z` cannot. toJSONSchema is a large module nothing here
  // calls, so its marker string being absent proves the shaking happened.
  it('lets a bundle that uses `import * as z` from the subpath shake unused zod', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'ginger-z-sub-'))
    const bundle = async (importLine: string) => {
      const entry = join(dir, `entry-${Math.random().toString(36).slice(2)}.ts`)
      writeFileSync(
        entry,
        `${importLine}\nconsole.log(z.object({ a: z.string() }).parse({ a: 'x' }))\n`,
      )
      const out = await Bun.build({ entrypoints: [entry], target: 'browser' })
      expect(out.success).toBe(true)
      return (await out.outputs[0]!.text()).length
    }
    try {
      const viaIndex = await bundle(
        `import { z } from ${JSON.stringify(join(import.meta.dir, 'index.ts'))}`,
      )
      const viaSubpath = await bundle(
        `import * as z from ${JSON.stringify(join(import.meta.dir, 'zod.ts'))}`,
      )
      expect(viaSubpath).toBeLessThan(viaIndex)
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  it('publishes ./zod in package.json exports at files tsc emits', async () => {
    const pkg = (await Bun.file(
      join(import.meta.dir, '..', 'package.json'),
    ).json()) as {
      exports: Record<string, { import: string; types: string }>
    }
    // rootDir is src and outDir is dist, so src/zod.ts is dist/zod.js + .d.ts.
    expect(pkg.exports['./zod']).toEqual({
      import: './dist/zod.js',
      types: './dist/zod.d.ts',
    })
  })
})
