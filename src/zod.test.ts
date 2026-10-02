/* global Bun */
import { describe, expect, it } from 'bun:test'
import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { build as esbuild } from 'esbuild'
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
  // `./zod.js` lets the bundler drop members the entry never touches, which a
  // named `z` binding from the index's `export * as z` may not. Only esbuild is
  // pinned here: Bun.build's shaker changes between releases (CI runs
  // `bun-version: latest`, and a newer Bun already shakes the index import as
  // well), so a Bun size comparison measures the Bun version, not this package.
  //
  // esbuild is what wrangler bundles Workers with, so it is the shaker that
  // decides the real cold-start cost. Unlike Bun it also keeps zod's English
  // message table through the subpath (see the ENGLISH note in zod.ts), which
  // is worth pinning: if a zod or esbuild upgrade changes that, Workers start
  // answering "Invalid input" to everything.
  it('shakes the subpath under esbuild and keeps English messages', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'ginger-z-esb-'))
    const bundle = async (name: string, importLine: string) => {
      const entry = join(dir, `${name}.ts`)
      writeFileSync(
        entry,
        `${importLine}\nconsole.log(z.object({ a: z.string() }).parse({ a: 'x' }))\n`,
      )
      const out = await esbuild({
        entryPoints: [entry],
        bundle: true,
        format: 'esm',
        write: false,
        logLevel: 'silent',
      })
      return out.outputFiles[0]!.text
    }
    try {
      const viaIndex = await bundle(
        'index',
        `import { z } from ${JSON.stringify(join(import.meta.dir, 'index.ts'))}`,
      )
      const viaSubpath = await bundle(
        'subpath',
        `import * as z from ${JSON.stringify(join(import.meta.dir, 'zod.ts'))}`,
      )
      expect(viaSubpath.length).toBeLessThan(viaIndex.length)
      expect(viaSubpath).toContain('Invalid input:')
      expect(viaSubpath).not.toContain('Ongeldige invoer') // no locale tables
    } finally {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  // CI runs `bun test` before `bun run build`, so dist/ does not exist yet and
  // the targets cannot be stat'ed. Instead check the mapping tsc will apply:
  // every exports target must be the rootDir -> outDir image of a source file
  // that exists, so a rename of src/zod.ts (or a tsconfig outDir change) fails
  // here rather than at publish time.
  it('publishes ./zod in package.json exports at files tsc emits from existing sources', async () => {
    const root = join(import.meta.dir, '..')
    const pkg = (await Bun.file(join(root, 'package.json')).json()) as {
      exports: Record<string, Record<string, string>>
      files: string[]
    }
    const tsconfig = (await Bun.file(join(root, 'tsconfig.json')).json()) as {
      compilerOptions: { outDir: string; rootDir: string }
    }
    const outDir = tsconfig.compilerOptions.outDir.replace(/^\.\//, '')
    const rootDir = tsconfig.compilerOptions.rootDir.replace(/^\.\//, '')
    const sourceFor = (target: string, ext: '.js' | '.d.ts') => {
      const prefix = `./${outDir}/`
      expect(target.startsWith(prefix)).toBe(true)
      return join(
        root,
        rootDir,
        target.slice(prefix.length).replace(new RegExp(`\\${ext}$`), '.ts'),
      )
    }

    for (const key of ['.', './zod']) {
      const conditions = pkg.exports[key]!
      // `types` first: resolvers take the first matching condition.
      expect(Object.keys(conditions)).toEqual(['types', 'import'])
      expect(existsSync(sourceFor(conditions.import!, '.js'))).toBe(true)
      expect(existsSync(sourceFor(conditions.types!, '.d.ts'))).toBe(true)
    }
    expect(pkg.exports['./zod']).toEqual({
      types: './dist/zod.d.ts',
      import: './dist/zod.js',
    })
    // The `files` allowlist is what actually ships the emitted outputs.
    expect(pkg.files).toContain('dist/**/*')
  })
})
