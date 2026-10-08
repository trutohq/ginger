/**
 * `z.core`, minus `locales` — see `zod.ts` for why. Kept as its own module
 * because a name can only be shadowed in the module whose star-export it
 * comes from.
 */
export * from 'zod/v4/core'

/** @deprecated See `locales` in `zod.ts`. */
export const locales: never = undefined as never
