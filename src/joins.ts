import { compileFilter, sql } from '@truto/sqlite-builder'
import { CursorError, SqlBuilderError } from './errors.js'
import type { ExposeDef, IncludeMap, JoinDef, OrderBy } from './types.js'

/** Keys that must never be used as dynamic object property names. */
const UNSAFE_OBJECT_KEYS = new Set(['__proto__', 'constructor', 'prototype'])

/** True when `key` must not be used for dynamic assignment on a plain object. */
export function isUnsafeObjectKey(key: string): boolean {
  return UNSAFE_OBJECT_KEYS.has(key)
}

function safeSet(
  target: Record<string, unknown>,
  key: string,
  value: unknown,
): boolean {
  if (isUnsafeObjectKey(key)) return false
  target[key] = value
  return true
}

/** Normalized include tree node after parsing boolean / object include specs. */
export interface NormalizedIncludeNode {
  enabled: boolean
  select?: string[] | undefined
  children: Record<string, NormalizedIncludeNode>
}

/** A single join edge resolved from the include tree, in dependency order. */
export interface ResolvedJoin {
  /** Dot-separated path, e.g. `environment_integration.integration`. */
  path: string
  /** Key in the parent joins map. */
  name: string
  joinDef: JoinDef
  parentTable: string
  localColumn: string
  sqlTable: string
  /** Prefix for flat SQL column aliases (`${columnPrefix}_${col}` by default). */
  columnPrefix: string
  select: string[]
  /** Collision-safe flat SQL alias per selected column (assigned after resolve). */
  columnAliases: Record<string, string>
}

export function getLocalJoinColumn(joinDef: JoinDef): string {
  const col = joinDef.localColumn ?? joinDef.localPk
  if (!col) {
    throw new SqlBuilderError(
      'JoinDef requires localColumn or localPk for the parent-table FK column',
    )
  }
  return col
}

/**
 * Parse an include map into a normalized tree. `true` enables a join with
 * default columns; an object enables the join when it has `select` and/or
 * nested children.
 */
export function normalizeInclude(
  include: IncludeMap | undefined,
): Record<string, NormalizedIncludeNode> {
  if (!include) return {}

  const out: Record<string, NormalizedIncludeNode> = {}
  for (const [key, value] of Object.entries(include)) {
    if (isUnsafeObjectKey(key)) continue
    const node = normalizeIncludeValue(value)
    if (node?.enabled) out[key] = node
  }
  return out
}

function normalizeIncludeValue(
  value: IncludeMap[string] | undefined,
): NormalizedIncludeNode | null {
  if (value === undefined || value === false) return null
  if (value === true) {
    return { enabled: true, select: undefined, children: {} }
  }

  const obj = value as Record<string, unknown>
  const select = Array.isArray(obj.select)
    ? (obj.select as string[])
    : undefined
  const children: Record<string, NormalizedIncludeNode> = {}

  for (const [key, childVal] of Object.entries(obj)) {
    if (key === 'select' || isUnsafeObjectKey(key)) continue
    const child = normalizeIncludeValue(childVal as IncludeMap[string])
    if (child?.enabled) children[key] = child
  }

  const enabled = select !== undefined || Object.keys(children).length > 0

  if (!enabled) return null

  return { enabled: true, select, children }
}

/**
 * Flatten the include tree into an ordered list of joins (parents before
 * children). Also auto-includes joins required by `expose` paths.
 */
export function resolveActiveJoins(
  rootJoins: Record<string, JoinDef> | undefined,
  include: Record<string, NormalizedIncludeNode>,
  expose: readonly ExposeDef[] | undefined,
  baseTable: string,
  joinColumnOverrides?: Record<string, string[]>,
  reservedColumnNames?: readonly string[],
): ResolvedJoin[] {
  if (!rootJoins) return []

  const resolved: ResolvedJoin[] = []
  const neededPaths = new Set<string>()

  for (const exp of expose ?? []) {
    const { joinPath } = parseQualifiedPath(exp.from)
    if (joinPath) neededPaths.add(joinPath)
  }

  function walk(
    joins: Record<string, JoinDef>,
    nodes: Record<string, NormalizedIncludeNode>,
    parentTable: string,
    parentPath: string,
  ): void {
    for (const [name, node] of Object.entries(nodes)) {
      if (!node.enabled) continue
      const joinDef = joins[name]
      if (!joinDef) continue

      const path = parentPath ? `${parentPath}.${name}` : name
      neededPaths.add(path)

      const columnPrefix = joinDef.remote.alias || name
      const override =
        joinColumnOverrides?.[path] ?? joinColumnOverrides?.[name]
      const select = override ?? node.select ?? joinDef.remote.select

      resolved.push({
        path,
        name,
        joinDef,
        parentTable,
        localColumn: getLocalJoinColumn(joinDef),
        sqlTable: joinDef.remote.table,
        columnPrefix,
        select,
        columnAliases: {},
      })

      if (joinDef.joins && Object.keys(node.children).length > 0) {
        walk(joinDef.joins, node.children, joinDef.remote.table, path)
      }
    }
  }

  walk(rootJoins, include, baseTable, '')

  // Auto-include ancestor joins for expose-only paths
  for (const path of neededPaths) {
    ensurePathIncluded(
      path,
      rootJoins,
      include,
      resolved,
      baseTable,
      joinColumnOverrides,
    )
  }

  const deduped = dedupeResolvedJoins(resolved)
  const reserved = new Set(reservedColumnNames ?? [])
  for (const exp of expose ?? []) {
    reserved.add(exp.as)
  }
  assignJoinColumnAliases(deduped, reserved)
  return deduped
}

function ensurePathIncluded(
  path: string,
  rootJoins: Record<string, JoinDef>,
  include: Record<string, NormalizedIncludeNode>,
  resolved: ResolvedJoin[],
  baseTable: string,
  joinColumnOverrides?: Record<string, string[]>,
): void {
  const segments = path.split('.')
  let joins = rootJoins
  let parentTable = baseTable
  let parentPath = ''

  for (let i = 0; i < segments.length; i++) {
    const name = segments[i]!
    const subPath = parentPath ? `${parentPath}.${name}` : name

    if (!resolved.some((r) => r.path === subPath)) {
      const joinDef = joins[name]
      if (!joinDef) break

      const node = getIncludeNode(include, subPath)
      const columnPrefix = joinDef.remote.alias || name
      const override =
        joinColumnOverrides?.[subPath] ?? joinColumnOverrides?.[name]
      const select = override ?? node?.select ?? joinDef.remote.select

      resolved.push({
        path: subPath,
        name,
        joinDef,
        parentTable,
        localColumn: getLocalJoinColumn(joinDef),
        sqlTable: joinDef.remote.table,
        columnPrefix,
        select,
        columnAliases: {},
      })
    }

    const joinDef = joins[name]
    if (!joinDef) break
    parentTable = joinDef.remote.table
    parentPath = subPath
    joins = joinDef.joins ?? {}
  }
}

function getIncludeNode(
  include: Record<string, NormalizedIncludeNode>,
  path: string,
): NormalizedIncludeNode | undefined {
  const segments = path.split('.')
  let nodes = include
  let current: NormalizedIncludeNode | undefined

  for (const seg of segments) {
    current = nodes[seg]
    if (!current) return undefined
    nodes = current.children
  }
  return current
}

function dedupeResolvedJoins(joins: ResolvedJoin[]): ResolvedJoin[] {
  const seen = new Set<string>()
  const out: ResolvedJoin[] = []
  for (const j of joins) {
    if (seen.has(j.path)) continue
    seen.add(j.path)
    out.push(j)
  }
  // Parents must appear before children; preserve first-seen order within a depth.
  return out.sort((a, b) => {
    const depthA = a.path.split('.').length
    const depthB = b.path.split('.').length
    if (depthA !== depthB) return depthA - depthB
    return joins.indexOf(a) - joins.indexOf(b)
  })
}

/** Parse `$join.path.column` or `$table.column` qualified paths. */
export function parseQualifiedPath(qualified: string): {
  joinPath: string
  column: string
  isTableQualified: boolean
} {
  if (!qualified.startsWith('$')) {
    throw new SqlBuilderError(
      `Qualified path must start with '$': ${qualified}`,
    )
  }
  const body = qualified.slice(1)
  const lastDot = body.lastIndexOf('.')
  if (lastDot === -1) {
    return { joinPath: '', column: body, isTableQualified: true }
  }
  return {
    joinPath: body.slice(0, lastDot),
    column: body.slice(lastDot + 1),
    isTableQualified: false,
  }
}

/** Build LEFT JOIN fragments for all resolved joins in order. */
export function buildJoinFragments(
  resolved: ResolvedJoin[],
): ReturnType<typeof sql>[] {
  const fragments: ReturnType<typeof sql>[] = []

  for (const j of resolved) {
    if (j.joinDef.through) {
      const { through, remote } = j.joinDef
      const localCol = j.localColumn
      const throughJoin = sql`LEFT JOIN ${sql.ident(through.table)} ON ${sql.ident(`${j.parentTable}.${localCol}`)} = ${sql.ident(`${through.table}.${through.from}`)}`
      const remoteJoin = sql`LEFT JOIN ${sql.ident(remote.table)} ON ${sql.ident(`${through.table}.${through.to}`)} = ${sql.ident(`${remote.table}.${remote.pk}`)}`
      const parts = [throughJoin, remoteJoin]
      if (j.joinDef.where && Object.keys(j.joinDef.where).length > 0) {
        parts.push(sql`AND ${compileFilter(j.joinDef.where as any)}`)
      }
      fragments.push(sql.join(parts, ' '))
    } else {
      const parts = [
        sql`LEFT JOIN ${sql.ident(j.sqlTable)} ON ${sql.ident(`${j.parentTable}.${j.localColumn}`)} = ${sql.ident(`${j.sqlTable}.${j.joinDef.remote.pk}`)}`,
      ]
      if (j.joinDef.where && Object.keys(j.joinDef.where).length > 0) {
        parts.push(sql`AND ${compileFilter(j.joinDef.where as any)}`)
      }
      fragments.push(sql.join(parts, ' '))
    }
  }

  return fragments
}

/** Legacy flat SQL alias (`prefix_col`) — default for join columns. */
export function legacyJoinColumnAlias(
  columnPrefix: string,
  column: string,
): string {
  return `${columnPrefix}_${column}`
}

/**
 * Assign collision-safe flat SQL aliases on each resolved join.
 *
 * Uses the legacy `prefix_col` form unless that name is already taken by a
 * base-table column, an expose alias, or another join column — then falls
 * back to `prefix__col`.
 */
export function assignJoinColumnAliases(
  resolved: ResolvedJoin[],
  reserved: ReadonlySet<string>,
): void {
  const taken = new Set(reserved)
  for (const j of resolved) {
    const columnAliases: Record<string, string> = {}
    for (const col of j.select) {
      const legacy = legacyJoinColumnAlias(j.columnPrefix, col)
      const alias = taken.has(legacy) ? `${j.columnPrefix}__${col}` : legacy
      columnAliases[col] = alias
      taken.add(alias)
    }
    j.columnAliases = columnAliases
  }
}

/** Flat SQL alias for a column on a resolved join. */
export function flatAliasForJoinColumn(
  join: ResolvedJoin,
  column: string,
): string {
  return (
    join.columnAliases[column] ??
    legacyJoinColumnAlias(join.columnPrefix, column)
  )
}

/** SELECT column fragments for resolved joins. */
export function buildJoinSelectColumns(
  resolved: ResolvedJoin[],
): ReturnType<typeof sql>[] {
  const columns: ReturnType<typeof sql>[] = []
  for (const j of resolved) {
    for (const col of j.select) {
      columns.push(
        sql`${sql.ident(`${j.sqlTable}.${col}`)} as ${sql.ident(flatAliasForJoinColumn(j, col))}`,
      )
    }
  }
  return columns
}

/** SELECT fragments for exposed top-level columns. */
export function buildExposeSelectColumns(
  expose: readonly ExposeDef[],
  resolved: ResolvedJoin[],
  _baseTable: string,
): ReturnType<typeof sql>[] {
  const columns: ReturnType<typeof sql>[] = []

  for (const exp of expose) {
    const { joinPath, column, isTableQualified } = parseQualifiedPath(exp.from)

    if (isTableQualified && !joinPath) {
      const parts = exp.from.slice(1).split('.')
      const table = parts[0]!
      const col = parts[1] ?? column
      columns.push(sql`${sql.ident(`${table}.${col}`)} as ${sql.ident(exp.as)}`)
      continue
    }

    const match = resolved.find((r) => r.path === joinPath)
    const table = match?.sqlTable ?? joinPath.split('.').pop() ?? joinPath
    columns.push(
      sql`${sql.ident(`${table}.${column}`)} as ${sql.ident(exp.as)}`,
    )
  }

  return columns
}

/**
 * Map of SQL table name → join path for compileFilter `$alias` blocks.
 * Also maps expose aliases to their source table.
 */
export function buildJoinFilterContext(
  resolved: ResolvedJoin[],
  expose: readonly ExposeDef[] | undefined,
  baseTable: string,
): {
  tableToPath: Map<string, string>
  exposeColumns: Map<string, { table: string; column: string }>
} {
  const tableToPath = new Map<string, string>()
  for (const j of resolved) {
    tableToPath.set(j.sqlTable, j.path)
  }

  const exposeColumns = new Map<string, { table: string; column: string }>()
  for (const exp of expose ?? []) {
    const { joinPath, column, isTableQualified } = parseQualifiedPath(exp.from)
    if (isTableQualified && !joinPath) {
      const parts = exp.from.slice(1).split('.')
      exposeColumns.set(exp.as, {
        table: parts[0] ?? baseTable,
        column: parts[1] ?? column,
      })
    } else {
      const match = resolved.find((r) => r.path === joinPath)
      exposeColumns.set(exp.as, {
        table: match?.sqlTable ?? joinPath,
        column,
      })
    }
  }

  return { tableToPath, exposeColumns }
}

/**
 * Rewrite a where filter so expose aliases and join-table columns compile
 * correctly when joins are active.
 */
export function translateWhereForJoins(
  where: Record<string, unknown>,
  baseTable: string,
  resolved: ResolvedJoin[],
  expose: readonly ExposeDef[] | undefined,
): Record<string, unknown> {
  if (Object.keys(where).length === 0) return where

  const { exposeColumns } = buildJoinFilterContext(resolved, expose, baseTable)
  const joinedTables = new Set(resolved.map((r) => r.sqlTable))

  const out: Record<string, unknown> = Object.create(null) as Record<
    string,
    unknown
  >
  const aliasBlocks = new Map<string, Record<string, unknown>>()

  function mergeBlock(table: string, filter: Record<string, unknown>): void {
    if (isUnsafeObjectKey(table)) return
    let block = aliasBlocks.get(table)
    if (!block) {
      block = Object.create(null) as Record<string, unknown>
      aliasBlocks.set(table, block)
    }
    for (const [filterKey, filterValue] of Object.entries(filter)) {
      safeSet(block, filterKey, filterValue)
    }
  }

  for (const [key, value] of Object.entries(where)) {
    if (isUnsafeObjectKey(key)) continue

    if (key === 'and' || key === 'or') {
      safeSet(
        out,
        key,
        (value as unknown[]).map((sub) =>
          typeof sub === 'object' && sub !== null
            ? translateWhereForJoins(
                sub as Record<string, unknown>,
                baseTable,
                resolved,
                expose,
              )
            : sub,
        ),
      )
      continue
    }

    if (key.startsWith('$')) {
      safeSet(out, key, value)
      continue
    }

    const exposeSource = exposeColumns.get(key)
    if (exposeSource) {
      if (!isUnsafeObjectKey(exposeSource.column)) {
        mergeBlock(exposeSource.table, { [exposeSource.column]: value })
      }
      continue
    }

    // Dot-path legacy: environment_integration.environment_id
    if (key.includes('.')) {
      const [tableOrJoin, ...rest] = key.split('.')
      const col = rest.join('.')
      if (!isUnsafeObjectKey(col)) {
        const byPath = resolved.find(
          (r) => r.path === tableOrJoin || r.name === tableOrJoin,
        )
        if (byPath) {
          mergeBlock(byPath.sqlTable, { [col]: value })
          continue
        }
        if (tableOrJoin && joinedTables.has(tableOrJoin)) {
          mergeBlock(tableOrJoin, { [col]: value })
          continue
        }
      }
    }

    safeSet(out, key, value)
  }

  for (const [table, filter] of aliasBlocks) {
    const blockKey = `$${table}`
    const existing = out[blockKey]
    const merged = Object.create(null) as Record<string, unknown>
    if (typeof existing === 'object' && existing !== null) {
      for (const [k, v] of Object.entries(
        existing as Record<string, unknown>,
      )) {
        safeSet(merged, k, v)
      }
    }
    for (const [k, v] of Object.entries(filter)) {
      safeSet(merged, k, v)
    }
    safeSet(out, blockKey, merged)
  }

  return out
}

/** Resolve an orderBy column to a SQL identifier string (`table.col`). */
export function resolveOrderByColumn(
  column: string,
  baseTable: string,
  resolved: ResolvedJoin[],
  expose?: readonly ExposeDef[],
): string {
  const exposeMatch = (expose ?? []).find((e) => e.as === column)
  if (exposeMatch) {
    const {
      joinPath,
      column: col,
      isTableQualified,
    } = parseQualifiedPath(exposeMatch.from)
    if (isTableQualified && !joinPath) {
      const parts = exposeMatch.from.slice(1).split('.')
      return `${parts[0]}.${parts[1] ?? col}`
    }
    const match = resolved.find((r) => r.path === joinPath)
    const table = match?.sqlTable ?? joinPath.split('.').pop() ?? joinPath
    return `${table}.${col}`
  }

  if (column.startsWith('$')) {
    const {
      joinPath,
      column: col,
      isTableQualified,
    } = parseQualifiedPath(column)
    if (isTableQualified && !joinPath) {
      const parts = column.slice(1).split('.')
      return `${parts[0]}.${parts[1]}`
    }
    const match =
      resolved.find((r) => r.path === joinPath) ??
      resolved.find((r) => r.name === joinPath || r.sqlTable === joinPath)
    if (match) return `${match.sqlTable}.${col}`
    return `${joinPath}.${col}`
  }
  return `${baseTable}.${column}`
}

/** Flat row key used for cursor pagination values. */
export function flatColumnKeyForOrderBy(
  column: string,
  _baseTable: string,
  resolved: ResolvedJoin[],
  expose: readonly ExposeDef[] | undefined,
): string {
  if (column.startsWith('$')) {
    const { joinPath, column: col } = parseQualifiedPath(column)
    const match =
      resolved.find((r) => r.path === joinPath) ??
      resolved.find((r) => r.name === joinPath || r.sqlTable === joinPath)
    if (match) return flatAliasForJoinColumn(match, col)
    return legacyJoinColumnAlias(joinPath, col)
  }

  const exposeMatch = (expose ?? []).find((e) => e.as === column)
  if (exposeMatch) return column

  return column
}

/**
 * Nest flat join columns into the include tree shape. Only nests joins present
 * in `include`; expose columns remain at the top level.
 */
export function nestJoinedData(
  row: Record<string, unknown>,
  rootJoins: Record<string, JoinDef>,
  include: Record<string, NormalizedIncludeNode>,
  resolved: ResolvedJoin[],
): Record<string, unknown> {
  const out = { ...row }

  for (const j of resolved) {
    for (const col of j.select) {
      const flatKey = flatAliasForJoinColumn(j, col)
      delete out[flatKey]
    }
  }

  function nestLevel(
    joins: Record<string, JoinDef>,
    nodes: Record<string, NormalizedIncludeNode>,
    parentPath: string,
  ): Record<string, unknown> {
    const result: Record<string, unknown> = {}

    for (const [name, node] of Object.entries(nodes)) {
      if (!node.enabled) continue
      const joinDef = joins[name]
      if (!joinDef) continue

      const path = parentPath ? `${parentPath}.${name}` : name
      const resolvedJoin = resolved.find((r) => r.path === path)
      if (!resolvedJoin) continue

      const data: Record<string, unknown> = {}
      for (const col of resolvedJoin.select) {
        const flatKey = flatAliasForJoinColumn(resolvedJoin, col)
        if (flatKey in row) data[col] = row[flatKey]
      }

      const childKeys = Object.keys(node.children)
      if (childKeys.length > 0 && joinDef.joins) {
        const nested = nestLevel(joinDef.joins, node.children, path)
        for (const [childName, childVal] of Object.entries(nested)) {
          data[childName] = childVal
        }
      }

      if (joinDef.kind === 'one') {
        const hasData = Object.values(data).some(
          (v) => v !== null && v !== undefined,
        )
        result[name] = hasData ? data : null
      } else {
        result[name] = []
      }
    }

    return result
  }

  const nested = nestLevel(rootJoins, include, '')
  return { ...out, ...nested }
}

/** Whether include tree has any enabled joins. */
export function hasActiveIncludes(
  include: Record<string, NormalizedIncludeNode>,
): boolean {
  return Object.keys(include).length > 0
}

/** Expand include for API compat: boolean map for top-level only. */
export function topLevelIncludeFlags(
  include: Record<string, NormalizedIncludeNode>,
): Record<string, boolean> {
  const flags: Record<string, boolean> = {}
  for (const [k, v] of Object.entries(include)) {
    if (v.enabled) flags[k] = true
  }
  return flags
}

export function validateOrderByWithJoins(
  orderBy: OrderBy[],
  baseColumns: string[],
  _resolved: ResolvedJoin[],
  expose: readonly ExposeDef[] | undefined,
  _baseTable: string,
): void {
  const exposeNames = new Set((expose ?? []).map((e) => e.as))

  for (const order of orderBy) {
    if (order.column.startsWith('$')) continue
    if (exposeNames.has(order.column)) continue
    if (!baseColumns.includes('*') && !baseColumns.includes(order.column)) {
      throw new CursorError(
        `Invalid order column "${order.column}". Allowed base columns: ${baseColumns.join(', ')}`,
        { column: order.column, allowedColumns: baseColumns },
      )
    }
  }
}
