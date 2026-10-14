---
'@truto/ginger': patch
---

Page past NULL values in the sort columns.

SQLite sorts NULL below every value, first on ASC and last on DESC, and
`= NULL`, `> NULL` and `< NULL` are never true. With two or more order-by
columns, a page ending on a row whose earlier sort column is NULL stopped the
listing: the `col = ?` equality prefix matched nothing, and `col > ?` against
NULL matched nothing either. On DESC, the NULL rows, which come last, were
never reached at all.

For a joined or exposed column, a table column the row schema doesn't
describe field by field (any schema but a plain `z.object`), a field that
accepts NULL, whatever it reads NULL as, and any NULL cursor value, the
cursor conditions are now NULL-aware: `col IS ?` for the equality prefix,
`col IS NOT NULL` after a NULL toward larger values, and `(col < ? OR col IS
NULL)` before a value. A cursor carries the stored values (see
`cursor-values-as-stored`), so a NULL in it is a stored NULL. A field that
rejects NULL keeps byte-identical SQL. The cursor format is unchanged.

`buildCursorConditions` takes an optional fourth argument,
`isNullable(column)`, which decides that per column. It defaults to
`() => false`, the SQL from before, so a direct caller that orders by a
nullable column should pass it.
