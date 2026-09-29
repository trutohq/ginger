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

For a column the row schema's shape doesn't describe (a joined or exposed
column, or any column of a schema without a shape), a field that reads NULL as
NULL, and any NULL cursor value, the cursor conditions are now NULL-aware:
`col IS ?` for the equality prefix, `col IS NOT NULL` after a NULL toward
larger values, and `(col < ? OR col IS NULL)` before a value. A field that
rejects NULL, or reads it as another value (a `.transform`, a `.catch`, a
`z.coerce` without `.nullable()`), keeps byte-identical SQL: its cursors carry
that value, not NULL, so its NULL rows past a page boundary are still skipped,
in either direction, as before. Cursor tokens are unchanged.
