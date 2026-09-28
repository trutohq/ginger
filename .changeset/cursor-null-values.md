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

For a column the row schema lets hold NULL, and for any NULL cursor value, the
cursor conditions are now NULL-aware: `col IS ?` for the equality prefix,
`col IS NOT NULL` after a NULL toward larger values, and
`(col < ? OR col IS NULL)` before a value. A column that can't hold NULL keeps
byte-identical SQL, and cursor tokens are unchanged.
