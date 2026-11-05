---
'@truto/ginger': minor
---

Check `where` keys in `list` and `count`, and answer a `ValidationError` (400) for the ones that are not columns.

`select`, `orderBy` and cursors were checked against the row schema; `where` went to the SQL builder as is. A key that was not an identifier (`-s`) threw a `TypeError` out of `sql.ident()`, a 500 that quoted the builder's message. A valid identifier that was not a column was read by SQLite as a string literal, so `{ foo: 'foo' }` matched every row, `{ foo: 'bar' }` none, and a dotted key into a missing column died in `json_extract` ("malformed JSON").

Still accepted: columns, JSON paths into a column, `expose` aliases, paths into an included join, `$alias` / `$table.column` join filters (their names are checked), and `and` / `or` arrays. Entries whose value is `undefined` are skipped, as the builder skips them, and `translateWhereForJoins` no longer throws on `and: undefined`.

Stricter than before: a column that is in the table but not in the row schema is now refused in `where`, as it already was in `select` and `orderBy`; add it to the row schema. A schema with no `.shape` only gets the identifier check. `count` refuses an `expose` alias when no join is active (it matched every row or none). A JSON path into a column that is not JSON (`status.x`) still fails in SQLite, and the `where` scopes of `get`, `update` and `delete` are not checked.
