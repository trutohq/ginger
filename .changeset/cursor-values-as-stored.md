---
'@truto/ginger': patch
---

Build cursors from the order-by values as stored.

The next page compares a cursor's values with the stored column values, but
the cursor took them from the row as the row schema parsed it. A column the
schema transforms reads back in another form: a `z.coerce.date()` over text
SQLite wrote as `YYYY-MM-DD HH:MM:SS` went into the cursor as ISO text
(`YYYY-MM-DDTHH:MM:SS.sssZ`), and `T` sorts after the space. Ordered DESC by
such a column, each page started over at the first row of the boundary row's
day, so a list followed to its end never ended once a day held `limit` rows;
ordered ASC, the rest of that day was skipped. Ordering by a column of an
included join (`$alias.col`) threw "not found in row for cursor creation" on
any page with a next one, since the parsed row nests the joined values.

Cursor values now come from the row as the database returned it, under the
column's flat key, for a column the response carries: a joined one when the
query returns it for an included `one` join with no `many` join above it
(`list` returns a `many` join as `[]`). The query doesn't return a joined
column that `select` or the join's `include.select` leaves out, so ordering by
one still doesn't page (see below); select it too. The parsed row is used for a
value the query returned under no flat key, or in a form a cursor can't carry
(a driver's bigint; under bun:sqlite's `safeIntegers`, an INTEGER column of a
join, an exposed one, any under `select`, or one the row schema reads as a
`bigint` still throws). The rows a list returns are unchanged. A cursor over a
column the schema doesn't transform is byte-identical; one over a transformed
column holds the stored value.

A cursor issued before this still decodes, and the cursors on the page it
returns hold stored values, so paging goes on correctly from there. That page
isn't always the one it returned before. Where the NULL-aware conditions
(`cursor-null-values`) apply, it also returns rows the old conditions lost
around NULLs. Over a transformed column, an older cursor holds the parsed
value, which that page compares with the stored values, as before: ordered
DESC by a field that reads NULL as `'none'`, an older cursor holding `'none'`
returned rows already listed, and now returns those and the NULL rows too.

Cursors are base64 JSON, neither encrypted nor signed, and a cursor's own
`orderBy` orders the page it fetches. A cursor now holds the stored value of
each column it orders by, which for a column the row schema transforms is not
the value the response shows (the whole address where a transform masks an
email, say). Without `select`, a column the response doesn't carry (one a
shapeless row schema drops, one a row schema sets to `undefined`, a joined one
the query doesn't return for its join, one of a `many` join) goes into no
cursor, and building a cursor over it fails, with two exceptions. A key the row
schema sets to `undefined` over a stored NULL (a field that leaves NULL out of
the response) says no more than the NULL, which the cursor holds. And where the
response carries another column under a joined column's flat name
(`profile_secret` for `$profile.secret`), the cursor takes that column's value,
as it did before. This doesn't hide such a column: `select` returns stored
values unparsed, and a cursor's own `orderBy` and values still filter on it. So
don't use a row-schema transform to hide or mask data.

A field that reads NULL as another value (a `.transform`, a `.catch`, a
`z.coerce` without `.nullable()`) now gets the NULL-aware cursor conditions
again, since its cursor holds the stored NULL: its NULL rows page exactly,
where they were skipped past a page boundary. So does any field that accepts
NULL where the column is `NOT NULL` (`created_at: z.coerce.date()`), and there
a DESC page, or a walk back from an ASC list, uses `(col < ? OR col IS NULL)`,
which an index range can't serve.
