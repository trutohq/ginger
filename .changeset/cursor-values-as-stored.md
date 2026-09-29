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
column's flat key, for a column the response carries (a joined one when its
join is included). The parsed row is used for a value the query returned
under no flat key, or in a form a cursor can't carry (a driver's bigint; an
INTEGER column of a join under bun:sqlite's `safeIntegers` still throws). The
rows a list returns are unchanged. A cursor over a column the schema doesn't
transform is byte-identical; one over a transformed column holds the stored
value. A cursor issued before this still decodes and returns the page it
returned before; the cursors on that page hold stored values, so paging goes
on correctly from there.

Cursors are base64 JSON, neither encrypted nor signed, and a cursor's own
`orderBy` orders the page it fetches. A cursor now holds the stored value of
each column it orders by, which for a column the row schema transforms is not
the value the response shows (the whole address where a transform masks an
email, say). A column the response doesn't carry (one a shapeless row schema
drops, one of a join resolved only for `expose`) goes into no cursor: ordering
by it still fails when the cursor is built.
