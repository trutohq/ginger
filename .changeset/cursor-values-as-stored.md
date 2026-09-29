---
'@truto/ginger': patch
---

Build cursors from the order-by values as stored.

The next page compares a cursor's values with the stored column values, but
the cursor took them from the row as the row schema parsed it. A column the
schema transforms reads back in another form: a `z.coerce.date()` over text
SQLite wrote as `YYYY-MM-DD HH:MM:SS` went into the cursor as ISO text
(`YYYY-MM-DDTHH:MM:SS.sssZ`), and `T` sorts after the space. Ordered DESC by
such a column, each page served the boundary row and the rest of its day
again, so a list followed to its end never ended once a day held `limit`
rows; ordered ASC, the rest of that day was skipped. Ordering by a joined
column (`$alias.col`) threw "not found in row for cursor creation" on any
page with a next one, since the parsed row nests the joined values.

Cursor values now come from the row as the database returned it, under the
column's flat key. The parsed row is used for a value the query returned
under no flat key, or in a form a cursor can't carry (a driver's bigint). The
rows a list returns are unchanged. A cursor over a column the schema doesn't
transform is byte-identical; one over a transformed column holds the stored
value. A cursor issued before this still decodes, and pages as it did until
it runs out.

Cursors are base64 JSON, neither encrypted nor signed, so they now show the
stored value of a column a list is ordered by, even where the row schema
transforms it for the response.
