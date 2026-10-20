---
'@truto/ginger': patch
---

Answer a base column that shares a nested join's name as the row schema reads it.

With that nested join included, `list` and `get` (and so `create` and
`update`, which answer through `get`) looked the nested join's name up in the
top-level row, found the base column of that name, and answered its stored
value in place of the one the row schema gives: a transform's output, a masked
value say. A nested join's data sits inside its parent's, so only the included
top-level joins are read from the top-level row now.
