---
'@truto/ginger': patch
---

Parenthesise cursor conditions so an outer `AND` cannot escape them.

`buildCursorConditions` OR-joins its conditions and returned them bare. Callers
`AND` the fragment with their own `WHERE`, and SQL binds `AND` tighter than
`OR`, so a paginated filtered list returned rows belonging to other records —
measured as 645 rows for a job that had 640, the extra five belonging to a
different job.

Reachable whenever ordering has two or more columns. `getDefaultOrderBy` orders
by every part of a composite primary key, so any such table was exposed by
default. Single-column ordering emits byte-identical SQL to before.
