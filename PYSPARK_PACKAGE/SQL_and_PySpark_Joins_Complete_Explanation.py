# SQL and PySpark Joins — Complete Explanation Using Two Tables

# This document explains INNER, LEFT, RIGHT, FULL OUTER, LEFT SEMI, LEFT ANTI, CROSS, SELF, and NULL-safe joins using the same two one-column tables containing values, duplicates, unmatched values, and NULLs.

# Our Two Tables

# Table A — Left table

# +------+
# | id   |
# +------+
# | 1    |
# | 2    |
# | 2    |
# | 3    |
# | 3    |
# | 3    |
# | 4    |
# | 7    |
# | NULL |
# | NULL |
# +------+

# Table B — Right table

# +------+
# | id   |
# +------+
# | 2    |
# | 2    |
# | 3    |
# | 4    |
# | 4    |
# | 5    |
# | 6    |
# | NULL |
# | NULL |
# +------+

# Normal Join Condition

# SQL: A.id = B.id

# PySpark: df_a["id"] == df_b["id"]

# Important: NULL = NULL does not evaluate to TRUE in a normal SQL/PySpark equality join. Therefore NULLs do not match when using A.id = B.id.

# Value Classification

# 1. INNER JOIN

# Meaning: Return only records that have a matching value in both tables.
# 
# SQL:
# SELECT A.id, B.id
# FROM A
# INNER JOIN B
#     ON A.id = B.id;
# 
# PySpark:
# df_a.join(
#     df_b,
#     df_a["id"] == df_b["id"],
#     "inner"
# )
# 
# Matching values are 2, 3, and 4.
# 
# Duplicate multiplication:
# 2: A has two 2s and B has two 2s → 2 × 2 = 4 rows.
# 3: A has three 3s and B has one 3 → 3 × 1 = 3 rows.
# 4: A has one 4 and B has two 4s → 1 × 2 = 2 rows.
# Total = 9 rows.
# 
# Result:
# +------+------+
# | A.id | B.id |
# +------+------+
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 3    | 3    |
# | 3    | 3    |
# | 3    | 3    |
# | 4    | 4    |
# | 4    | 4    |
# +------+------+
# 
# Interview point: If A has M occurrences and B has N occurrences of the same value, that value can produce M × N joined rows.

# 2. LEFT JOIN

# Meaning: Keep every row from Table A, and match B wherever possible.
# 
# SQL:
# SELECT A.id, B.id
# FROM A
# LEFT JOIN B
#     ON A.id = B.id;
# 
# PySpark:
# df_a.join(
#     df_b,
#     df_a["id"] == df_b["id"],
#     "left"
# )
# 
# Behavior:
# 1 has no match → 1 | NULL
# 2 → 2 × 2 = 4 rows
# 3 → 3 × 1 = 3 rows
# 4 → 1 × 2 = 2 rows
# 7 has no match → 7 | NULL
# A's two NULL rows are preserved as unmatched.
# 
# Conceptual result:
# +------+------+
# | A.id | B.id |
# +------+------+
# | 1    | NULL |
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 3    | 3    |
# | 3    | 3    |
# | 3    | 3    |
# | 4    | 4    |
# | 4    | 4    |
# | 7    | NULL |
# | NULL | NULL |
# | NULL | NULL |
# +------+------+
# 
# Total = 13 rows.
# 
# Remember: LEFT JOIN protects the LEFT table. Every A row survives.

# 3. RIGHT JOIN

# Meaning: Keep every row from Table B, and match A wherever possible.
# 
# SQL:
# SELECT A.id, B.id
# FROM A
# RIGHT JOIN B
#     ON A.id = B.id;
# 
# PySpark:
# df_a.join(
#     df_b,
#     df_a["id"] == df_b["id"],
#     "right"
# )
# 
# Behavior:
# 2 → 2 × 2 = 4 rows
# 3 → 3 × 1 = 3 rows
# 4 → 1 × 2 = 2 rows
# B's 5 has no match → NULL | 5
# B's 6 has no match → NULL | 6
# B's two NULL rows are preserved independently.
# 
# Conceptual result:
# +------+------+
# | A.id | B.id |
# +------+------+
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 3    | 3    |
# | 3    | 3    |
# | 3    | 3    |
# | 4    | 4    |
# | 4    | 4    |
# | NULL | 5    |
# | NULL | 6    |
# | NULL | NULL |
# | NULL | NULL |
# +------+------+
# 
# Total = 13 rows.

# 4. FULL OUTER JOIN

# Meaning: Keep everything from both tables.
# 
# SQL:
# SELECT A.id, B.id
# FROM A
# FULL OUTER JOIN B
#     ON A.id = B.id;
# 
# PySpark:
# df_a.join(
#     df_b,
#     df_a["id"] == df_b["id"],
#     "full"
# )
# 
# Matching:
# 2 → 4 rows
# 3 → 3 rows
# 4 → 2 rows
# 
# Unmatched A: 1, 7, NULL, NULL
# Unmatched B: 5, 6, NULL, NULL
# 
# Conceptual result:
# +------+------+
# | A.id | B.id |
# +------+------+
# | 1    | NULL |
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 2    | 2    |
# | 3    | 3    |
# | 3    | 3    |
# | 3    | 3    |
# | 4    | 4    |
# | 4    | 4    |
# | 7    | NULL |
# | NULL | NULL |
# | NULL | NULL |
# | NULL | 5    |
# | NULL | 6    |
# | NULL | NULL |
# | NULL | NULL |
# +------+------+
# 
# There are four NULL/NULL-looking rows conceptually: A has two unmatched NULL rows and B has two unmatched NULL rows. They did not match each other.

# 5. LEFT SEMI JOIN

# Meaning: Return rows from A only if a matching row exists in B. It does not return B's columns.
# 
# PySpark:
# df_a.join(
#     df_b,
#     df_a["id"] == df_b["id"],
#     "left_semi"
# )
# 
# Result:
# +----+
# | id |
# +----+
# | 2  |
# | 2  |
# | 3  |
# | 3  |
# | 3  |
# | 4  |
# +----+
# 
# A's 1, 7, and NULL rows do not have a match, so they are removed.
# 
# Think: "Which rows from A have a match in B?"
# 
# SQL mental equivalent: EXISTS.

# 6. LEFT ANTI JOIN

# Meaning: Return rows from A where no matching row exists in B.
# 
# PySpark:
# df_a.join(
#     df_b,
#     df_a["id"] == df_b["id"],
#     "left_anti"
# )
# 
# Result:
# +------+
# | id   |
# +------+
# | 1    |
# | 7    |
# | NULL |
# | NULL |
# +------+
# 
# This is extremely useful for NOT EXISTS, identifying missing records, incremental data loads, reconciliation, and finding unmatched records.

# 7. CROSS JOIN

# Meaning: There is no matching condition. Every row in A combines with every row in B.
# 
# A has 10 rows and B has 9 rows, so 10 × 9 = 90 rows.
# 
# SQL:
# SELECT *
# FROM A
# CROSS JOIN B;
# 
# PySpark:
# df_a.crossJoin(df_b)
# 
# or:
# df_a.join(df_b, how="cross")
# 
# Examples include A=1 with B=2, A=1 with B=3, and so on. NULLs are also combined because there is no equality condition.
# 
# Warning: CROSS JOIN can be extremely expensive. For example, 1,000,000 × 1,000,000 rows produces 1,000,000,000,000 combinations.

# 8. SELF JOIN

# Meaning: Join a table with itself. There is no special PySpark "self" join type; it is a normal join using aliases.
# 
# Example employee table:
# +----+-------+-----------+
# | id | name  | manager_id|
# +----+-------+-----------+
# | 1  | Alice | NULL      |
# | 2  | Bob   | 1         |
# | 3  | Carol | 1         |
# +----+-------+-----------+
# 
# SQL:
# SELECT
#     e.NAME AS employee,
#     m.NAME AS manager
# FROM employee e
# LEFT JOIN employee m
#     ON e.MANAGER_ID = m.ID;
# 
# Result:
# Alice → NULL
# Bob → Alice
# Carol → Alice
# 
# PySpark:
# e = employee.alias("e")
# m = employee.alias("m")
# 
# result = e.join(
#     m,
#     e["manager_id"] == m["id"],
#     "left"
# )
# 
# Use self joins for employee-manager relationships, hierarchical data, and comparing rows within the same table.

# 9. NULL-SAFE JOIN

# Normal equality does not match NULLs:
# df_a["id"] == df_b["id"]
# 
# If the requirement is to treat NULL as equal to NULL, PySpark provides eqNullSafe().
# 
# PySpark:
# df_a.join(
#     df_b,
#     df_a["id"].eqNullSafe(df_b["id"]),
#     "inner"
# )
# 
# A has two NULLs and B has two NULLs, so NULL produces 2 × 2 = 4 matches.
# 
# With null-safe matching:
# 2 → 4 rows
# 3 → 3 rows
# 4 → 2 rows
# NULL → 4 rows
# Total = 13 rows.

# 10. SQL NULL-SAFE COMPARISON IN MYSQL

# In MySQL, use the null-safe equality operator <=> instead of = when NULL should match NULL.
# 
# SQL:
# SELECT *
# FROM A
# INNER JOIN B
#     ON A.id <=> B.id;
# 
# This matches 3 ↔ 3, 4 ↔ 4, and NULL ↔ NULL.

# Complete Join Comparison

# Easy Mental Model

# INNER: Give me employees present in both systems. A ∩ B.

# LEFT: Give me everyone in my system, whether or not they are in the other system.

# RIGHT: Give me everyone in the other system.

# FULL: Give me everyone from both systems. A ∪ B.

# LEFT SEMI: Give me employees from A who exist in B.

# LEFT ANTI: Give me employees from A who do not exist in B.

# CROSS: Give me every possible combination. A × B.

# SQL ↔ PySpark Join Mapping

# PySpark Join Syntax to Memorize

# # INNER
# df_a.join(df_b, condition, "inner")
# 
# # LEFT
# df_a.join(df_b, condition, "left")
# 
# # RIGHT
# df_a.join(df_b, condition, "right")
# 
# # FULL
# df_a.join(df_b, condition, "full")
# 
# # LEFT SEMI = EXISTS
# df_a.join(df_b, condition, "left_semi")
# 
# # LEFT ANTI = NOT EXISTS
# df_a.join(df_b, condition, "left_anti")
# 
# # CROSS
# df_a.crossJoin(df_b)

# Most Important Concepts for Interviews

# 1. Duplicates affect the number of output rows, while NULLs affect whether rows match.

# 2. If A has M occurrences and B has N occurrences of the same non-NULL value, that value can generate M × N joined rows.

# 3. Normal equality: NULL = NULL is not TRUE.

# 4. PySpark eqNullSafe() allows NULL to match NULL.

# 5. LEFT SEMI is the common PySpark pattern for 'match exists' / EXISTS.

# 6. LEFT ANTI is the common PySpark pattern for 'no match exists' / NOT EXISTS.

# 7. LEFT JOIN protects the left table; RIGHT JOIN protects the right table; FULL protects both.

# 8. CROSS JOIN has no join condition and can create a huge Cartesian product.

# 9. SELF JOIN is implemented using aliases and a normal join.

# Final Memory Cheat Sheet

# INNER
#     ↓
# Only matching rows
# 
# LEFT
#     ↓
# Everything from LEFT
# 
# RIGHT
#     ↓
# Everything from RIGHT
# 
# FULL
#     ↓
# Everything from BOTH
# 
# LEFT SEMI
#     ↓
# LEFT rows where match EXISTS
# 
# LEFT ANTI
#     ↓
# LEFT rows where match DOES NOT EXIST
# 
# CROSS
#     ↓
# Every LEFT row × every RIGHT row
# 
# NULL-safe
#     ↓
# Use eqNullSafe() in PySpark
# Use <=> in MySQL

# --- TABLE / REFERENCE ---

# Value | Table A | Table B | Status

# 1 | 1 | 0 | Only A

# 2 | 2 | 2 | Both

# 3 | 3 | 1 | Both

# 4 | 1 | 2 | Both

# 5 | 0 | 1 | Only B

# 6 | 0 | 1 | Only B

# 7 | 1 | 0 | Only A

# NULL | 2 | 2 | Neither matches with =

# --- TABLE / REFERENCE ---

# Join | Meaning | Result / Use

# INNER | Only matching rows from both | Matching 2, 3, 4; duplicates multiply

# LEFT | All left rows + matching right | Every A row survives

# RIGHT | All right rows + matching left | Every B row survives

# FULL | Everything from both | Matched + unmatched rows from both

# LEFT SEMI | Left rows where match exists | Common PySpark pattern for EXISTS

# LEFT ANTI | Left rows where match does not exist | Common PySpark pattern for NOT EXISTS

# CROSS | Every possible combination | Cartesian product

# SELF | A table joined to itself | Use aliases

# --- TABLE / REFERENCE ---

# SQL | PySpark

# INNER JOIN | "inner"

# LEFT JOIN | "left"

# LEFT OUTER JOIN | "left_outer"

# RIGHT JOIN | "right"

# RIGHT OUTER JOIN | "right_outer"

# FULL JOIN | "full"

# FULL OUTER JOIN | "full_outer"

# CROSS JOIN | .crossJoin() / 'cross'

# EXISTS | "left_semi"

# NOT EXISTS | "left_anti"

# SELF JOIN | Normal join + aliases
