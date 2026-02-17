# qt-synth

AST-first synthetic SQL evaluation and witness generation.

## Canonical Eval Representation

This is the canonical structure for SQL eval in `qt-synth`:

```text
├── [CTE] ss_items
│   ├── SCAN store_sales
│   ├── FILTER (ss_list_price BETWEEN 242 AND 271)  [>>> SET: 250.00]
│   ├── AGG (GROUP BY item_id)                      [>>> RESULT: Rev must be > 0]
│   └── OUTPUT (item_id, ss_item_rev)               [>>> OUT: {1, 250.00}]
├── [CTE] cs_items
│   ├── SCAN catalog_sales
│   ├── FILTER (cs_list_price BETWEEN 242 AND 271)  [>>> SET: 250.00]
│   ├── AGG (GROUP BY item_id)                      [>>> RESULT: Rev must match SS +/- 10%]
│   └── OUTPUT (item_id, cs_item_rev)               [>>> OUT: {1, 250.00}]
└── [MAIN] main_query
    ├── JOIN (ss_items.item_id = cs_items.item_id)  [>>> CHECK: 1 == 1 (OK)]
    ├── FILTER (ss_rev BETWEEN 0.9 * cs_rev ...)    [>>> CHECK: 250 vs 250 (OK)]
```

## AST-Only Contract

All representation and witness decisions must be derivable from the SQL AST:

- relation and alias nodes define `SCAN` and graph edges
- boolean expressions (`WHERE`, `ON`, `HAVING`) define `FILTER` and `CHECK`
- aggregate expressions and group keys define `AGG` result constraints
- projection expressions define `OUTPUT`
- no raw SQL string heuristics as a source of truth

## Sidecar Extensions (Window + Correlation)

The base tree can lose critical semantics for window functions and correlated
subqueries. In those cases, keep the same tree and attach an AST-derived
`ATTACHMENT` sidecar that carries the missing constraints.

Window-function sidecar method:

```text
QUERY: (Window Function "Sidecar" Method)
├── [CTE] store_sales_ranked
│   ├── SCAN store_sales  [=]  Constraint: Must generate CLUSTER
│   │   │
│   │   ├── [ATTACHMENT: WINDOW DEFINITION]
│   │   │   ├── PARTITION BY: s_store_sk (The "Bucket")
│   │   │   ├── ORDER BY:     ss_sales_price DESC (The "Sort")
│   │   │   └── TARGET:       RANK() = 3
│   │   │
│   │   └── [REQUIRED CLUSTER MANIFEST]
│   │       ├── Row 1: {Store: 1, Price: 100}  (Filler)
│   │       ├── Row 2: {Store: 1, Price: 90}   (Filler)
│   │       └── Row 3: {Store: 1, Price: 80}   (The "Witness")
│   │
│   ├── AGG (None)
│   └── OUTPUT (store_id, item_id, revenue, rnk)
└── [MAIN] ...
```

Correlated subqueries follow the same rule:

- represent the outer and inner scopes in the base tree
- attach a `CORRELATION` sidecar with binding keys, predicate direction, and
  required witness pair(s)

## Row Growth Strategy

Row growth starts at one witness row and expands with deterministic AST-driven cases:

1. Golden witness partition that must satisfy all constraints and produce one row.
2. Clone partition (shifted IDs, equivalent math) that stress-tests structural invariance.
3. Boundary-fail partition that violates exactly one AST predicate by epsilon.
4. Booster rows only where needed to preserve aggregate context for the target witness.

Reference pattern:

```sql
-- 1. THE GOLDEN WITNESS (Store 1) - Must return 1 row
INSERT INTO store_sales VALUES (1, 1, 1, 40.00, 100.00);   -- Target (Pass)
INSERT INTO store_sales VALUES (1, 2, 1, 800.00, 2000.00); -- Booster

-- 2. THE CLONE (Store 2) - Stress test, Must return 1 row
INSERT INTO store VALUES (2, 'Store2', 'IA');
INSERT INTO item VALUES (3, 'CloneItem', 10.00, 5.00, 'BrandX', 80);
INSERT INTO store_sales VALUES (2, 3, 1, 40.00, 100.00);   -- Clone Target (Pass)
INSERT INTO store_sales VALUES (2, 4, 1, 800.00, 2000.00); -- Clone Booster

-- 3. THE BOUNDARY FAIL (Store 3) - Negative Test, Must return 0 rows
INSERT INTO store VALUES (3, 'Store3', 'IA');
INSERT INTO item VALUES (5, 'FailItem', 10.00, 5.00, 'BrandX', 80);
INSERT INTO store_sales VALUES (3, 5, 1, 43.00, 100.00);   -- Target (Fail: 43 > 42)
INSERT INTO store_sales VALUES (3, 6, 1, 797.00, 2000.00); -- Booster
```

## Quick Start

```bash
python3 qt-synth/validator.py your_query.sql
python3 qt-synth/validator.py your_query.sql --target-rows 1000 --min-rows 800 --max-rows 5000
python3 qt-synth/validator.py your_query.sql --target-rows 1000 --output results.json
```

## MVROWS One-Row Eval

See `qt-synth/README_MVROWS_ONE_ROW.md`.
