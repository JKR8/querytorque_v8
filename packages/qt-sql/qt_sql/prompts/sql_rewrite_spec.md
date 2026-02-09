# SQL Query Rewrite Specification v1.0

## "Decomposed Attention Protocol" (DAP)

*A structured format for LLM-assisted rewriting of enterprise SQL (1000+ lines) that maximises accuracy by isolating attention per component.*

---

## 1. Problem Statement

When an LLM is asked to rewrite or optimise a SQL file exceeding ~1000 lines, three failure modes dominate:

1. **Attention dilution** — The model holds the full statement in context while generating a rewrite. Distant CTE definitions interfere with the generation of the current block, causing column mismatches, dropped joins, and phantom aliases.
2. **Diff fragility** — Unified diffs against SQL are brittle. A single misaligned line anchor causes the patch to fail silently or corrupt the output. Search-and-replace fares no better when the "old" text appears in multiple CASE blocks.
3. **Pipeline blindness** — Enterprise SQL is rarely one query. It is a *pipeline* of `CREATE TABLE ... AS` statements where output tables feed downstream statements. Whole-file diffs obscure cross-statement dependencies.

### Academic validation

The decomposition-for-attention principle is empirically validated. DEA-SQL (Xie et al., ACL Findings 2024) demonstrated 2–3 percentage-point accuracy gains on Spider/BIRD benchmarks by decomposing complex text-to-SQL into isolated sub-tasks, directly reducing attention diffusion. LITHE (Dharwada et al., 2025) achieved 13.2× runtime speedup over native optimisers by combining ensemble prompts with database-sensitive rewrite rules — outperforming SOTA rule-based systems that scored 4.9×. LLM-R2 (Li et al., VLDB 2024) models each query as a *query tree* of operator nodes — the same structural intuition behind Part 1 of this spec. GenRewrite (Liu & Mozafari, 2024) introduces *Natural Language Rewrite Rules* (NLR²s) as transferable hints between queries — a concept we formalise below as `rewrite_rules`.

---

## 2. Scope

This spec applies when **all** of the following are true:

- The SQL file exceeds **500 lines** or contains **3+ CTE definitions** within a single statement.
- The task is a *rewrite* (optimisation, refactor, migration, bug fix) — not a from-scratch generation.
- The user wants to **verify intent** before inspecting implementation.

For SQL under 500 lines with simple structure, standard inline code blocks or diffs remain sufficient.

---

## 3. Format: Two-Part Response

Every rewrite response consists of exactly two parts, delivered in order.

### Part 1 — The Logic Tree (intent verification)

A visual, indented tree showing *what changed and why*, without any SQL. The reader should be able to approve or reject the rewrite plan from this alone.

#### Syntax

```
PIPELINE: <file_or_task_name>
├── [STMT] <target_table_1>  <change_marker>
│   ├── [CTE] <cte_name>  <change_marker>
│   │   └── <operation_description>
│   └── [MAIN] SELECT
│       └── <operation_description>
├── [STMT] <target_table_2>  [=]
└── [STMT] <target_table_3>  <change_marker>
    └── ...
```

#### Change markers (mandatory)

| Marker | Meaning                                      |
|--------|----------------------------------------------|
| `[+]`  | New component added                          |
| `[-]`  | Component removed entirely                   |
| `[~]`  | Component modified (describe what changed)   |
| `[=]`  | Unchanged (include for completeness, no children needed) |
| `[!]`  | Structural change (e.g. CTE converted to subquery or vice versa) |

#### Node types

| Prefix    | Meaning                                |
|-----------|----------------------------------------|
| `[STMT]`  | A top-level `CREATE TABLE AS` or standalone statement |
| `[CTE]`   | A Common Table Expression              |
| `[MAIN]`  | The final SELECT of a CTE chain        |
| `[SUB]`   | An inline subquery                     |
| `[MACRO]` | A repeated pattern (see §5 Macros)     |

#### Operation vocabulary

Use concise, scannable descriptions. Prefer these standard operations:

- `FILTER (Pushdown: <predicate>)` — A WHERE clause was added or moved earlier
- `JOIN (REMOVED: <table> x<N>)` — Redundant joins eliminated
- `JOIN (ADDED: <table> ON <key>)` — New join introduced
- `JOIN (CHANGED: <type_from> → <type_to>)` — Join type modified
- `SCAN (<source_table>)` — Base table read
- `AGG (GROUP BY: <columns>)` — Aggregation changed
- `UNION (<branch_count> branches)` — Union structure
- `CASE_MAP (<input_col> → <output_col>, <N> branches)` — Large CASE-WHEN lookup (frozen, see §5)
- `WINDOW (<function> OVER <partition>)` — Window function
- `SORT (<columns>)` — ORDER BY
- `DEDUP (DISTINCT on <columns>)` — Deduplication
- `COMPUTE (<expression_summary>)` — Derived column computation (e.g. Haversine distance)

#### Example (based on the enterprise SQL)

```
PIPELINE: everyhousehold_portfolio
├── [STMT] tbl_household_segmentation  [=]
│   └── CASE_MAP (segment_type_cd → segment_type, 51 branches)  [FROZEN]
├── [STMT] tbl_tech_transition_history  [~]
│   ├── [CTE] dataset  [~]
│   │   └── FILTER (Pushdown: calendar_date >= current_date - 180d)
│   │       └── AGG (GROUP BY: customer_id, technology_type, location_id)
│   └── [MAIN] SELECT  [~]
│       └── JOIN (REPLACED: inline address_mapping subquery → [MACRO] addr_map)
├── [STMT] tbl_broadband_service_status  [~]
│   ├── [CTE] broadband_rfs  [~]
│   │   └── JOIN (REPLACED: inline address_mapping subquery → [MACRO] addr_map)
│   └── [MAIN] SELECT  [=]
├── [STMT] tbl_service_usage_profile  [~]
│   └── JOIN (REPLACED: inline address_mapping subquery → [MACRO] addr_map)
├── [STMT] tbl_usage_data  [=]
├── [STMT] tbl_connectivity_usage  [=]
├── [STMT] tbl_address_portfolio_v1  [~]
│   ├── [CTE] broadband_canvas  [~]
│   │   ├── FILTER (Pushdown: calendar_date = max(calendar_date))
│   │   ├── JOIN plan_mapping ON product_name
│   │   └── JOIN (REPLACED: inline address_mapping subquery → [MACRO] addr_map)
│   ├── [CTE] mobile_canvas  [=]
│   ├── [CTE] prepaid_canvas  [=]
│   ├── [CTE] broadband_active_status  [~]
│   │   └── JOIN (REPLACED: inline address_mapping subquery → [MACRO] addr_map)
│   ├── [CTE] service_data_s1  [=]
│   │   └── UNION (3 branches: broadband_canvas, prepaid_canvas, mobile_canvas)
│   ├── [CTE] service_tech  [=]
│   ├── [CTE] service_data  [~]
│   │   └── JOIN (REPLACED: inline address_mapping subquery → [MACRO] addr_map)
│   ├── [CTE] connectivity_household_canvas  [=]
│   └── [MAIN] SELECT  [=]
│       └── JOIN (broadband_footprint, tbl_household_segmentation)
└── [STMT] tbl_address_portfolio  [=]
    ├── [CTE] location_record  [=]
    ├── [CTE] locations_within_range  [=]
    │   └── COMPUTE (Haversine distance, threshold 80km)
    └── [MAIN] SELECT  [=]
```

---

### Part 2 — The Component Payload (machine-parsable rewrite)

A JSON object containing every component of every modified statement, plus metadata for reconstruction and verification.

#### Schema

```json
{
  "spec_version": "1.0",
  "dialect": "duckdb | postgres | tsql | bigquery | snowflake | spark",
  "source_file": "<filename>",
  "source_hash": "<sha256 of original file, first 12 chars>",
  "generated_at": "<ISO 8601 timestamp>",

  "macros": {
    "<macro_name>": {
      "description": "<what this repeated pattern does>",
      "sql": "<the SQL fragment>",
      "used_in": ["<component_id>", "..."]
    }
  },

  "rewrite_rules": [
    {
      "id": "R1",
      "type": "join_elimination | predicate_pushdown | cte_extraction | macro_dedup | materialization | subquery_decorrelation | type_cast_cleanup | union_consolidation",
      "description": "<natural language explanation>",
      "applied_to": ["<component_id>", "..."]
    }
  ],

  "statements": [
    {
      "target_table": "<table_name>",
      "change": "modified | unchanged | added | removed",
      "components": {
        "<component_id>": {
          "type": "cte | main_query | subquery | setup",
          "change": "modified | unchanged | added | removed",
          "sql": "<complete SQL for this component>",
          "interfaces": {
            "outputs": ["<col1>", "<col2>", "..."],
            "consumes": ["<upstream_component_id>", "..."],
            "referenced_by": ["<downstream_component_id>", "..."]
          }
        }
      },
      "reconstruction_order": ["<component_id>", "..."],
      "assembly_template": "WITH <cte1> AS ({cte1}), <cte2> AS ({cte2}) {main_query}"
    }
  ],

  "frozen_blocks": [
    {
      "id": "<block_id>",
      "statement": "<target_table>",
      "component": "<component_id>",
      "line_range": [170, 317],
      "description": "CASE-WHEN lookup: segment_type_cd → segment_type (51 branches)",
      "reason": "Pure value mapping. No structural or logical change."
    }
  ],

  "validation_checks": [
    {
      "check": "column_contract",
      "from": "<component_A>",
      "to": "<component_B>",
      "expected_columns": ["col1", "col2"],
      "description": "service_data_s1 UNION expects matching column count from broadband_canvas, mobile_canvas, prepaid_canvas"
    },
    {
      "check": "row_count_invariant",
      "description": "tbl_address_portfolio row count must equal tbl_address_portfolio_v1 (LEFT JOIN preserves all rows)"
    }
  ]
}
```

---

## 4. Field Reference

### `macros`

Repeated SQL fragments that appear 3+ times across the file. Extracted once, referenced by ID.

**Why this matters for this codebase**: The subquery `(SELECT id AS location_id, address_id FROM address_mapping GROUP BY id, address_id) ak` appears **6 times** across 4 statements. The segmentation column passthrough (`segment_type, segment_group, affluence, household_composition, household_income, head_of_household_age, household_lifestage, child_young_probability, child_teen_probability`) appears in every analytical statement. Extracting these as macros reduces token count by ~40% in the component payloads and eliminates copy-paste drift.

```json
"macros": {
  "addr_map": {
    "description": "Resolves location_id to address_id via address_mapping",
    "sql": "SELECT id AS location_id, address_id FROM address_mapping GROUP BY id, address_id",
    "used_in": [
      "tbl_tech_transition_history.main_query",
      "tbl_broadband_service_status.broadband_rfs",
      "tbl_service_usage_profile.main_query",
      "tbl_address_portfolio_v1.broadband_canvas",
      "tbl_address_portfolio_v1.broadband_active_status",
      "tbl_address_portfolio_v1.service_data"
    ]
  },
  "seg_columns": {
    "description": "Standard segmentation column set passed through from tbl_household_segmentation",
    "sql": "c.segment_type, c.segment_group, c.affluence, c.household_composition, c.household_income, c.head_of_household_age, c.household_lifestage, c.child_young_probability, c.child_teen_probability",
    "used_in": [
      "tbl_tech_transition_history.main_query",
      "tbl_broadband_service_status.broadband_rfs",
      "tbl_service_usage_profile.main_query",
      "tbl_connectivity_usage.main_query"
    ]
  }
}
```

### `rewrite_rules`

Natural-language descriptions of each transformation applied, linked to the components they affect. Inspired by GenRewrite's NLR² concept — these rules are *transferable*: if the same codebase has other files with identical patterns, the rules can be reused as hints.

### `statements`

One entry per `CREATE TABLE AS` (or equivalent DDL) in the pipeline. Each statement contains:

- **`components`**: A flat map of every CTE, subquery, and the main query. Keys are stable identifiers (CTE names for CTEs, `main_query` for the final SELECT). Each component includes complete SQL — no ellipsis, no abbreviation.
- **`interfaces`**: The column contract. `outputs` lists columns produced. `consumes` lists upstream component IDs. `referenced_by` lists downstream consumers. This enables the LLM (and any tooling) to validate that UNION branches match, JOIN keys exist, and no column is referenced before it's defined.
- **`reconstruction_order`**: A topologically sorted array. Components are assembled in this order. For linear CTE chains this is trivial; for fan-out DAGs (where one CTE feeds multiple others) this resolves ambiguity that a flat map cannot.
- **`assembly_template`**: A string template showing how components combine into the final executable SQL. Placeholders use `{component_id}` syntax. This removes all ambiguity about CTE nesting, semicolon placement, and DDL wrapping.

### `frozen_blocks`

Regions of the SQL that are semantically inert — they occupy many lines but carry low information density. Typically large CASE-WHEN lookup tables, long IN-lists, or repeated column enumerations.

**Purpose**: Tells the LLM "do not attend to this block; carry it forward verbatim." In the enterprise example, lines 170–317 (the 51-branch segment_type CASE mapping) consume 148 lines but require zero reasoning. Marking it frozen prevents the LLM from accidentally modifying, truncating, or hallucinating branches.

**Rule**: A frozen block is included in the component's `sql` field verbatim but is *not* discussed in the Logic Tree beyond a single `CASE_MAP` node tagged `[FROZEN]`.

### `validation_checks`

Post-rewrite assertions that can be checked statically (column contract matching) or at runtime (row count invariants, NULL distribution). These are the *acceptance criteria* for the rewrite.

---

## 5. Rules for the LLM

When generating a rewrite using this spec, the LLM must follow these rules:

### Generation rules

1. **Tree first, always.** Generate the complete Logic Tree before writing any SQL. This forces intent-level planning before implementation.
2. **One component at a time.** When generating the `sql` value for component X, treat all other components as opaque interfaces defined only by their `outputs` array. Do not "look ahead" to downstream consumers.
3. **No ellipsis in SQL.** Every `sql` value must be complete, executable SQL. Use `-- [MACRO: addr_map]` comments to reference macros inline, but the macro's SQL must be expanded in the final assembly.
4. **Frozen blocks are copy-paste.** If a component contains a frozen block, copy it character-for-character from the source. Do not reformat, reorder, or "clean up" CASE branches.
5. **Validate interfaces after generation.** After all components are written, verify that every `consumes` reference exists in `outputs` of the upstream component. Flag mismatches as errors.

### Unchanged components

For components marked `[=]` (unchanged):

- **In the Tree**: Show the component node with `[=]` marker. No children needed.
- **In the Payload**: Set `"change": "unchanged"`. The `sql` field may be omitted (the consumer should use the original source) OR included verbatim for self-contained payloads. State which convention is used in a top-level `"unchanged_policy": "omit | include"` field.

### Multi-statement pipelines

- Each `CREATE TABLE AS` is a separate entry in the `statements` array.
- Cross-statement dependencies are captured in `interfaces.consumes` (e.g., `service_data_s1` consumes columns from `broadband_canvas` within the same statement, but `tbl_address_portfolio.main_query` consumes `tbl_address_portfolio_v1` from a *different* statement).
- Use dotted notation for cross-statement references: `"tbl_address_portfolio_v1.main_query"`.

---

## 6. Reconstruction Algorithm

To reassemble executable SQL from the payload:

```
FOR each statement in statements (in array order):
  1. Expand macros: replace [MACRO: x] comments with macros[x].sql
  2. For each component_id in reconstruction_order:
     a. Retrieve components[component_id].sql
     b. If change == "unchanged" and unchanged_policy == "omit":
        retrieve from original source file
  3. Interpolate into assembly_template
  4. Wrap in DDL (DROP IF EXISTS + CREATE TABLE AS) if applicable
  5. Append semicolon
  6. Run validation_checks for this statement
```

---

## 7. When NOT to Use This Spec

- **Simple queries (<500 lines, <3 CTEs)**: Use inline code blocks.
- **Schema-only changes** (ADD COLUMN, ALTER TYPE): Use plain DDL statements.
- **New query generation from scratch**: Use standard prompting. This spec is for *rewrites* of existing SQL.
- **One-line fixes** (typo in a WHERE clause, wrong date literal): Use a targeted search-and-replace instruction.

---

## 8. Versioning

The `spec_version` field enables forward compatibility. Breaking changes increment the major version. Additive fields increment the minor version.

| Version | Change |
|---------|--------|
| 1.0     | Initial release |

---

## Appendix A: Quick Reference Card

```
┌──────────────────────────────────────────────────────────┐
│  DECOMPOSED ATTENTION PROTOCOL — QUICK REFERENCE         │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  STEP 1: Identify pipeline (list all CREATE TABLE AS)    │
│  STEP 2: For each statement, list CTEs + main query      │
│  STEP 3: Identify macros (patterns appearing 3+ times)   │
│  STEP 4: Identify frozen blocks (inert CASE maps, etc.)  │
│  STEP 5: Build Logic Tree with change markers            │
│  STEP 6: Build Component Payload, one component at a time│
│  STEP 7: Fill interfaces (outputs, consumes, ref_by)     │
│  STEP 8: Add validation checks                           │
│  STEP 9: Verify column contracts across interfaces       │
│  STEP 10: Deliver Tree + Payload                         │
│                                                          │
│  CHANGE MARKERS:  [+] add  [-] remove  [~] modify       │
│                   [=] unchanged  [!] structural change   │
│                                                          │
│  RULES:  Tree before SQL.  One component at a time.      │
│          No ellipsis.  Frozen = verbatim copy.            │
│          Validate interfaces after generation.            │
└──────────────────────────────────────────────────────────┘
```

## Appendix B: Observed Patterns in Enterprise SQL (Analysis Notes)

The following patterns were identified in the `everyhousehold_deidentified.sql` reference file (1213 lines, 9 statements, ~15 CTEs) and directly informed this spec:

| Pattern | Frequency | Lines consumed | Spec response |
|---------|-----------|----------------|---------------|
| `address_mapping` inline subquery | 6 occurrences | ~18 lines total | `macros.addr_map` |
| Segmentation column passthrough | 5 occurrences | ~50 lines total | `macros.seg_columns` |
| CASE-WHEN segment_type lookup | 1 occurrence | 148 lines | `frozen_blocks` |
| CASE-WHEN plan_parent mapping | 3 occurrences | ~90 lines total | `frozen_blocks` |
| CASE-WHEN usage bucketing | 4 occurrences (SELECT + GROUP BY) | ~120 lines | `frozen_blocks` |
| CTE fan-out (service_data_s1 consumed by service_tech + service_data) | 1 occurrence | N/A | `interfaces + reconstruction_order` |
| Cross-statement dependency (tbl_household_segmentation → 4 downstream) | 1 table, 4 consumers | N/A | `statements[].interfaces.consumes` |
| Haversine distance (duplicated in SELECT + WHERE) | 1 occurrence | 24 lines | Component isolation |

**Estimated attention savings**: Of 1213 source lines, approximately 426 lines (~35%) are frozen blocks or macro-deduplicable patterns. The LLM's active reasoning surface is reduced to ~787 lines, distributed across 9 isolated statement contexts — the largest being `tbl_address_portfolio_v1` at ~300 lines split across 8 independently-generated components averaging ~37 lines each.
