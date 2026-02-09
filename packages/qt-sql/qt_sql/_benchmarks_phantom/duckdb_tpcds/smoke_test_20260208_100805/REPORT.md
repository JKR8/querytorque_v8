# ADO Swarm Pipeline Smoke Test Report

**Query**: `query_42`
**Engine**: DuckDB SF10
**Date**: 2026-02-08 10:10:40
**Duration**: 155.2s
**Status**: PASS (140/140 checks)

## Step Summary

| Step | Name | Time | Checks | Status |
|-----:|------|-----:|-------:|:------:|
| 01 | Load Configuration | 0.1s | 6/6 | PASS |
| 02 | Load Query SQL | 0.0s | 3/3 | PASS |
| 03 | Parse DAG + EXPLAIN | 0.3s | 2/2 | PASS |
| 04 | FAISS Example Retrieval | 0.2s | 5/5 | PASS |
| 05 | Regression Warnings | 0.0s | 2/2 | PASS |
| 06 | Build Fan-out Prompt | 0.0s | 4/4 | PASS |
| 07 | Analyst LLM Call | 70.3s | 5/5 | PASS |
| 08 | Parse Worker Assignments | 0.0s | 11/11 | PASS |
| 09 | Build Worker Prompts | 0.1s | 21/21 | PASS |
| 10 | Worker LLM Generation | 83.1s | 11/11 | PASS |
| 11 | Syntax Validation | 0.0s | 1/1 | PASS |
| 12 | Benchmark Original (Baseline) | 0.5s | 3/3 | PASS |
| 13 | Validate Candidates | 0.4s | 16/16 | PASS |
| 14 | Learning Records | 0.0s | 17/17 | PASS |
| 15 | Session Save + Audit | 0.1s | 33/33 | PASS |

## Code Review Finding Coverage

| Finding | Description | Checks | Status |
|---------|-------------|-------:|:------:|
| F1 | Full prompt+response persistence | 28/28 | PASS |
| F2 | Unique worker IDs (no overwrites) | 7/7 | PASS |
| F4 | Structured validation diagnostics | 18/18 | PASS |
| F5 | Structured logging (logger exists) | 1/1 | PASS |
| F6 | DRY worker strategy header | 12/12 | PASS |
| F7 | API call count correctness | 1/1 | PASS |

## Best Result

- **Worker**: 2 (moderate_dimension_isolation)
- **Status**: NEUTRAL
- **Speedup**: 1.02x
- **Transforms**: materialize_cte
- **Baseline**: 27.8ms

## Step Details

### Step 01: Load Configuration

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| engine_duckdb | PASS |  | duckdb |
| db_exists | PASS |  | /mnt/d/TPC-DS/tpcds_sf10.duckdb |
| timeout_positive | PASS |  | 300 |
| validation_method_valid | PASS |  | 3-run |
| scale_factor_10 | PASS |  | 10 |
| pipeline_has_logger | PASS | F5 | Logger |

### Step 02: Load Query SQL

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| sql_non_empty | PASS |  |  |
| sql_has_select | PASS |  |  |
| sql_parses | PASS |  |  |

### Step 03: Parse DAG + EXPLAIN

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| dag_has_nodes | PASS |  | 1 |
| costs_non_empty | PASS |  |  |

### Step 04: FAISS Example Retrieval

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| faiss_returns_examples | PASS |  | 5 |
| faiss_max_12 | PASS |  |  |
| faiss_ids_unique | PASS |  | ['dimension_cte_isolate', 'or_to_union', 'multi_date_range_cte', 'multi_dimensio |
| all_examples_have_id | PASS |  |  |
| catalog_non_empty | PASS |  |  |

### Step 05: Regression Warnings

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| regressions_is_list | PASS |  |  |
| no_gold_regression_overlap | PASS |  |  |

### Step 06: Build Fan-out Prompt

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| prompt_non_empty | PASS |  |  |
| prompt_substantial | PASS |  | 5066 chars |
| prompt_contains_sql | PASS |  |  |
| prompt_mentions_workers | PASS |  |  |

### Step 07: Analyst LLM Call

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| response_non_empty | PASS |  |  |
| response_substantial | PASS |  | 1185 chars |
| response_has_worker_refs | PASS |  | should contain WORKER_N blocks |
| analyst_prompt_saved | PASS | F1 |  |
| analyst_response_saved | PASS | F1 |  |

### Step 08: Parse Worker Assignments

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| exactly_4_assignments | PASS | F2 | got 4 |
| worker_ids_unique | PASS | F2 | ids=[1, 2, 3, 4] |
| worker_ids_are_1_to_4 | PASS | F2 | ids=[1, 2, 3, 4] |
| w1_has_strategy | PASS |  | conservative_pushdown_earlyfilter |
| w1_has_hint | PASS |  | Apply early filtering to dimension tables before joining wit |
| w2_has_strategy | PASS |  | moderate_dimension_isolation |
| w2_has_hint | PASS |  | Isolate filtered date and item dimensions into separate CTEs |
| w3_has_strategy | PASS |  | aggressive_prefetch_restructure |
| w3_has_hint | PASS |  | Pre-filter both dimension tables into CTEs, then pre-join th |
| w4_has_strategy | PASS |  | novel_structural_transform |
| w4_has_hint | PASS |  | Transform query structure by splitting potential OR conditio |

### Step 09: Build Worker Prompts

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| four_prompts_built | PASS |  | got 4 |
| w1_prompt_substantial | PASS |  | 18987 chars |
| w1_contains_sql | PASS |  |  |
| w2_prompt_substantial | PASS |  | 19506 chars |
| w2_contains_sql | PASS |  |  |
| w3_prompt_substantial | PASS |  | 20125 chars |
| w3_contains_sql | PASS |  |  |
| w4_prompt_substantial | PASS |  | 21156 chars |
| w4_contains_sql | PASS |  |  |
| w1_header_has_strategy_title | PASS | F6 |  |
| w1_header_has_approach | PASS | F6 |  |
| w1_header_has_focus | PASS | F6 |  |
| w2_header_has_strategy_title | PASS | F6 |  |
| w2_header_has_approach | PASS | F6 |  |
| w2_header_has_focus | PASS | F6 |  |
| w3_header_has_strategy_title | PASS | F6 |  |
| w3_header_has_approach | PASS | F6 |  |
| w3_header_has_focus | PASS | F6 |  |
| w4_header_has_strategy_title | PASS | F6 |  |
| w4_header_has_approach | PASS | F6 |  |
| w4_header_has_focus | PASS | F6 |  |

### Step 10: Worker LLM Generation

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| four_candidates | PASS |  | got 4 |
| all_have_response | PASS |  | 4/4 |
| at_least_one_changed | PASS |  | 4/4 differ from original |
| w1_prompt_persisted | PASS | F1 |  |
| w1_response_persisted | PASS | F1 |  |
| w2_prompt_persisted | PASS | F1 |  |
| w2_response_persisted | PASS | F1 |  |
| w3_prompt_persisted | PASS | F1 |  |
| w3_response_persisted | PASS | F1 |  |
| w4_prompt_persisted | PASS | F1 |  |
| w4_response_persisted | PASS | F1 |  |

### Step 11: Syntax Validation

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| at_least_one_valid | PASS |  | 4/4 valid |

### Step 12: Benchmark Original (Baseline)

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| time_positive | PASS |  | 27.8ms |
| rows_positive | PASS |  | 11 |
| checksum_present | PASS |  | 31d0af9e5fdd7e555b7052eb7309d33c |

### Step 13: Validate Candidates

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| all_validated | PASS |  | 4/4 |
| w1_has_status | PASS |  | fail |
| w1_speedup_is_float | PASS |  | float |
| w1_errors_is_list | PASS | F4 | type=list |
| w1_error_category_set | PASS | F4 | category=semantic |
| w2_has_status | PASS |  | pass |
| w2_speedup_is_float | PASS |  | float |
| w2_errors_is_list | PASS | F4 | type=list |
| w3_has_status | PASS |  | fail |
| w3_speedup_is_float | PASS |  | float |
| w3_errors_is_list | PASS | F4 | type=list |
| w3_error_category_set | PASS | F4 | category=semantic |
| w4_has_status | PASS |  | pass |
| w4_speedup_is_float | PASS |  | float |
| w4_errors_is_list | PASS | F4 | type=list |
| at_least_one_passes | PASS |  | 2/4 pass |

### Step 14: Learning Records

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| four_records_created | PASS |  | got 4 |
| lr_w1_has_timestamp | PASS |  |  |
| lr_w1_has_query_id | PASS |  |  |
| lr_w1_transforms_is_list | PASS |  | type=list |
| lr_w1_error_msgs_is_list | PASS | F4 | type=list |
| lr_w2_has_timestamp | PASS |  |  |
| lr_w2_has_query_id | PASS |  |  |
| lr_w2_transforms_is_list | PASS |  | type=list |
| lr_w2_error_msgs_is_list | PASS | F4 | type=list |
| lr_w3_has_timestamp | PASS |  |  |
| lr_w3_has_query_id | PASS |  |  |
| lr_w3_transforms_is_list | PASS |  | type=list |
| lr_w3_error_msgs_is_list | PASS | F4 | type=list |
| lr_w4_has_timestamp | PASS |  |  |
| lr_w4_has_query_id | PASS |  |  |
| lr_w4_transforms_is_list | PASS |  | type=list |
| lr_w4_error_msgs_is_list | PASS | F4 | type=list |

### Step 15: Session Save + Audit

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| session_json_exists | PASS |  |  |
| iteration_dir_exists | PASS |  |  |
| analyst_prompt_in_session | PASS | F1 |  |
| analyst_response_in_session | PASS | F1 |  |
| worker_01_dir_exists | PASS | F2 |  |
| worker_02_dir_exists | PASS | F2 |  |
| worker_03_dir_exists | PASS | F2 |  |
| worker_04_dir_exists | PASS | F2 |  |
| worker_01_result_json_saved | PASS | F1 |  |
| worker_01_optimized_sql_saved | PASS | F1 |  |
| worker_01_prompt_txt_saved | PASS | F1 |  |
| worker_01_response_txt_saved | PASS | F1 |  |
| worker_02_result_json_saved | PASS | F1 |  |
| worker_02_optimized_sql_saved | PASS | F1 |  |
| worker_02_prompt_txt_saved | PASS | F1 |  |
| worker_02_response_txt_saved | PASS | F1 |  |
| worker_03_result_json_saved | PASS | F1 |  |
| worker_03_optimized_sql_saved | PASS | F1 |  |
| worker_03_prompt_txt_saved | PASS | F1 |  |
| worker_03_response_txt_saved | PASS | F1 |  |
| worker_04_result_json_saved | PASS | F1 |  |
| worker_04_optimized_sql_saved | PASS | F1 |  |
| worker_04_prompt_txt_saved | PASS | F1 |  |
| worker_04_response_txt_saved | PASS | F1 |  |
| worker_01_result_has_status | PASS | F4 |  |
| worker_01_result_has_speedup | PASS | F4 |  |
| worker_02_result_has_status | PASS | F4 |  |
| worker_02_result_has_speedup | PASS | F4 |  |
| worker_03_result_has_status | PASS | F4 |  |
| worker_03_result_has_speedup | PASS | F4 |  |
| worker_04_result_has_status | PASS | F4 |  |
| worker_04_result_has_speedup | PASS | F4 |  |
| api_call_count_correct | PASS | F7 | expected 5, got 5 |

## Artifact Inventory

- `01_config/` (2 files)
  - `contract.json`
  - `output.json`
- `02_query/` (3 files)
  - `contract.json`
  - `input.json`
  - `output.sql`
- `03_dag/` (3 files)
  - `contract.json`
  - `input.sql`
  - `output.json`
- `04_faiss/` (2 files)
  - `contract.json`
  - `output.json`
- `05_regressions/` (2 files)
  - `contract.json`
  - `output.json`
- `06_fan_out_prompt/` (3 files)
  - `contract.json`
  - `meta.json`
  - `output.txt`
- `07_analyst_call/` (4 files)
  - `contract.json`
  - `input_prompt.txt`
  - `meta.json`
  - `output_response.txt`
- `08_parse_assignments/` (2 files)
  - `contract.json`
  - `output.json`
- `09_worker_prompts/` (5 files)
  - `contract.json`
  - `worker_1_prompt.txt`
  - `worker_2_prompt.txt`
  - `worker_3_prompt.txt`
  - `worker_4_prompt.txt`
- `10_generate/` (18 files)
  - `candidate.json`
  - `candidate.json`
  - `candidate.json`
  - `candidate.json`
  - `contract.json`
  - `meta.json`
  - `optimized.sql`
  - `optimized.sql`
  - `optimized.sql`
  - `optimized.sql`
  - ... and 8 more
- `11_syntax/` (2 files)
  - `contract.json`
  - `output.json`
- `12_baseline/` (3 files)
  - `contract.json`
  - `input.sql`
  - `output.json`
- `13_validate/` (6 files)
  - `contract.json`
  - `summary.json`
  - `worker_1.json`
  - `worker_2.json`
  - `worker_3.json`
  - `worker_4.json`
- `14_learning/` (2 files)
  - `contract.json`
  - `records.json`
- `15_session_save/` (20 files)
  - `analyst_prompt.txt`
  - `analyst_response.txt`
  - `contract.json`
  - `optimized.sql`
  - `optimized.sql`
  - `optimized.sql`
  - `optimized.sql`
  - `prompt.txt`
  - `prompt.txt`
  - `prompt.txt`
  - ... and 10 more
