"""Test witness DB against ground truth from previous benchmark runs.

Builds a per-query DuckDB from witness data, runs original + each patch,
compares row counts, and checks against ground truth verdicts.

Usage:
    cd QueryTorque_V8
    python3 research/solver/test_witness_db.py
"""

import json
import hashlib
import re
import sys
import time
from pathlib import Path

import duckdb
import sqlglot
from sqlglot.optimizer.qualify import qualify

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SOLVER_DIR = Path(__file__).resolve().parent
WITNESS_RUN = SOLVER_DIR / "witness_runs" / "20260219_142815"
MANUAL_WITNESS = SOLVER_DIR / "manual_witness.py"
BENCHMARK_RUN = (
    SOLVER_DIR.parents[1]
    / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/runs/run_20260216_140604"
)
QUERIES_DIR = (
    SOLVER_DIR.parents[1]
    / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries"
)

# ---------------------------------------------------------------------------
# Schema DDL (same as witness_llm.py)
# ---------------------------------------------------------------------------

SCHEMA_DDL = """
CREATE TABLE call_center (cc_call_center_sk INTEGER, cc_call_center_id VARCHAR, cc_rec_start_date DATE, cc_rec_end_date DATE, cc_closed_date_sk INTEGER, cc_open_date_sk INTEGER, cc_name VARCHAR, cc_class VARCHAR, cc_employees INTEGER, cc_sq_ft INTEGER, cc_hours VARCHAR, cc_manager VARCHAR, cc_mkt_id INTEGER, cc_mkt_class VARCHAR, cc_mkt_desc VARCHAR, cc_market_manager VARCHAR, cc_division INTEGER, cc_division_name VARCHAR, cc_company INTEGER, cc_company_name VARCHAR, cc_street_number VARCHAR, cc_street_name VARCHAR, cc_street_type VARCHAR, cc_suite_number VARCHAR, cc_city VARCHAR, cc_county VARCHAR, cc_state VARCHAR, cc_zip VARCHAR, cc_country VARCHAR, cc_gmt_offset DECIMAL(5,2), cc_tax_percentage DECIMAL(5,2));
CREATE TABLE catalog_page (cp_catalog_page_sk INTEGER, cp_catalog_page_id VARCHAR, cp_start_date_sk INTEGER, cp_end_date_sk INTEGER, cp_department VARCHAR, cp_catalog_number INTEGER, cp_catalog_page_number INTEGER, cp_description VARCHAR, cp_type VARCHAR);
CREATE TABLE catalog_returns (cr_returned_date_sk INTEGER, cr_returned_time_sk INTEGER, cr_item_sk INTEGER, cr_refunded_customer_sk INTEGER, cr_refunded_cdemo_sk INTEGER, cr_refunded_hdemo_sk INTEGER, cr_refunded_addr_sk INTEGER, cr_returning_customer_sk INTEGER, cr_returning_cdemo_sk INTEGER, cr_returning_hdemo_sk INTEGER, cr_returning_addr_sk INTEGER, cr_call_center_sk INTEGER, cr_catalog_page_sk INTEGER, cr_ship_mode_sk INTEGER, cr_warehouse_sk INTEGER, cr_reason_sk INTEGER, cr_order_number INTEGER, cr_return_quantity INTEGER, cr_return_amount DECIMAL(7,2), cr_return_tax DECIMAL(7,2), cr_return_amt_inc_tax DECIMAL(7,2), cr_fee DECIMAL(7,2), cr_return_ship_cost DECIMAL(7,2), cr_refunded_cash DECIMAL(7,2), cr_reversed_charge DECIMAL(7,2), cr_store_credit DECIMAL(7,2), cr_net_loss DECIMAL(7,2));
CREATE TABLE catalog_sales (cs_sold_date_sk INTEGER, cs_sold_time_sk INTEGER, cs_ship_date_sk INTEGER, cs_bill_customer_sk INTEGER, cs_bill_cdemo_sk INTEGER, cs_bill_hdemo_sk INTEGER, cs_bill_addr_sk INTEGER, cs_ship_customer_sk INTEGER, cs_ship_cdemo_sk INTEGER, cs_ship_hdemo_sk INTEGER, cs_ship_addr_sk INTEGER, cs_call_center_sk INTEGER, cs_catalog_page_sk INTEGER, cs_ship_mode_sk INTEGER, cs_warehouse_sk INTEGER, cs_item_sk INTEGER, cs_promo_sk INTEGER, cs_order_number INTEGER, cs_quantity INTEGER, cs_wholesale_cost DECIMAL(7,2), cs_list_price DECIMAL(7,2), cs_sales_price DECIMAL(7,2), cs_ext_discount_amt DECIMAL(7,2), cs_ext_sales_price DECIMAL(7,2), cs_ext_wholesale_cost DECIMAL(7,2), cs_ext_list_price DECIMAL(7,2), cs_ext_tax DECIMAL(7,2), cs_coupon_amt DECIMAL(7,2), cs_ext_ship_cost DECIMAL(7,2), cs_net_paid DECIMAL(7,2), cs_net_paid_inc_tax DECIMAL(7,2), cs_net_paid_inc_ship DECIMAL(7,2), cs_net_paid_inc_ship_tax DECIMAL(7,2), cs_net_profit DECIMAL(7,2));
CREATE TABLE customer (c_customer_sk INTEGER, c_customer_id VARCHAR, c_current_cdemo_sk INTEGER, c_current_hdemo_sk INTEGER, c_current_addr_sk INTEGER, c_first_shipto_date_sk INTEGER, c_first_sales_date_sk INTEGER, c_salutation VARCHAR, c_first_name VARCHAR, c_last_name VARCHAR, c_preferred_cust_flag VARCHAR, c_birth_day INTEGER, c_birth_month INTEGER, c_birth_year INTEGER, c_birth_country VARCHAR, c_login VARCHAR, c_email_address VARCHAR, c_last_review_date_sk INTEGER);
CREATE TABLE customer_address (ca_address_sk INTEGER, ca_address_id VARCHAR, ca_street_number VARCHAR, ca_street_name VARCHAR, ca_street_type VARCHAR, ca_suite_number VARCHAR, ca_city VARCHAR, ca_county VARCHAR, ca_state VARCHAR, ca_zip VARCHAR, ca_country VARCHAR, ca_gmt_offset DECIMAL(5,2), ca_location_type VARCHAR);
CREATE TABLE customer_demographics (cd_demo_sk INTEGER, cd_gender VARCHAR, cd_marital_status VARCHAR, cd_education_status VARCHAR, cd_purchase_estimate INTEGER, cd_credit_rating VARCHAR, cd_dep_count INTEGER, cd_dep_employed_count INTEGER, cd_dep_college_count INTEGER);
CREATE TABLE date_dim (d_date_sk INTEGER, d_date_id VARCHAR, d_date DATE, d_month_seq INTEGER, d_week_seq INTEGER, d_quarter_seq INTEGER, d_year INTEGER, d_dow INTEGER, d_moy INTEGER, d_dom INTEGER, d_qoy INTEGER, d_fy_year INTEGER, d_fy_quarter_seq INTEGER, d_fy_week_seq INTEGER, d_day_name VARCHAR, d_quarter_name VARCHAR, d_holiday VARCHAR, d_weekend VARCHAR, d_following_holiday VARCHAR, d_first_dom INTEGER, d_last_dom INTEGER, d_same_day_ly INTEGER, d_same_day_lq INTEGER, d_current_day VARCHAR, d_current_week VARCHAR, d_current_month VARCHAR, d_current_quarter VARCHAR, d_current_year VARCHAR);
CREATE TABLE household_demographics (hd_demo_sk INTEGER, hd_income_band_sk INTEGER, hd_buy_potential VARCHAR, hd_dep_count INTEGER, hd_vehicle_count INTEGER);
CREATE TABLE income_band (ib_income_band_sk INTEGER, ib_lower_bound INTEGER, ib_upper_bound INTEGER);
CREATE TABLE inventory (inv_date_sk INTEGER, inv_item_sk INTEGER, inv_warehouse_sk INTEGER, inv_quantity_on_hand INTEGER);
CREATE TABLE item (i_item_sk INTEGER, i_item_id VARCHAR, i_rec_start_date DATE, i_rec_end_date DATE, i_item_desc VARCHAR, i_current_price DECIMAL(7,2), i_wholesale_cost DECIMAL(7,2), i_brand_id INTEGER, i_brand VARCHAR, i_class_id INTEGER, i_class VARCHAR, i_category_id INTEGER, i_category VARCHAR, i_manufact_id INTEGER, i_manufact VARCHAR, i_size VARCHAR, i_formulation VARCHAR, i_color VARCHAR, i_units VARCHAR, i_container VARCHAR, i_manager_id INTEGER, i_product_name VARCHAR);
CREATE TABLE promotion (p_promo_sk INTEGER, p_promo_id VARCHAR, p_start_date_sk INTEGER, p_end_date_sk INTEGER, p_item_sk INTEGER, p_cost DECIMAL(15,2), p_response_target INTEGER, p_promo_name VARCHAR, p_channel_dmail VARCHAR, p_channel_email VARCHAR, p_channel_catalog VARCHAR, p_channel_tv VARCHAR, p_channel_radio VARCHAR, p_channel_press VARCHAR, p_channel_event VARCHAR, p_channel_demo VARCHAR, p_channel_details VARCHAR, p_purpose VARCHAR, p_discount_active VARCHAR);
CREATE TABLE reason (r_reason_sk INTEGER, r_reason_id VARCHAR, r_reason_desc VARCHAR);
CREATE TABLE ship_mode (sm_ship_mode_sk INTEGER, sm_ship_mode_id VARCHAR, sm_type VARCHAR, sm_code VARCHAR, sm_carrier VARCHAR, sm_contract VARCHAR);
CREATE TABLE store (s_store_sk INTEGER, s_store_id VARCHAR, s_rec_start_date DATE, s_rec_end_date DATE, s_closed_date_sk INTEGER, s_store_name VARCHAR, s_number_employees INTEGER, s_floor_space INTEGER, s_hours VARCHAR, s_manager VARCHAR, s_market_id INTEGER, s_geography_class VARCHAR, s_market_desc VARCHAR, s_market_manager VARCHAR, s_division_id INTEGER, s_division_name VARCHAR, s_company_id INTEGER, s_company_name VARCHAR, s_street_number VARCHAR, s_street_name VARCHAR, s_street_type VARCHAR, s_suite_number VARCHAR, s_city VARCHAR, s_county VARCHAR, s_state VARCHAR, s_zip VARCHAR, s_country VARCHAR, s_gmt_offset DECIMAL(5,2), s_tax_precentage DECIMAL(5,2));
CREATE TABLE store_returns (sr_returned_date_sk INTEGER, sr_return_time_sk INTEGER, sr_item_sk INTEGER, sr_customer_sk INTEGER, sr_cdemo_sk INTEGER, sr_hdemo_sk INTEGER, sr_addr_sk INTEGER, sr_store_sk INTEGER, sr_reason_sk INTEGER, sr_ticket_number INTEGER, sr_return_quantity INTEGER, sr_return_amt DECIMAL(7,2), sr_return_tax DECIMAL(7,2), sr_return_amt_inc_tax DECIMAL(7,2), sr_fee DECIMAL(7,2), sr_return_ship_cost DECIMAL(7,2), sr_refunded_cash DECIMAL(7,2), sr_reversed_charge DECIMAL(7,2), sr_store_credit DECIMAL(7,2), sr_net_loss DECIMAL(7,2));
CREATE TABLE store_sales (ss_sold_date_sk INTEGER, ss_sold_time_sk INTEGER, ss_item_sk INTEGER, ss_customer_sk INTEGER, ss_cdemo_sk INTEGER, ss_hdemo_sk INTEGER, ss_addr_sk INTEGER, ss_store_sk INTEGER, ss_promo_sk INTEGER, ss_ticket_number INTEGER, ss_quantity INTEGER, ss_wholesale_cost DECIMAL(7,2), ss_list_price DECIMAL(7,2), ss_sales_price DECIMAL(7,2), ss_ext_discount_amt DECIMAL(7,2), ss_ext_sales_price DECIMAL(7,2), ss_ext_wholesale_cost DECIMAL(7,2), ss_ext_list_price DECIMAL(7,2), ss_ext_tax DECIMAL(7,2), ss_coupon_amt DECIMAL(7,2), ss_net_paid DECIMAL(7,2), ss_net_paid_inc_tax DECIMAL(7,2), ss_net_profit DECIMAL(7,2));
CREATE TABLE time_dim (t_time_sk INTEGER, t_time_id VARCHAR, t_time INTEGER, t_hour INTEGER, t_minute INTEGER, t_second INTEGER, t_am_pm VARCHAR, t_shift VARCHAR, t_sub_shift VARCHAR, t_meal_time VARCHAR);
CREATE TABLE warehouse (w_warehouse_sk INTEGER, w_warehouse_id VARCHAR, w_warehouse_name VARCHAR, w_warehouse_sq_ft INTEGER, w_street_number VARCHAR, w_street_name VARCHAR, w_street_type VARCHAR, w_suite_number VARCHAR, w_city VARCHAR, w_county VARCHAR, w_state VARCHAR, w_zip VARCHAR, w_country VARCHAR, w_gmt_offset DECIMAL(5,2));
CREATE TABLE web_page (wp_web_page_sk INTEGER, wp_web_page_id VARCHAR, wp_rec_start_date DATE, wp_rec_end_date DATE, wp_creation_date_sk INTEGER, wp_access_date_sk INTEGER, wp_autogen_flag VARCHAR, wp_customer_sk INTEGER, wp_url VARCHAR, wp_type VARCHAR, wp_char_count INTEGER, wp_link_count INTEGER, wp_image_count INTEGER, wp_max_ad_count INTEGER);
CREATE TABLE web_returns (wr_returned_date_sk INTEGER, wr_returned_time_sk INTEGER, wr_item_sk INTEGER, wr_refunded_customer_sk INTEGER, wr_refunded_cdemo_sk INTEGER, wr_refunded_hdemo_sk INTEGER, wr_refunded_addr_sk INTEGER, wr_returning_customer_sk INTEGER, wr_returning_cdemo_sk INTEGER, wr_returning_hdemo_sk INTEGER, wr_returning_addr_sk INTEGER, wr_web_page_sk INTEGER, wr_reason_sk INTEGER, wr_order_number INTEGER, wr_return_quantity INTEGER, wr_return_amt DECIMAL(7,2), wr_return_tax DECIMAL(7,2), wr_return_amt_inc_tax DECIMAL(7,2), wr_fee DECIMAL(7,2), wr_return_ship_cost DECIMAL(7,2), wr_refunded_cash DECIMAL(7,2), wr_reversed_charge DECIMAL(7,2), wr_account_credit DECIMAL(7,2), wr_net_loss DECIMAL(7,2));
CREATE TABLE web_sales (ws_sold_date_sk INTEGER, ws_sold_time_sk INTEGER, ws_ship_date_sk INTEGER, ws_item_sk INTEGER, ws_bill_customer_sk INTEGER, ws_bill_cdemo_sk INTEGER, ws_bill_hdemo_sk INTEGER, ws_bill_addr_sk INTEGER, ws_ship_customer_sk INTEGER, ws_ship_cdemo_sk INTEGER, ws_ship_hdemo_sk INTEGER, ws_ship_addr_sk INTEGER, ws_web_page_sk INTEGER, ws_web_site_sk INTEGER, ws_ship_mode_sk INTEGER, ws_warehouse_sk INTEGER, ws_promo_sk INTEGER, ws_order_number INTEGER, ws_quantity INTEGER, ws_wholesale_cost DECIMAL(7,2), ws_list_price DECIMAL(7,2), ws_sales_price DECIMAL(7,2), ws_ext_discount_amt DECIMAL(7,2), ws_ext_sales_price DECIMAL(7,2), ws_ext_wholesale_cost DECIMAL(7,2), ws_ext_list_price DECIMAL(7,2), ws_ext_tax DECIMAL(7,2), ws_coupon_amt DECIMAL(7,2), ws_ext_ship_cost DECIMAL(7,2), ws_net_paid DECIMAL(7,2), ws_net_paid_inc_tax DECIMAL(7,2), ws_net_paid_inc_ship DECIMAL(7,2), ws_net_paid_inc_ship_tax DECIMAL(7,2), ws_net_profit DECIMAL(7,2));
CREATE TABLE web_site (web_site_sk INTEGER, web_site_id VARCHAR, web_rec_start_date DATE, web_rec_end_date DATE, web_name VARCHAR, web_open_date_sk INTEGER, web_close_date_sk INTEGER, web_class VARCHAR, web_manager VARCHAR, web_mkt_id INTEGER, web_mkt_class VARCHAR, web_mkt_desc VARCHAR, web_market_manager VARCHAR, web_company_id INTEGER, web_company_name VARCHAR, web_street_number VARCHAR, web_street_name VARCHAR, web_street_type VARCHAR, web_suite_number VARCHAR, web_city VARCHAR, web_county VARCHAR, web_state VARCHAR, web_zip VARCHAR, web_country VARCHAR, web_gmt_offset DECIMAL(5,2), web_tax_percentage DECIMAL(5,2));
""".strip()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_qualify_schema() -> dict:
    schema = {}
    for stmt in SCHEMA_DDL.strip().split(';'):
        stmt = stmt.strip()
        if not stmt.upper().startswith('CREATE TABLE'):
            continue
        m = re.match(r'CREATE TABLE (\w+)\s*\((.+)\)', stmt, re.DOTALL | re.IGNORECASE)
        if not m:
            continue
        table = m.group(1).lower()
        cols = {}
        for col_def in m.group(2).split(','):
            parts = col_def.strip().split(None, 2)
            if len(parts) >= 2:
                cols[parts[0].lower()] = parts[1].upper()
        schema[table] = cols
    return schema


_QUALIFY_SCHEMA = None


def qualify_sql(sql: str) -> str:
    global _QUALIFY_SCHEMA
    if _QUALIFY_SCHEMA is None:
        _QUALIFY_SCHEMA = _build_qualify_schema()
    try:
        tree = sqlglot.parse_one(sql, read='duckdb')
        qualified = qualify(tree, schema=_QUALIFY_SCHEMA,
                           validate_qualify_columns=False)
        return qualified.sql(dialect='duckdb')
    except Exception:
        return sql


def clean_query(sql: str) -> str:
    lines = sql.strip().split('\n')
    clean_lines = [l for l in lines if not l.strip().startswith('--')]
    sql = '\n'.join(clean_lines).strip()
    if sql.endswith(';'):
        sql = sql[:-1]
    return sql


def compute_checksum(rows: list) -> str:
    """MD5 of sorted, normalized rows."""
    normalized = []
    for row in rows:
        norm_row = []
        for v in row:
            if v is None:
                norm_row.append(None)
            elif isinstance(v, float):
                norm_row.append(round(v, 9))
            else:
                norm_row.append(v)
        normalized.append(tuple(norm_row))
    normalized.sort()
    return hashlib.md5(json.dumps(normalized, default=str).encode()).hexdigest()


# ---------------------------------------------------------------------------
# Witness DB builder — per query
# ---------------------------------------------------------------------------

def load_witness_json(query_id: str) -> dict | None:
    """Find the successful witness JSON for a query."""
    qdir = WITNESS_RUN / query_id
    if not qdir.is_dir():
        return None

    result_file = qdir / "result.json"
    if not result_file.exists():
        return None
    result = json.loads(result_file.read_text())
    if not result.get("success"):
        return None

    # Find the successful attempt's witness
    for attempt in range(1, 10):
        wf = qdir / f"attempt_{attempt}_witness.json"
        if wf.exists():
            witness = json.loads(wf.read_text())
            if isinstance(witness, dict) and "tables" in witness:
                return witness

    # Check manual witness
    manual = qdir / "manual_witness.json"
    if manual.exists():
        return json.loads(manual.read_text())

    return None


def build_witness_db(witness_json: dict) -> duckdb.DuckDBPyConnection:
    """Create in-memory DuckDB with schema + witness data."""
    con = duckdb.connect(":memory:")

    # Create schema
    for stmt in SCHEMA_DDL.split(';'):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)

    # Insert witness data
    tables = witness_json.get("tables", {})
    for tname, tdata in tables.items():
        cols = tdata["columns"]
        for row in tdata["rows"]:
            vals = []
            for v in row:
                if v is None:
                    vals.append("NULL")
                elif isinstance(v, str):
                    vals.append(f"'{v}'")
                else:
                    vals.append(str(v))
            sql = f"INSERT INTO {tname} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
            try:
                con.execute(sql)
            except Exception:
                pass  # skip bad inserts silently

    return con


def run_query_on_witness(con: duckdb.DuckDBPyConnection, sql: str) -> tuple:
    """Run a query, return (row_count, checksum, error)."""
    sql = clean_query(sql)
    sql = qualify_sql(sql)

    # Handle multi-statement queries (separated by ;)
    stmts = [s.strip() for s in sql.strip().split(';') if s.strip()]
    if not stmts:
        return (0, None, "empty query")

    # Execute last statement (the actual SELECT), earlier ones are setup
    try:
        for s in stmts[:-1]:
            con.execute(s)
        rows = con.execute(stmts[-1]).fetchall()
        cksum = compute_checksum(rows) if rows else "empty"
        return (len(rows), cksum, None)
    except Exception as e:
        return (0, None, str(e)[:200])


# ---------------------------------------------------------------------------
# Ground truth extraction
# ---------------------------------------------------------------------------

def classify_ground_truth(patch: dict) -> str:
    """Classify patch ground truth into categories.

    Returns one of:
        EQUIVALENT      - semantic_passed=true, ran on PG and produced same results
        CHECKSUM_FAIL   - ran on PG, checksums didn't match (semantic error)
        STRUCTURAL_FAIL - Tier-1 AST check caught it (not an execution-based check)
        EXEC_ERROR      - SQL execution error (missing column, syntax, etc.)
        UNKNOWN         - other
    """
    sem = patch.get("semantic_passed", False)
    error = patch.get("error") or ""
    status = patch.get("status", "")

    if sem and status in ("WIN", "NEUTRAL", "REGRESSION"):
        return "EQUIVALENT"
    if sem and status == "PENDING":
        return "EQUIVALENT"  # passed semantic but wasn't benchmarked

    if "Tier-1" in error or "Tier-2" in error:
        return "STRUCTURAL_FAIL"
    if "checksum" in error.lower():
        return "CHECKSUM_FAIL"
    if "execution error" in error.lower() or "does not exist" in error.lower():
        return "EXEC_ERROR"
    if "Equivalence" in error and "checksum" not in error.lower():
        return "CHECKSUM_FAIL"  # "Equivalence (after retry)" without explicit checksum text

    if not sem and error:
        return "EXEC_ERROR"

    return "UNKNOWN"


# ---------------------------------------------------------------------------
# Main test
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("  WITNESS DB GROUND TRUTH TEST")
    print("=" * 70)

    # 1. Collect all ground truth patches from benchmark run
    if not BENCHMARK_RUN.exists():
        print(f"ERROR: Benchmark run not found: {BENCHMARK_RUN}")
        sys.exit(1)

    all_patches = []
    query_dirs = sorted(BENCHMARK_RUN.iterdir())
    for qdir in query_dirs:
        result_file = qdir / "result.json"
        if not result_file.exists():
            continue
        result = json.loads(result_file.read_text())
        query_id = result["query_id"]
        original_sql = result.get("original_sql", "")

        for iteration in result.get("iterations", []):
            for patch in iteration.get("patches", []):
                gt = classify_ground_truth(patch)
                all_patches.append({
                    "query_id": query_id,
                    "patch_id": patch.get("patch_id", "?"),
                    "iteration": iteration["iteration"],
                    "original_sql": original_sql,
                    "patch_sql": patch.get("output_sql", ""),
                    "ground_truth": gt,
                    "gt_status": patch.get("status", "?"),
                    "gt_error": (patch.get("error") or "")[:80],
                    "gt_semantic_passed": patch.get("semantic_passed", False),
                })

    print(f"\nLoaded {len(all_patches)} patches from {len(query_dirs)} queries")

    # Count ground truth categories
    gt_counts = {}
    for p in all_patches:
        gt = p["ground_truth"]
        gt_counts[gt] = gt_counts.get(gt, 0) + 1
    print("\nGround truth distribution:")
    for gt, count in sorted(gt_counts.items()):
        print(f"  {gt:20s}: {count}")

    # 2. Filter to testable patches
    # We test EQUIVALENT and CHECKSUM_FAIL patches (both actually ran on PG)
    # STRUCTURAL_FAIL are AST-only — our witness DB wouldn't test those differently
    # EXEC_ERROR patches won't run on DuckDB either
    testable = [p for p in all_patches if p["ground_truth"] in ("EQUIVALENT", "CHECKSUM_FAIL")]
    print(f"\nTestable patches (EQUIVALENT + CHECKSUM_FAIL): {len(testable)}")

    if not testable:
        print("No testable patches found!")
        sys.exit(1)

    # 3. Run each testable patch on witness DB
    results = []
    tested_queries = set()
    skipped_no_witness = set()
    t0 = time.time()

    for i, p in enumerate(testable):
        qid = p["query_id"]

        # Load witness for this query
        witness = load_witness_json(qid)
        if witness is None:
            skipped_no_witness.add(qid)
            continue

        tested_queries.add(qid)

        # Build per-query witness DB
        con = build_witness_db(witness)

        # Run original
        orig_rows, orig_cksum, orig_err = run_query_on_witness(con, p["original_sql"])

        # Run patch
        patch_rows, patch_cksum, patch_err = run_query_on_witness(con, p["patch_sql"])

        con.close()

        # Determine witness verdict
        if orig_err:
            verdict = "ORIG_ERROR"
        elif patch_err:
            verdict = "PATCH_ERROR"
        elif orig_rows == 0:
            verdict = "ZERO_ROWS"  # witness doesn't cover this query
        elif orig_rows != patch_rows:
            verdict = "ROW_MISMATCH"
        elif orig_cksum != patch_cksum:
            verdict = "CHECKSUM_MISMATCH"
        else:
            verdict = "MATCH"

        results.append({
            **p,
            "orig_rows": orig_rows,
            "patch_rows": patch_rows,
            "orig_cksum": orig_cksum,
            "patch_cksum": patch_cksum,
            "orig_err": orig_err,
            "patch_err": patch_err,
            "witness_verdict": verdict,
        })

    elapsed = time.time() - t0

    if skipped_no_witness:
        print(f"\nSkipped {len(skipped_no_witness)} queries (no witness): "
              f"{', '.join(sorted(skipped_no_witness)[:10])}...")

    print(f"Tested {len(results)} patches across {len(tested_queries)} queries in {elapsed:.1f}s")

    # 4. Analyze results
    # True positive: ground truth = CHECKSUM_FAIL, witness caught it (ROW_MISMATCH or CHECKSUM_MISMATCH or PATCH_ERROR)
    # False negative: ground truth = CHECKSUM_FAIL, witness missed it (MATCH)
    # True negative: ground truth = EQUIVALENT, witness says MATCH
    # False positive: ground truth = EQUIVALENT, witness says mismatch

    tp = fn = tn = fp = 0
    tp_details = []
    fn_details = []
    fp_details = []

    for r in results:
        gt = r["ground_truth"]
        v = r["witness_verdict"]

        caught = v in ("ROW_MISMATCH", "CHECKSUM_MISMATCH", "PATCH_ERROR")

        if gt == "CHECKSUM_FAIL":
            if caught:
                tp += 1
                tp_details.append(r)
            else:
                fn += 1
                fn_details.append(r)
        elif gt == "EQUIVALENT":
            if caught:
                fp += 1
                fp_details.append(r)
            else:
                tn += 1

    total_semantic_fails = tp + fn
    total_equivalent = tn + fp

    print("\n" + "=" * 70)
    print("  RESULTS")
    print("=" * 70)

    print(f"\n{'Metric':<35s} {'Count':>6s}  {'Rate':>6s}")
    print("-" * 50)

    if total_semantic_fails > 0:
        detection_rate = tp / total_semantic_fails * 100
        print(f"{'True Positives (caught bad)':<35s} {tp:>6d}  {detection_rate:>5.1f}%")
        print(f"{'False Negatives (missed bad)':<35s} {fn:>6d}  {fn/total_semantic_fails*100:>5.1f}%")
    else:
        detection_rate = 0
        print(f"{'True Positives (caught bad)':<35s} {tp:>6d}  {'N/A':>6s}")
        print(f"{'False Negatives (missed bad)':<35s} {fn:>6d}  {'N/A':>6s}")

    if total_equivalent > 0:
        print(f"{'True Negatives (correct pass)':<35s} {tn:>6d}  {tn/total_equivalent*100:>5.1f}%")
        print(f"{'False Positives (wrong reject)':<35s} {fp:>6d}  {fp/total_equivalent*100:>5.1f}%")
    else:
        print(f"{'True Negatives (correct pass)':<35s} {tn:>6d}  {'N/A':>6s}")
        print(f"{'False Positives (wrong reject)':<35s} {fp:>6d}  {'N/A':>6s}")

    print("-" * 50)
    if total_semantic_fails > 0:
        print(f"{'DETECTION RATE':<35s} {tp:>6d}/{total_semantic_fails:<4d} {detection_rate:>5.1f}%")
    if total_equivalent > 0:
        precision = tp / (tp + fp) * 100 if (tp + fp) > 0 else 0
        print(f"{'PRECISION':<35s} {tp:>6d}/{tp+fp:<4d} {precision:>5.1f}%")
    if total_semantic_fails + total_equivalent > 0:
        accuracy = (tp + tn) / (tp + tn + fp + fn) * 100
        print(f"{'ACCURACY':<35s} {tp+tn:>6d}/{tp+tn+fp+fn:<4d} {accuracy:>5.1f}%")

    # 5. Detail tables
    if fn_details:
        print(f"\n{'='*70}")
        print(f"  FALSE NEGATIVES — missed semantic errors ({len(fn_details)})")
        print(f"{'='*70}")
        print(f"{'Query':<25s} {'Patch':<12s} {'Orig':>5s} {'Patch':>5s} {'Verdict':<18s} {'GT Error'}")
        print("-" * 100)
        for r in fn_details:
            print(f"{r['query_id']:<25s} {r['patch_id']:<12s} "
                  f"{r['orig_rows']:>5d} {r['patch_rows']:>5d} "
                  f"{r['witness_verdict']:<18s} {r['gt_error'][:40]}")

    if fp_details:
        print(f"\n{'='*70}")
        print(f"  FALSE POSITIVES — wrongly rejected equivalent patches ({len(fp_details)})")
        print(f"{'='*70}")
        print(f"{'Query':<25s} {'Patch':<12s} {'Orig':>5s} {'Patch':>5s} {'Verdict':<18s}")
        print("-" * 80)
        for r in fp_details:
            print(f"{r['query_id']:<25s} {r['patch_id']:<12s} "
                  f"{r['orig_rows']:>5d} {r['patch_rows']:>5d} "
                  f"{r['witness_verdict']:<18s}")

    if tp_details:
        print(f"\n{'='*70}")
        print(f"  TRUE POSITIVES — correctly caught ({len(tp_details)})")
        print(f"{'='*70}")
        print(f"{'Query':<25s} {'Patch':<12s} {'Orig':>5s} {'Patch':>5s} {'Verdict':<18s}")
        print("-" * 80)
        for r in tp_details:
            print(f"{r['query_id']:<25s} {r['patch_id']:<12s} "
                  f"{r['orig_rows']:>5d} {r['patch_rows']:>5d} "
                  f"{r['witness_verdict']:<18s}")

    # 6. Per-query breakdown for all witness verdicts
    print(f"\n{'='*70}")
    print(f"  WITNESS VERDICT DISTRIBUTION")
    print(f"{'='*70}")
    verdict_counts = {}
    for r in results:
        v = r["witness_verdict"]
        verdict_counts[v] = verdict_counts.get(v, 0) + 1
    for v, c in sorted(verdict_counts.items(), key=lambda x: -x[1]):
        print(f"  {v:<25s}: {c}")

    # 7. Save results
    out_file = SOLVER_DIR / "witness_ground_truth_results.json"
    out_file.write_text(json.dumps(results, indent=2, default=str))
    print(f"\nResults saved to {out_file}")


if __name__ == "__main__":
    main()
