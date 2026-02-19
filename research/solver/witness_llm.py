"""LLM-based witness row generator for DSB/TPC-DS queries.

Fires all queries concurrently via DeepSeek V3.2 on OpenRouter.
Every LLM response and retry is saved to disk.
Outputs summary + detailed tables at the end.

Usage:
    python3 research/solver/witness_llm.py --all
    python3 research/solver/witness_llm.py --first 10
    python3 research/solver/witness_llm.py query001_multi_i1.sql
"""

import argparse
import json
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import duckdb
import sqlglot

from query_analyzer import analyze_query

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema DDL (all 24 TPC-DS tables)
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
# Prompt templates
# ---------------------------------------------------------------------------

WITNESS_PROMPT = """Generate minimal witness data so this SQL query returns at least 2 rows.
Output ONLY valid JSON — no markdown fences, no extra text.

## Query analysis (pre-computed)
{analysis}

## SQL (for reference)
{query}

## Example (different query, shows the format)
For a query: "SELECT c_name FROM customer, store WHERE c_store_sk = s_store_sk AND s_state = 'TX' AND c_age > (SELECT avg(c_age)*1.2 FROM customer c2 WHERE c2.c_store_sk = c.c_store_sk)"

{{"reasoning": "Need 2+ customers at same store. avg age = (50+20)/2=35, 1.2*35=42. Customer with age 50>42 qualifies. Second store: avg=(60+25)/2=42.5, 1.2*42.5=51. Customer 60>51 qualifies.", "tables": {{"store": {{"columns": ["s_store_sk", "s_state"], "rows": [[1000, "TX"], [1001, "TX"]]}}, "customer": {{"columns": ["c_customer_sk", "c_name", "c_store_sk", "c_age"], "rows": [[2000, "Alice", 1000, 50], [2001, "Bob", 1000, 20], [2002, "Carol", 1001, 60], [2003, "Dan", 1001, 25]]}}}}}}

## Rules
1. Include ONLY the columns listed in COLUMNS NEEDED above.
2. Every _sk join must have matching rows on both sides.
3. For correlated aggregate comparisons (value > avg*1.2): you need 2+ rows per group.
   The "winner" row's value must exceed 1.2x the group average (which includes that row).
   SHOW THE MATH in reasoning.
4. Denominators in division must be > 0.
5. String values are case-sensitive: use exact values from FILTERS.
6. Use integer keys starting at 1000.
7. Dates as strings: "2002-06-15".
8. null for columns you don't care about.
"""

RETRY_PROMPT = """Your previous witness data did NOT make the query return any rows.
Output ONLY corrected JSON (same format).

## Query analysis
{analysis}

## Feedback
{error_info}

## Your previous data
{prev_json}

## SQL
{query}

Common mistakes:
- Missing FK rows (every _sk join needs matching rows on BOTH sides)
- Aggregate math wrong (need value > avg*1.2 but only 1 row per group = impossible)
- Division by zero (denominator columns must be > 0)
- Date or string values don't match FILTERS exactly
"""


# ---------------------------------------------------------------------------
# LLM call via OpenRouter
# ---------------------------------------------------------------------------

_CLIENT = None


def _get_client(api_key: str):
    global _CLIENT
    if _CLIENT is None:
        from openai import OpenAI
        _CLIENT = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key,
        )
    return _CLIENT


def call_llm(prompt: str, api_key: str, model: str) -> tuple[str, dict]:
    """Call OpenRouter API. Returns (response_text, metadata_dict)."""
    client = _get_client(api_key)
    t0 = time.time()
    resp = client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model=model,
        max_tokens=8192,
        temperature=0.3,
    )
    elapsed = time.time() - t0
    text = resp.choices[0].message.content
    usage = resp.usage
    meta = {
        "model_requested": model,
        "model_actual": getattr(resp, "model", model),
        "elapsed_s": round(elapsed, 1),
        "prompt_tokens": getattr(usage, "prompt_tokens", 0),
        "completion_tokens": getattr(usage, "completion_tokens", 0),
    }
    return text, meta


# ---------------------------------------------------------------------------
# JSON parsing + INSERT building
# ---------------------------------------------------------------------------

def parse_witness_json(response: str) -> dict | None:
    """Extract JSON from LLM response, handling markdown fences."""
    text = response.strip()
    if "```json" in text:
        text = text.split("```json", 1)[1]
        text = text.split("```", 1)[0]
    elif "```" in text:
        text = text.split("```", 1)[1]
        text = text.split("```", 1)[0]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{[\s\S]*\}', text)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
        return None


def json_to_inserts(data: dict) -> list[str]:
    """Convert witness JSON to INSERT statements with named columns."""
    inserts = []
    tables = data.get("tables", {})
    for table_name, table_data in tables.items():
        columns = table_data.get("columns", [])
        rows = table_data.get("rows", [])
        if not columns or not rows:
            continue
        col_list = ", ".join(columns)
        for row in rows:
            vals = []
            for v in row:
                if v is None:
                    vals.append("NULL")
                elif isinstance(v, str):
                    vals.append(f"'{v.replace(chr(39), chr(39)+chr(39))}'")
                elif isinstance(v, bool):
                    vals.append("TRUE" if v else "FALSE")
                elif isinstance(v, (int, float)):
                    vals.append(str(v))
                else:
                    vals.append(f"'{v}'")
            inserts.append(f"INSERT INTO {table_name} ({col_list}) VALUES ({', '.join(vals)});")
    return inserts


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def _build_qualify_schema() -> dict:
    """Build schema dict for sqlglot qualify: {table: {col: type}}."""
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


def qualify_query(sql: str) -> str:
    """Use sqlglot qualify to resolve ambiguous column references."""
    global _QUALIFY_SCHEMA
    if _QUALIFY_SCHEMA is None:
        _QUALIFY_SCHEMA = _build_qualify_schema()

    try:
        from sqlglot.optimizer.qualify import qualify
        tree = sqlglot.parse_one(sql, read='duckdb')
        qualified = qualify(tree, schema=_QUALIFY_SCHEMA,
                           validate_qualify_columns=False)
        return qualified.sql(dialect='duckdb')
    except Exception:
        return sql  # fail-open: return original


def clean_query(query_sql: str) -> str:
    lines = query_sql.strip().split('\n')
    clean_lines = [l for l in lines if not l.strip().startswith('--')]
    sql = '\n'.join(clean_lines).strip()
    if sql.endswith(';'):
        sql = sql[:-1]
    return sql


def _diagnose_query(con, query_sql: str) -> str:
    sql = clean_query(query_sql)
    diagnostics = []

    table_names = set()
    for m in re.finditer(r'\b(?:FROM|JOIN|,)\s+(\w+)', sql, re.IGNORECASE):
        name = m.group(1).lower()
        if name not in ('select', 'where', 'and', 'or', 'on', 'as', 'group',
                         'order', 'by', 'having', 'limit', 'with', 'union',
                         'intersect', 'except', 'case', 'when', 'then', 'else',
                         'end', 'not', 'in', 'between', 'like', 'is', 'null',
                         'exists', 'avg', 'sum', 'count', 'min', 'max'):
            table_names.add(name)

    for t in sorted(table_names):
        try:
            cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            if cnt == 0:
                diagnostics.append(f"Table {t} is EMPTY — needs rows")
            else:
                diagnostics.append(f"Table {t}: {cnt} rows")
        except Exception:
            pass

    # Try running CTEs in isolation
    cte_match = re.match(r'(?i)(WITH\s+.+?\)\s*)\s*SELECT', sql, re.DOTALL)
    if cte_match:
        cte_part = cte_match.group(1)
        cte_name_match = re.match(r'(?i)WITH\s+(\w+)\s+AS', cte_part)
        if cte_name_match:
            cte_name = cte_name_match.group(1)
            try:
                cte_sql = f"{cte_part} SELECT * FROM {cte_name} LIMIT 10"
                rows = con.execute(cte_sql).fetchall()
                cols = [d[0] for d in con.description] if con.description else []
                diagnostics.append(f"CTE '{cte_name}': {len(rows)} rows")
                if len(rows) == 0:
                    diagnostics.append(
                        f"  -> CTE returns 0 rows! Check join keys and WHERE filters.")
                else:
                    diagnostics.append(f"  columns: {cols}")
                    for row in rows[:5]:
                        diagnostics.append(f"  data: {list(row)}")
            except Exception as e:
                diagnostics.append(f"CTE '{cte_name}' error: {e}")

    return "\n".join(diagnostics) if diagnostics else ""


def verify_witness(query_sql: str, inserts: list[str]) -> dict:
    """Create temp DuckDB, insert witness rows, run query, check results."""
    con = duckdb.connect(":memory:")

    for stmt in SCHEMA_DDL.split(';\n'):
        stmt = stmt.strip()
        if stmt:
            try:
                con.execute(stmt)
            except Exception:
                pass

    inserts_ok = 0
    inserts_failed = 0
    insert_errors = []
    for stmt in inserts:
        try:
            con.execute(stmt)
            inserts_ok += 1
        except Exception as e:
            inserts_failed += 1
            short = stmt[:120] + "..." if len(stmt) > 120 else stmt
            insert_errors.append(f"{short} -> {e}")

    diagnostics = ""
    try:
        sql = clean_query(query_sql)
        sql = qualify_query(sql)
        result = con.execute(sql).fetchall()
        row_count = len(result)
        if row_count == 0:
            diagnostics = _diagnose_query(con, query_sql)
        con.close()
        return {
            "success": row_count > 0,
            "row_count": row_count,
            "error": None,
            "diagnostics": diagnostics,
            "inserts_ok": inserts_ok,
            "inserts_failed": inserts_failed,
            "insert_errors": insert_errors,
        }
    except Exception as e:
        diagnostics = _diagnose_query(con, query_sql)
        con.close()
        return {
            "success": False, "row_count": 0, "error": str(e),
            "diagnostics": diagnostics,
            "inserts_ok": inserts_ok, "inserts_failed": inserts_failed,
            "insert_errors": insert_errors,
        }


# ---------------------------------------------------------------------------
# File I/O helpers
# ---------------------------------------------------------------------------

def _save(out_dir: Path, filename: str, content: str):
    (out_dir / filename).write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Single query: generate + verify + retry (with full logging)
# ---------------------------------------------------------------------------

def generate_witness(
    query_sql: str,
    query_name: str,
    api_key: str,
    model: str,
    max_retries: int,
    out_dir: Path,
) -> dict:
    """Generate witness rows for a single query. All artifacts saved to out_dir."""
    qdir = out_dir / query_name
    qdir.mkdir(parents=True, exist_ok=True)

    # Save original SQL
    _save(qdir, "query.sql", query_sql)

    # Pre-analyze
    analysis = analyze_query(query_sql, SCHEMA_DDL)
    analysis_text = analysis.format()
    _save(qdir, "analysis.txt", analysis_text)

    # --- Attempt 1 ---
    prompt = WITNESS_PROMPT.format(analysis=analysis_text, query=query_sql)
    _save(qdir, "attempt_1_prompt.txt", prompt)

    attempt = 1
    try:
        response, meta = call_llm(prompt, api_key, model)
    except Exception as e:
        log.error("[%s] LLM call failed: %s", query_name, e)
        _save(qdir, "attempt_1_error.txt", str(e))
        return _build_result(query_name, False, 0, attempt, 0, 0, str(e), None, [])

    _save(qdir, "attempt_1_response.txt", response)
    _save(qdir, "attempt_1_meta.json", json.dumps(meta, indent=2))

    witness_data = parse_witness_json(response)
    if witness_data is None:
        inserts = []
        raw_json = None
    else:
        raw_json = witness_data
        _save(qdir, "attempt_1_witness.json", json.dumps(witness_data, indent=2))
        inserts = json_to_inserts(witness_data)
        _save(qdir, "attempt_1_inserts.sql", "\n".join(inserts))

    result = verify_witness(query_sql, inserts)
    log.info("[%s] attempt 1: rows=%d inserts=%d/%d %s",
             query_name, result["row_count"],
             result["inserts_ok"], result["inserts_ok"] + result["inserts_failed"],
             "PASS" if result["success"] else "FAIL")

    # --- Retries ---
    for retry_num in range(max_retries):
        if result["success"]:
            break

        attempt = retry_num + 2
        error_info = _build_error_feedback(result)
        prev_json_str = json.dumps(raw_json, indent=2) if raw_json else "(failed to parse)"
        retry_prompt = RETRY_PROMPT.format(
            analysis=analysis_text,
            error_info=error_info,
            query=query_sql,
            prev_json=prev_json_str,
        )
        _save(qdir, f"attempt_{attempt}_prompt.txt", retry_prompt)

        try:
            response, meta = call_llm(retry_prompt, api_key, model)
        except Exception as e:
            log.error("[%s] retry %d LLM failed: %s", query_name, attempt, e)
            _save(qdir, f"attempt_{attempt}_error.txt", str(e))
            continue

        _save(qdir, f"attempt_{attempt}_response.txt", response)
        _save(qdir, f"attempt_{attempt}_meta.json", json.dumps(meta, indent=2))

        witness_data = parse_witness_json(response)
        if witness_data is None:
            inserts = []
            raw_json = None
        else:
            raw_json = witness_data
            _save(qdir, f"attempt_{attempt}_witness.json",
                  json.dumps(witness_data, indent=2))
            inserts = json_to_inserts(witness_data)
            _save(qdir, f"attempt_{attempt}_inserts.sql", "\n".join(inserts))

        result = verify_witness(query_sql, inserts)
        log.info("[%s] attempt %d: rows=%d inserts=%d/%d %s",
                 query_name, attempt, result["row_count"],
                 result["inserts_ok"],
                 result["inserts_ok"] + result["inserts_failed"],
                 "PASS" if result["success"] else "FAIL")

    # Save final result
    final = _build_result(
        query_name, result["success"], result["row_count"], attempt,
        result["inserts_ok"], result["inserts_failed"],
        result["error"], raw_json, inserts,
    )
    _save(qdir, "result.json", json.dumps(final, indent=2))
    return final


def _build_error_feedback(result: dict) -> str:
    parts = []
    if result["error"]:
        parts.append(f"Query execution error: {result['error']}")
    if result["insert_errors"]:
        parts.append("Insert errors:\n" + "\n".join(result["insert_errors"][:10]))
    if result.get("diagnostics"):
        parts.append(f"Table/CTE diagnostics:\n{result['diagnostics']}")
    if not result["error"] and not result["insert_errors"]:
        parts.append(
            "No SQL errors, but query returned 0 rows. "
            "The data satisfied the schema but NOT the query logic. "
            "Check: join FK matches, WHERE filter values, aggregate math."
        )
    return "\n\n".join(parts)


def _build_result(query_name, success, row_count, attempts,
                  inserts_ok, inserts_failed, error, witness_json, inserts):
    return {
        "query": query_name,
        "success": success,
        "row_count": row_count,
        "attempts": attempts,
        "inserts_ok": inserts_ok,
        "inserts_failed": inserts_failed,
        "error": error,
    }


# ---------------------------------------------------------------------------
# Pretty tables
# ---------------------------------------------------------------------------

def print_summary_table(results: list[dict]):
    """Print a compact PASS/FAIL summary."""
    passes = [r for r in results if r["success"]]
    fails = [r for r in results if not r["success"]]
    total = len(results)

    print("\n" + "=" * 70)
    print(f"  SUMMARY: {len(passes)}/{total} PASS ({100*len(passes)/total:.0f}%)"
          f"   |   {len(fails)} FAIL")
    print("=" * 70)

    if fails:
        print(f"\n  FAILED queries ({len(fails)}):")
        for r in fails:
            err = (r["error"] or "0 rows")[:60]
            print(f"    {r['query']:40s}  {err}")

    print()


def print_detail_table(results: list[dict]):
    """Print full detailed table."""
    print("-" * 95)
    print(f"  {'QUERY':<40s} {'STATUS':>6s} {'ROWS':>5s} {'ATT':>4s} "
          f"{'INS_OK':>6s} {'INS_FL':>6s} {'ERROR':<20s}")
    print("-" * 95)

    for r in results:
        status = "PASS" if r["success"] else "FAIL"
        err = (r["error"] or "")[:20]
        print(f"  {r['query']:<40s} {status:>6s} {r['row_count']:>5d} "
              f"{r['attempts']:>4d} {r['inserts_ok']:>6d} "
              f"{r['inserts_failed']:>6d} {err:<20s}")

    print("-" * 95)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="LLM witness row generator (DeepSeek V3.2)")
    parser.add_argument("query", nargs="*", help="SQL file(s) or query name(s)")
    parser.add_argument("--all", action="store_true", help="Run all DSB queries")
    parser.add_argument("--first", type=int, default=0, help="Run first N queries")
    parser.add_argument("--model", default="deepseek/deepseek-v3.2",
                        help="Model (default: deepseek/deepseek-v3.2)")
    parser.add_argument("--retries", type=int, default=3, help="Max retries per query")
    parser.add_argument("--workers", type=int, default=8,
                        help="Concurrent workers (default: 8)")
    parser.add_argument("--outdir", default=None,
                        help="Output directory (default: research/solver/witness_runs/<timestamp>)")
    args = parser.parse_args()

    # Load API key
    api_key = os.environ.get("QT_OPENROUTER_API_KEY", "")
    if not api_key:
        env_file = Path(__file__).resolve().parents[2] / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("QT_OPENROUTER_API_KEY="):
                    api_key = line.split("=", 1)[1].strip()
                    break
    if not api_key:
        log.error("Set QT_OPENROUTER_API_KEY in .env or environment")
        sys.exit(1)

    # Collect queries
    queries_dir = Path(__file__).resolve().parents[2] / \
        "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries"

    if args.all:
        query_files = sorted(queries_dir.glob("*.sql"))
    elif args.first > 0:
        query_files = sorted(queries_dir.glob("*.sql"))[:args.first]
    elif args.query:
        query_files = []
        for q in args.query:
            p = Path(q)
            if not p.exists():
                p = queries_dir / q
            if not p.exists():
                log.error("File not found: %s", q)
                sys.exit(1)
            query_files.append(p)
    else:
        parser.print_help()
        sys.exit(1)

    # Output directory
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.outdir) if args.outdir else \
        Path(__file__).resolve().parent / "witness_runs" / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    total = len(query_files)
    log.info("Firing %d queries concurrently (%d workers) with %s",
             total, args.workers, args.model)
    log.info("Output: %s", out_dir)

    # Save run config
    _save(out_dir, "run_config.json", json.dumps({
        "model": args.model,
        "retries": args.retries,
        "workers": args.workers,
        "total_queries": total,
        "timestamp": ts,
    }, indent=2))

    # Fire all concurrently
    results = []
    t_start = time.time()

    def _run_one(qf):
        return generate_witness(
            query_sql=qf.read_text(),
            query_name=qf.stem,
            api_key=api_key,
            model=args.model,
            max_retries=args.retries,
            out_dir=out_dir,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(_run_one, qf): qf.stem for qf in query_files}
        done_count = 0
        for future in as_completed(futures):
            qname = futures[future]
            done_count += 1
            try:
                r = future.result()
                results.append(r)
                status = "PASS" if r["success"] else "FAIL"
                log.info("[%d/%d] %s %s (rows=%d, attempts=%d)",
                         done_count, total, status, qname,
                         r["row_count"], r["attempts"])
            except Exception as e:
                log.error("[%d/%d] EXCEPTION %s: %s", done_count, total, qname, e)
                results.append({
                    "query": qname, "success": False, "row_count": 0,
                    "attempts": 0, "inserts_ok": 0, "inserts_failed": 0,
                    "error": str(e),
                })

    elapsed = time.time() - t_start

    # Sort results by query name for clean output
    results.sort(key=lambda r: r["query"])

    # Save all results
    _save(out_dir, "results.json", json.dumps(results, indent=2))

    # Print tables
    print_summary_table(results)
    print_detail_table(results)

    passes = sum(1 for r in results if r["success"])
    print(f"\n  Total time: {elapsed:.0f}s  |  Output: {out_dir}")
    print(f"  Cost estimate: ~{total * 4 * 0.00015:.3f}$ "
          f"(~1500 tok/query, {args.retries} retries max)\n")

    # Save summary
    _save(out_dir, "summary.txt", (
        f"PASS: {passes}/{total} ({100*passes/total:.0f}%)\n"
        f"FAIL: {total - passes}/{total}\n"
        f"Time: {elapsed:.0f}s\n"
        f"Model: {args.model}\n"
    ))


if __name__ == "__main__":
    main()
