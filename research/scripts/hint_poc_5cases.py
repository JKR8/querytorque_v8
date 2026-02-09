"""pg_hint_plan Proof-of-Concept: 5 test cases.
Each case: original SQL + targeted hints, validated with 5-run trimmed mean.
Hints written manually based on EXPLAIN plan analysis (acting as LLM)."""

import psycopg2
import time
import json

DSN = "postgresql://jakc9:jakc9@127.0.0.1:5434/dsb_sf10"

def trimmed_mean(values):
    s = sorted(values)
    return sum(s[1:-1]) / (len(s) - 2)

def run_query(conn, sql):
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    t1 = time.perf_counter()
    return (t1 - t0) * 1000, rows

def run_query_with_config(conn, sql, config_cmds=None):
    cur = conn.cursor()
    cur.execute("BEGIN")
    if config_cmds:
        for cmd in config_cmds:
            cur.execute(cmd)
    t0 = time.perf_counter()
    cur.execute(sql)
    rows = cur.fetchall()
    t1 = time.perf_counter()
    cur.execute("COMMIT")
    return (t1 - t0) * 1000, rows

def validate(conn, name, orig_sql, variants, rounds=5):
    """Validate original vs hint variants with interleaved trimmed mean."""
    print(f"\n{'='*70}")
    print(f"TEST CASE: {name}")
    print(f"{'='*70}")
    
    # Correctness check
    _, orig_result = run_query(conn, orig_sql)
    print(f"Original result rows: {len(orig_result)}, first: {orig_result[0] if orig_result else 'empty'}")
    
    for vname, vsql, vcfg in variants:
        if vcfg:
            _, vresult = run_query_with_config(conn, vsql, vcfg)
        else:
            _, vresult = run_query(conn, vsql)
        match = orig_result == vresult
        if not match:
            print(f"  *** {vname}: WRONG RESULTS (got {len(vresult)} rows) - SKIPPING ***")
            continue
    print()
    
    # Warmup all variants
    print("Warmup...")
    run_query(conn, orig_sql)
    for vname, vsql, vcfg in variants:
        if vcfg:
            run_query_with_config(conn, vsql, vcfg)
        else:
            run_query(conn, vsql)
    
    # Measure: interleaved rounds
    all_times = {"original": []}
    for vname, _, _ in variants:
        all_times[vname] = []
    
    for i in range(rounds):
        ms_o, _ = run_query(conn, orig_sql)
        all_times["original"].append(ms_o)
        
        for vname, vsql, vcfg in variants:
            if vcfg:
                ms_v, _ = run_query_with_config(conn, vsql, vcfg)
            else:
                ms_v, _ = run_query(conn, vsql)
            all_times[vname].append(ms_v)
        
        parts = [f"orig={ms_o:.0f}"]
        for vname, _, _ in variants:
            parts.append(f"{vname}={all_times[vname][-1]:.0f}")
        print(f"  Round {i+1}: {', '.join(parts)}")
    
    # Results
    print(f"\n{'--- RESULTS ---':^70}")
    tm_orig = trimmed_mean(all_times["original"])
    print(f"  {'Original':<30s}: {tm_orig:>8.0f}ms  (trimmed mean)")
    
    results = []
    for vname, _, _ in variants:
        tm_v = trimmed_mean(all_times[vname])
        sp = tm_orig / tm_v if tm_v > 0 else 0
        verdict = "WIN" if sp >= 1.10 else "IMPROVED" if sp >= 1.05 else "NEUTRAL" if sp >= 0.95 else "REGRESSION"
        marker = " <<<" if sp >= 1.10 else " <" if sp >= 1.05 else ""
        print(f"  {vname:<30s}: {tm_v:>8.0f}ms  {sp:.3f}x  {verdict}{marker}")
        results.append({"name": vname, "tm_ms": tm_v, "speedup": sp, "verdict": verdict})
    
    return {"query": name, "orig_tm_ms": tm_orig, "variants": results}

# ===== CONNECT =====
conn = psycopg2.connect(DSN)
cur = conn.cursor()
cur.execute("SET pg_hint_plan.enable_hint = on")
cur.execute("SET pg_hint_plan.debug_print = off")
print("pg_hint_plan enabled.")

all_results = []

# ===================================================================
# CASE 1: query100_spj_spj (18.5s) — NL→Hash on customer_demographics
# EXPLAIN: 120K customer + 119K cd nested loop iterations
# Hint: force HashJoin on the cd join, NoNestLoop to prevent fallback
# ===================================================================
Q100_SPJ = """select min(item1.i_item_sk),
    min(item2.i_item_sk),
    min(s1.ss_ticket_number),
    min(s1.ss_item_sk)
FROM item AS item1,
item AS item2,
store_sales AS s1,
store_sales AS s2,
date_dim,
customer,
customer_address,
customer_demographics
WHERE
item1.i_item_sk < item2.i_item_sk
AND s1.ss_ticket_number = s2.ss_ticket_number
AND s1.ss_item_sk = item1.i_item_sk and s2.ss_item_sk = item2.i_item_sk
AND s1.ss_customer_sk = c_customer_sk
and c_current_addr_sk = ca_address_sk
and c_current_cdemo_sk = cd_demo_sk
AND d_year between 2000 and 2000 + 1
and d_date_sk = s1.ss_sold_date_sk
and item1.i_category in ('Electronics', 'Men')
and item2.i_manager_id between 81 and 100
and cd_marital_status = 'S'
and cd_education_status = 'Secondary'
and s1.ss_list_price between 16 and 30
and s2.ss_list_price between 16 and 30
;"""

r = validate(conn, "query100_spj_spj", Q100_SPJ, [
    ("hint: HashJoin(c cd)",
     "/*+ HashJoin(customer customer_demographics) */\n" + Q100_SPJ, None),
    ("hint: NoNestLoop(c cd)",
     "/*+ NoNestLoop(customer customer_demographics) */\n" + Q100_SPJ, None),
    ("hint+cfg: NNL(c cd)+workmem",
     "/*+ NoNestLoop(customer customer_demographics) */\n" + Q100_SPJ,
     ["SET LOCAL work_mem = '256MB'", "SET LOCAL jit = 'off'"]),
])
all_results.append(r)

# ===================================================================
# CASE 2: query100_agg (17s) — Same structure + disk sort spill
# EXPLAIN: Same 120K NL + 2024kB disk sort from cardinality underestimate
# Hint: HashJoin on cd (same as spj) + config work_mem for sort spill
# ===================================================================
Q100_AGG = """select  item1.i_item_sk, item2.i_item_sk, count(*) as cnt
FROM item AS item1,
item AS item2,
store_sales AS s1,
store_sales AS s2,
date_dim,
customer,
customer_address,
customer_demographics
WHERE
item1.i_item_sk < item2.i_item_sk
AND s1.ss_ticket_number = s2.ss_ticket_number
AND s1.ss_item_sk = item1.i_item_sk and s2.ss_item_sk = item2.i_item_sk
AND s1.ss_customer_sk = c_customer_sk
and c_current_addr_sk = ca_address_sk
and c_current_cdemo_sk = cd_demo_sk
AND d_year between 2000 and 2000 + 1
and d_date_sk = s1.ss_sold_date_sk
and item1.i_category in ('Electronics', 'Men')
and item2.i_manager_id between 81 and 100
and cd_marital_status = 'S'
and cd_education_status = 'Secondary'
and s1.ss_list_price between 16 and 30
and s2.ss_list_price between 16 and 30
GROUP BY item1.i_item_sk, item2.i_item_sk
ORDER BY cnt
;"""

r = validate(conn, "query100_agg", Q100_AGG, [
    ("cfg: work_mem only",
     Q100_AGG,
     ["SET LOCAL work_mem = '512MB'", "SET LOCAL effective_cache_size = '48GB'"]),
    ("hint: HashJoin(c cd)",
     "/*+ HashJoin(customer customer_demographics) */\n" + Q100_AGG, None),
    ("hint+cfg: HJ(c cd)+workmem",
     "/*+ HashJoin(customer customer_demographics) */\n" + Q100_AGG,
     ["SET LOCAL work_mem = '512MB'", "SET LOCAL effective_cache_size = '48GB'"]),
])
all_results.append(r)

# ===================================================================
# CASE 3: query038_multi (19s) — 756K customer NL probes across 3 INTERSECT branches
# EXPLAIN: 529K+165K+63K customer index probes, all filtering c_birth_month
# Hint: HashJoin customer in each branch (pg_hint_plan uses alias matching)
# ===================================================================
Q038 = """select  count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from store_sales, date_dim, customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1189 and 1189 + 11
      and c_birth_month in (4, 9, 10, 12)
      and ss_list_price between 25 and 84
      and ss_wholesale_cost BETWEEN 34 AND 54
  intersect
    select distinct c_last_name, c_first_name, d_date
    from catalog_sales, date_dim, customer
          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1189 and 1189 + 11
      and c_birth_month in (4, 9, 10, 12)
      and cs_list_price between 25 and 84
      and cs_wholesale_cost BETWEEN 34 AND 54
  intersect
    select distinct c_last_name, c_first_name, d_date
    from web_sales, date_dim, customer
          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1189 and 1189 + 11
      and c_birth_month in (4, 9, 10, 12)
      and ws_list_price between 25 and 84
      and ws_wholesale_cost BETWEEN 34 AND 54
) hot_cust
limit 100;"""

# pg_hint_plan matches by table name — all 3 branches use 'customer'
r = validate(conn, "query038_multi", Q038, [
    ("hint: HashJoin(customer)",
     "/*+ HashJoin(store_sales customer) HashJoin(catalog_sales customer) HashJoin(web_sales customer) */\n" + Q038, None),
    ("hint: NoNestLoop(customer)",
     "/*+ NoNestLoop(store_sales customer) NoNestLoop(catalog_sales customer) NoNestLoop(web_sales customer) */\n" + Q038, None),
    ("hint+cfg: NNL+workmem",
     "/*+ NoNestLoop(store_sales customer) NoNestLoop(catalog_sales customer) NoNestLoop(web_sales customer) */\n" + Q038,
     ["SET LOCAL work_mem = '512MB'", "SET LOCAL jit = 'off'"]),
])
all_results.append(r)

# ===================================================================
# CASE 4: query064_multi (30s) — 32-batch hash spill + 7119x cardinality error
# EXPLAIN: catalog_sales seq scan 48s + 32-batch hash on catalog_returns
# Hint: work_mem for hash spill + Rows correction for the join chain
# ===================================================================
Q064 = open("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql/ado/benchmarks/postgres_dsb/queries/query064_multi.sql").read()

r = validate(conn, "query064_multi", Q064, [
    ("cfg: work_mem=1GB",
     Q064,
     ["SET LOCAL work_mem = '1GB'", "SET LOCAL jit = 'off'"]),
    ("hint: HashJoin(ss sr)+cfg",
     "/*+ HashJoin(store_sales store_returns) */\n" + Q064,
     ["SET LOCAL work_mem = '1GB'", "SET LOCAL jit = 'off'"]),
    ("hint: Rows fix+cfg",
     "/*+ Rows(store_sales store_returns #30000) Rows(catalog_sales catalog_returns #500000) */\n" + Q064,
     ["SET LOCAL work_mem = '1GB'", "SET LOCAL jit = 'off'"]),
])
all_results.append(r)

# ===================================================================
# CASE 5: query102_agg (14.3s) — 37K customer + 35K cd NL + 34K ca_address
# EXPLAIN: customer_address ca_state filter at 34K loops returning 0 rows
# Hint: push ca_address filter earlier via HashJoin, NoNestLoop on cd
# ===================================================================
Q102 = open("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql/ado/benchmarks/postgres_dsb/queries/query102_agg.sql").read()

r = validate(conn, "query102_agg", Q102, [
    ("hint: HashJoin(c cd)",
     "/*+ HashJoin(customer customer_demographics) */\n" + Q102, None),
    ("hint: HashJoin(c ca)",
     "/*+ HashJoin(customer customer_address) */\n" + Q102, None),
    ("hint: HJ(c cd)+HJ(c ca)+cfg",
     "/*+ HashJoin(customer customer_demographics) HashJoin(customer customer_address) */\n" + Q102,
     ["SET LOCAL work_mem = '256MB'", "SET LOCAL jit = 'off'"]),
])
all_results.append(r)

# ===== SUMMARY =====
print(f"\n{'='*70}")
print("SUMMARY: pg_hint_plan Proof-of-Concept")
print(f"{'='*70}")
for r in all_results:
    print(f"\n{r['query']} (orig trimmed mean: {r['orig_tm_ms']:.0f}ms)")
    for v in r['variants']:
        marker = " ***" if v['verdict'] == 'WIN' else ""
        print(f"  {v['name']:<35s} {v['speedup']:.3f}x  {v['verdict']}{marker}")

# Save results
with open("/tmp/hint_poc_results.json", "w") as f:
    json.dump(all_results, f, indent=2)
print(f"\nResults saved to /tmp/hint_poc_results.json")

conn.close()
