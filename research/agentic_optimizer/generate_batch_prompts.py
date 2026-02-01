"""Generate gold-standard optimization prompts for TPC-DS Q1-Q23.

Includes:
- Block Map (from AST)
- Plan data (from EXPLAIN ANALYZE) 
- Algorithm + patterns
- Operations format with replace_cte
"""

import os
import sys
import json
import duckdb

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-sql'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-shared'))

from qt_sql.optimization import build_full_prompt

QUERY_DIR = "/mnt/d/TPC-DS/queries_duckdb_converted"
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "batch_prompts")


def get_plan_summary(sql: str, db_path: str) -> dict:
    """Run EXPLAIN ANALYZE and extract plan summary."""
    conn = duckdb.connect(db_path, read_only=True)
    
    try:
        # Enable profiling
        conn.execute("PRAGMA enable_profiling='json'")
        conn.execute("PRAGMA profiling_output='/tmp/plan.json'")
        
        # Run query
        conn.execute(sql).fetchall()
        conn.close()
        
        # Read profile
        with open('/tmp/plan.json') as f:
            plan = json.load(f)
        
        # Extract summary
        operators = []
        scans = []
        total_time = 0.0
        
        def walk(node, depth=0):
            nonlocal total_time
            name = node.get("operator_name", node.get("name", "")).strip()
            timing = node.get("operator_timing", 0)
            rows = node.get("operator_cardinality", 0)
            extra = node.get("extra_info", {})
            
            if name and name != "EXPLAIN_ANALYZE":
                total_time += timing
                
                # Get table name if it's a scan
                table = ""
                if isinstance(extra, dict):
                    table = extra.get("Table", "")
                elif isinstance(extra, str) and "Table:" in extra:
                    table = extra.split("Table:")[-1].strip().split()[0]
                
                operators.append({
                    "op": name,
                    "table": table,
                    "time": timing,
                    "rows_out": rows,
                })
                
                if "SCAN" in name.upper() and table:
                    has_filter = False
                    filter_expr = ""
                    if isinstance(extra, dict):
                        filters = extra.get("Filters", "")
                        if filters:
                            has_filter = True
                            filter_expr = filters[:50]
                    
                    scans.append({
                        "table": table,
                        "rows": rows,
                        "has_filter": has_filter,
                        "filter_expr": filter_expr,
                    })
            
            for child in node.get("children", []):
                walk(child, depth + 1)
        
        for child in plan.get("children", []):
            walk(child)
        
        # Calculate cost percentages
        for op in operators:
            if total_time > 0:
                op["cost_pct"] = round(op["time"] / total_time * 100, 1)
            else:
                op["cost_pct"] = 0
        
        # Sort by cost
        operators.sort(key=lambda x: x["cost_pct"], reverse=True)
        
        return {
            "top_operators": operators[:5],
            "scans": scans,
            "total_time_ms": round(total_time * 1000, 1),
        }
        
    except Exception as e:
        print(f"  Warning: Could not get plan: {e}")
        return None
    finally:
        try:
            conn.close()
        except:
            pass


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    prompts = {}
    
    for i in range(1, 100):  # All 99 TPC-DS queries
        query_file = os.path.join(QUERY_DIR, f"query_{i}.sql")
        
        if not os.path.exists(query_file):
            print(f"Q{i}: NOT FOUND")
            continue
        
        with open(query_file, 'r') as f:
            sql = f.read()
        
        print(f"Q{i}: Getting plan...", end=" ", flush=True)
        
        # Get plan summary
        plan_summary = get_plan_summary(sql, SAMPLE_DB)
        
        # Build prompt with plan data
        prompt = build_full_prompt(sql, plan_summary)
        
        # Save prompt
        prompt_file = os.path.join(OUTPUT_DIR, f"q{i}_prompt.txt")
        with open(prompt_file, 'w') as f:
            f.write(prompt)
        
        has_plan = "with plan" if plan_summary else "no plan"
        print(f"{len(prompt):,} chars ({has_plan})")
        
        prompts[f"q{i}"] = {
            "sql_file": query_file,
            "prompt_file": prompt_file,
            "prompt_length": len(prompt),
            "has_plan": plan_summary is not None,
        }
    
    # Save manifest
    manifest_file = os.path.join(OUTPUT_DIR, "manifest.json")
    with open(manifest_file, 'w') as f:
        json.dump(prompts, f, indent=2)
    
    print(f"\nGenerated {len(prompts)} gold-standard prompts")
    print(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
