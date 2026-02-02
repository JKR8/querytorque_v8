#!/usr/bin/env python3
"""
Test V2 Prompt with Kimi K2.5 via OpenRouter

Tests the new prompt format that includes:
- Algorithm in YAML format
- Knowledge base patterns with detection/fix/speedup
- Detected opportunities from the detector
- Patch-based output format

Usage:
    .venv/bin/python research/scripts/test_v2_prompt.py q1
"""

import os
import sys
import json
import time
import re
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
import requests

# ============================================================
# Configuration
# ============================================================
API_KEY = os.getenv("OPENROUTER_API_KEY")
if not API_KEY:
    key_file = Path("openrouter.txt")
    if key_file.exists():
        API_KEY = key_file.read_text().strip()
    else:
        print("ERROR: Set OPENROUTER_API_KEY or create openrouter.txt")
        sys.exit(1)

API_BASE = "https://openrouter.ai/api/v1/chat/completions"
MODEL_NAME = "moonshotai/kimi-k2.5"
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"

# ============================================================
# Main
# ============================================================
def main():
    if len(sys.argv) < 2:
        print("Usage: test_v2_prompt.py <query_id>")
        sys.exit(1)

    query_id = sys.argv[1]
    prompt_file = Path(f"/tmp/v2_{query_id}_prompt.txt")

    # Generate V2 prompt if needed
    if not prompt_file.exists():
        print(f"Generating V2 prompt for {query_id}...")
        from research.scripts.build_prompt_v2 import build_prompt
        prompt = build_prompt(query_id, SAMPLE_DB)
        prompt_file.write_text(prompt)
    else:
        prompt = prompt_file.read_text()

    print(f"V2 prompt loaded ({len(prompt)} chars)")

    # Extract original SQL from prompt
    sql_match = re.search(r'```sql\n(.*?)```', prompt, re.DOTALL)
    if not sql_match:
        print("ERROR: No SQL found in prompt")
        sys.exit(1)

    original_sql = sql_match.group(1).strip()

    # Connect to DuckDB
    print(f"Connecting to database...")
    conn = duckdb.connect(SAMPLE_DB, read_only=True)

    # Benchmark original
    print(f"Benchmarking original SQL...")
    times = []
    for _ in range(3):
        start = time.perf_counter()
        orig_result = conn.execute(original_sql).fetchall()
        times.append(time.perf_counter() - start)
    orig_time = sum(times[1:]) / 2
    print(f"Original: {orig_time*1000:.1f}ms, {len(orig_result)} rows")

    # Call Kimi K2.5 with V2 prompt
    print(f"\nCalling {MODEL_NAME}...")
    llm_start = time.time()

    response = requests.post(
        API_BASE,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://querytorque.com",
            "X-Title": "QueryTorque V2 Test"
        },
        json={
            "model": MODEL_NAME,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1,
        }
    )

    llm_time = time.time() - llm_start

    if response.status_code != 200:
        print(f"ERROR: API returned {response.status_code}")
        print(response.text)
        sys.exit(1)

    result = response.json()
    content = result["choices"][0]["message"]["content"]

    print(f"LLM response in {llm_time:.1f}s")
    print(f"\n{'='*60}")
    print("LLM Response:")
    print("="*60)
    print(content)
    print("="*60)

    # Parse JSON from response
    json_match = re.search(r'```json\n(.*?)```', content, re.DOTALL)
    if not json_match:
        # Try without code fence
        json_match = re.search(r'\{.*"patches".*\}', content, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
        else:
            print("No JSON patches found in response")
            return
    else:
        json_str = json_match.group(1)

    try:
        patches_data = json.loads(json_str)
        patches = patches_data.get("patches", [])
        explanation = patches_data.get("explanation", "")
        warnings = patches_data.get("semantic_warnings", [])

        print(f"\nPatches: {len(patches)}")
        for p in patches:
            print(f"  - {p.get('op')}: {p.get('target', p.get('name', ''))}")

        print(f"\nExplanation: {explanation}")
        if warnings:
            print(f"Warnings: {warnings}")

    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
        print(f"Raw: {json_str[:500]}")
        return

    # Apply patches to generate optimized SQL
    # For now, use a simple approach: look for complete rewritten SQL
    # In the future, implement actual patch application

    # Check if response includes full main_query
    if "main_query" in patches_data:
        optimized_sql = patches_data["main_query"]
    else:
        # Look for replace_cte patches that reconstruct SQL
        for p in patches:
            if p.get("op") == "replace_cte" and p.get("name") == "customer_total_return":
                cte_sql = p.get("sql", "")
                # Reconstruct the full query
                # This is a simplified approach - real implementation would use AST
                optimized_sql = f"""
WITH customer_total_return AS ({cte_sql})
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > ctr1.threshold
AND s_store_sk = ctr1.ctr_store_sk
AND s_state = 'SD'
AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
LIMIT 100
"""
                break
        else:
            print("\nCould not construct optimized SQL from patches")
            print("(Patch application not fully implemented)")
            return

    print(f"\n{'='*60}")
    print("Optimized SQL:")
    print("="*60)
    print(optimized_sql)
    print("="*60)

    # Validate and benchmark
    try:
        times = []
        for _ in range(3):
            start = time.perf_counter()
            opt_result = conn.execute(optimized_sql).fetchall()
            times.append(time.perf_counter() - start)
        opt_time = sum(times[1:]) / 2

        # Check semantic equivalence
        orig_set = set(tuple(r) for r in orig_result)
        opt_set = set(tuple(r) for r in opt_result)
        correct = orig_set == opt_set

        speedup = orig_time / opt_time if opt_time > 0 else 1.0

        print(f"\nResults:")
        print(f"  Original:  {orig_time*1000:.1f}ms ({len(orig_result)} rows)")
        print(f"  Optimized: {opt_time*1000:.1f}ms ({len(opt_result)} rows)")
        print(f"  Speedup:   {speedup:.2f}x")
        print(f"  Correct:   {'✓' if correct else '✗'}")

        if not correct:
            missing = orig_set - opt_set
            extra = opt_set - orig_set
            if missing:
                print(f"  Missing {len(missing)} rows")
            if extra:
                print(f"  Extra {len(extra)} rows")

    except Exception as e:
        print(f"\nOptimized SQL failed: {e}")

    conn.close()


if __name__ == "__main__":
    main()
