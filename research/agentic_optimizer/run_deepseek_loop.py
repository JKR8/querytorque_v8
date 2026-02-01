"""Run DeepSeek through the agentic optimization loop on Q23."""

import os
import sys
import json
import time

# Add package paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-sql'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-shared'))

from qt_sql.optimization import (
    build_full_prompt,
    apply_operations,
    parse_response,
    test_optimization,
    format_test_feedback,
)
from qt_shared.llm.deepseek import DeepSeekClient


# Q23 original query
ORIGINAL_SQL = """
with frequent_ss_items as
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year in (2000,2000+1,2000+2,2000+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  select sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim
       where d_year = 2000
         and d_moy = 5
         and cs_sold_date_sk = d_date_sk
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales
           ,date_dim
       where d_year = 2000
         and d_moy = 5
         and ws_sold_date_sk = d_date_sk
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))
 LIMIT 100;
"""

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
FULL_DB = "/mnt/d/TPC-DS/tpcds_sf100.duckdb"


def run_optimization_loop(
    sql: str,
    llm_client,
    max_iterations: int = 5,
    target_speedup: float = 2.0,
):
    """Run iterative optimization loop with DeepSeek."""
    
    current_sql = sql
    best_sql = sql
    best_speedup = 1.0
    history = []
    
    # Build initial prompt
    prompt = build_full_prompt(sql)
    
    print("=" * 70)
    print("AGENTIC OPTIMIZATION LOOP")
    print(f"Target speedup: {target_speedup}x")
    print(f"Max iterations: {max_iterations}")
    print("=" * 70)
    
    for iteration in range(1, max_iterations + 1):
        print(f"\n{'='*70}")
        print(f"ITERATION {iteration}")
        print("=" * 70)
        
        # Call LLM
        print("\n📤 Sending to DeepSeek...")
        start = time.time()
        response = llm_client.analyze(prompt)
        elapsed = time.time() - start
        print(f"📥 Response received ({elapsed:.1f}s, {len(response)} chars)")

        # Debug: show first 500 chars of response
        if response:
            print(f"📝 Response preview: {response[:500]}...")
        else:
            print("⚠️  Empty response from LLM")
        
        # Parse operations
        parsed = parse_response(response)
        operations = parsed.get("operations", [])
        explanation = parsed.get("explanation", "")
        
        print(f"\n📋 Operations: {len(operations)}")
        for op in operations:
            op_type = op.get("op")
            if op_type in ("replace_cte", "add_cte", "delete_cte"):
                print(f"   - {op_type}: {op.get('name')}")
            elif op_type == "replace_clause":
                print(f"   - {op_type}: {op.get('target')}")
            elif op_type == "patch":
                print(f"   - {op_type}: {op.get('target')} ({len(op.get('patches', []))} patches)")
        
        if not operations:
            print("❌ No operations returned")
            history.append({
                "iteration": iteration,
                "error": "No operations returned",
                "raw_response": response[:1000] if response else "empty",
            })
            # Add feedback and continue instead of breaking
            prompt += f"\n\n---\n\n## Iteration {iteration} Error\nNo valid operations JSON returned. Please return a JSON object with 'operations' array."
            continue
        
        # Apply operations
        try:
            optimized_sql = apply_operations(current_sql, operations)
            print(f"✅ Operations applied ({len(optimized_sql)} chars)")
        except Exception as e:
            print(f"❌ Failed to apply operations: {e}")
            history.append({
                "iteration": iteration,
                "error": str(e),
            })
            prompt += f"\n\n---\n\n## Iteration {iteration} Error\nFailed to apply: {e}\nFix and try again."
            continue
        
        # Test on sample DB
        print("\n🧪 Testing on sample DB...")
        result = test_optimization(sql, optimized_sql, SAMPLE_DB)
        
        print(f"\n📊 Results:")
        print(f"   Original:  {result.original_time:.3f}s")
        print(f"   Optimized: {result.optimized_time:.3f}s")
        print(f"   Speedup:   {result.speedup:.2f}x")
        print(f"   Semantics: {'✅ CORRECT' if result.semantically_correct else '❌ INCORRECT'}")
        
        if result.error:
            print(f"   Error: {result.error}")
        
        history.append({
            "iteration": iteration,
            "operations": operations,
            "explanation": explanation,
            "speedup": result.speedup,
            "correct": result.semantically_correct,
            "error": result.error,
        })
        
        # Update best if correct and faster
        if result.semantically_correct and result.speedup > best_speedup:
            best_sql = optimized_sql
            best_speedup = result.speedup
            current_sql = optimized_sql
            print(f"\n🏆 New best: {best_speedup:.2f}x")
        
        # Check if we've reached target
        if result.speedup >= target_speedup and result.semantically_correct:
            print(f"\n✅ TARGET REACHED: {result.speedup:.2f}x >= {target_speedup}x")
            break
        
        # Generate feedback for next iteration
        feedback = format_test_feedback(result, iteration)
        prompt += f"\n\n---\n\n{feedback}"
        
        # Add hint about what to try next
        if result.semantically_correct and result.speedup < 1.5:
            prompt += "\n\nLook for join elimination opportunities - tables joined only for FK validation."
    
    return {
        "original_sql": sql,
        "best_sql": best_sql,
        "best_speedup": best_speedup,
        "history": history,
    }


def main():
    # Check for API key
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        print("ERROR: DEEPSEEK_API_KEY environment variable not set")
        print("Run: export DEEPSEEK_API_KEY=your_key_here")
        sys.exit(1)
    
    # Initialize client - use deepseek-reasoner for best results
    model = os.environ.get("DEEPSEEK_MODEL", "deepseek-reasoner")
    print(f"Using model: {model}")
    client = DeepSeekClient(api_key=api_key, model=model)

    # Run loop with up to 20 iterations
    result = run_optimization_loop(
        sql=ORIGINAL_SQL,
        llm_client=client,
        max_iterations=20,
        target_speedup=2.0,
    )
    
    # Summary
    print("\n" + "=" * 70)
    print("FINAL RESULTS")
    print("=" * 70)
    print(f"Best speedup: {result['best_speedup']:.2f}x")
    print(f"Iterations: {len(result['history'])}")
    
    for h in result['history']:
        status = "✅" if h.get('correct') else "❌"
        speedup = h.get('speedup', 0)
        print(f"  Iter {h['iteration']}: {status} {speedup:.2f}x")
    
    if result['best_speedup'] >= 2.0:
        print("\n🎯 Testing best result on FULL SF100 database...")
        full_result = test_optimization(
            ORIGINAL_SQL,
            result['best_sql'],
            FULL_DB,
        )
        print(f"   Full DB speedup: {full_result.speedup:.2f}x")
        print(f"   Semantics: {'✅ CORRECT' if full_result.semantically_correct else '❌ INCORRECT'}")
    
    # Save results
    output_file = os.path.join(os.path.dirname(__file__), "deepseek_loop_result.json")
    with open(output_file, "w") as f:
        json.dump({
            "best_speedup": result['best_speedup'],
            "history": result['history'],
            "best_sql": result['best_sql'],
        }, f, indent=2)
    print(f"\n📁 Results saved to: {output_file}")


if __name__ == "__main__":
    main()
