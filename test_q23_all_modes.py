#!/usr/bin/env python3
"""
Test Q23 with all three V5 modes.

Q23 is challenging because:
- Complex: 3 CTEs, UNION ALL, multiple subqueries
- Previous run: 2.33x speedup but FAILED validation (semantic error)
- Perfect for testing error feedback and iterative improvement

This script will run all three modes and compare results.
"""

import sys
import os
from pathlib import Path
import json
import time
import logging

sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

# Enable detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_results/q23_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

from qt_sql.optimization.adaptive_rewriter_v5 import (
    optimize_v5_retry,
    optimize_v5_json_queue,
    optimize_v5_evolutionary,
)
from qt_sql.validation.validator import ValidationStatus

# Q23 SQL
Q23_SQL = """-- TPC-DS Query 23: Best customers by sales
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


def print_header(title):
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def print_section(title):
    """Print a formatted section."""
    print("\n" + "-" * 80)
    print(title)
    print("-" * 80)


def get_db_paths():
    """Get database paths from environment or defaults."""
    sample_db = os.getenv('QT_SAMPLE_DB', 'D:/TPC-DS/tpcds_sf1_sample.duckdb')
    full_db = os.getenv('QT_FULL_DB', 'D:/TPC-DS/tpcds_sf100.duckdb')

    return sample_db, full_db


def check_prerequisites():
    """Check if API key and databases are configured."""
    print_header("Q23 THREE-MODE TEST - Prerequisites Check")

    # Check API key
    api_key = os.getenv('QT_DEEPSEEK_API_KEY') or os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        print("\n❌ ERROR: DeepSeek API key not found!")
        print("Set environment variable:")
        print("  export QT_DEEPSEEK_API_KEY=your_api_key")
        return False
    print(f"✓ API key configured (length: {len(api_key)})")

    # Check databases
    sample_db, full_db = get_db_paths()

    if not Path(sample_db).exists():
        print(f"\n⚠️  WARNING: Sample DB not found at {sample_db}")
        print("Set QT_SAMPLE_DB environment variable")
        return False
    print(f"✓ Sample DB: {sample_db}")

    if not Path(full_db).exists():
        print(f"\n⚠️  WARNING: Full DB not found at {full_db}")
        print("Set QT_FULL_DB environment variable")
        return False
    print(f"✓ Full DB: {full_db}")

    print("\n✅ All prerequisites met!")
    return True


def test_mode1_retry(output_dir=None):
    """Test Mode 1: Retry with error feedback."""
    print_header("MODE 1: RETRY (Corrective Learning)")

    print("\n[Configuration]")
    print("  Strategy: Single worker with retries")
    print("  Max retries: 3")
    print("  Learning: From errors (error feedback)")
    print("  Goal: Produce valid optimized SQL (any speedup is a win)")
    print("  Why good for Q23: Can learn from semantic errors and retry")

    sample_db, full_db = get_db_paths()

    print("\n[Running...]")
    start_time = time.time()

    try:
        candidate, full_result, attempts = optimize_v5_retry(
            sql=Q23_SQL,
            sample_db=sample_db,
            full_db=full_db,
            query_id='q23',
            max_retries=3,
            target_speedup=2.0,  # Not used by Mode 1, but kept for API compatibility
            provider='deepseek',
            model=None,
            output_dir=output_dir,
        )

        elapsed = time.time() - start_time

        print(f"\n[Results] (completed in {elapsed:.1f}s)")
        print(f"  Attempts: {len(attempts)}")

        for attempt in attempts:
            status_icon = "✓" if attempt['status'] == 'pass' else "✗"
            print(f"  {status_icon} Attempt {attempt['attempt']}: {attempt['status']}")
            if attempt.get('error'):
                print(f"    Error: {attempt['error'][:100]}...")
            if attempt.get('speedup'):
                print(f"    Speedup: {attempt['speedup']:.2f}x")

        if candidate and full_result and full_result.full_status == ValidationStatus.PASS:
            print(f"\n✅ SUCCESS - Produced valid optimized SQL!")
            print(f"  Validation: PASSED")
            print(f"  Speedup: {full_result.full_speedup:.2f}x")
            print(f"  Attempts needed: {len(attempts)}")

            return {
                'mode': 'retry',
                'success': True,
                'attempts': len(attempts),
                'sample_speedup': candidate.speedup,
                'full_speedup': full_result.full_speedup,
                'time': elapsed,
                'sql': candidate.optimized_sql,
            }
        else:
            print(f"\n❌ FAILED: All {len(attempts)} attempts had validation errors")
            return {
                'mode': 'retry',
                'success': False,
                'attempts': len(attempts),
                'time': elapsed,
            }

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {
            'mode': 'retry',
            'success': False,
            'error': str(e),
            'time': elapsed,
        }


def test_mode2_parallel(output_dir=None):
    """Test Mode 2: Parallel workers."""
    print_header("MODE 2: PARALLEL (Tournament Competition)")

    print("\n[Configuration]")
    print("  Strategy: 5 workers with different strategies")
    print("  Max workers: 5")
    print("  Learning: From diversity (best wins)")
    print("  Why good for Q23: Multiple approaches, one might avoid semantic issues")

    sample_db, full_db = get_db_paths()

    print("\n[Running...]")
    start_time = time.time()

    try:
        valid, full_results, winner = optimize_v5_json_queue(
            sql=Q23_SQL,
            sample_db=sample_db,
            full_db=full_db,
            query_id='q23',
            max_workers=5,
            target_speedup=2.0,
            provider='deepseek',
            model=None,
            output_dir=output_dir,
        )

        elapsed = time.time() - start_time

        print(f"\n[Results] (completed in {elapsed:.1f}s)")
        print(f"  Valid candidates: {len(valid)}/5")

        for cand in valid:
            print(f"  ✓ Worker {cand.worker_id}: {cand.speedup:.2f}x (sample)")

        if winner:
            print(f"\n🏆 WINNER: Worker {winner.sample.worker_id}")
            print(f"  Sample speedup: {winner.sample.speedup:.2f}x")
            print(f"  Full DB speedup: {winner.full_speedup:.2f}x")
            print(f"  Target met: {'Yes' if winner.full_speedup >= 2.0 else 'No'}")

            return {
                'mode': 'parallel',
                'success': True,
                'valid_workers': len(valid),
                'winner_id': winner.sample.worker_id,
                'sample_speedup': winner.sample.speedup,
                'full_speedup': winner.full_speedup,
                'time': elapsed,
                'sql': winner.sample.optimized_sql,
            }
        elif full_results and len(full_results) > 0:
            # No winner met target, but return best result
            best = max(full_results, key=lambda x: x.full_speedup if x.full_speedup else 0)
            print(f"\n⚠️  COMPLETED but no winner met target")
            print(f"  Best: Worker {best.sample.worker_id}")
            print(f"  Full DB speedup: {best.full_speedup:.2f}x (below 2.0x target)")

            return {
                'mode': 'parallel',
                'success': False,
                'valid_workers': len(valid),
                'winner_id': best.sample.worker_id,
                'sample_speedup': best.sample.speedup,
                'full_speedup': best.full_speedup,
                'time': elapsed,
                'sql': best.sample.optimized_sql,  # Include best SQL even if below target
            }
        else:
            print(f"\n❌ FAILED: No valid candidates")
            return {
                'mode': 'parallel',
                'success': False,
                'valid_workers': len(valid),
                'time': elapsed,
            }

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {
            'mode': 'parallel',
            'success': False,
            'error': str(e),
            'time': elapsed,
        }


def test_mode3_evolutionary(output_dir=None):
    """Test Mode 3: Evolutionary."""
    print_header("MODE 3: EVOLUTIONARY (Stacking)")

    print("\n[Configuration]")
    print("  Strategy: Iterative improvement")
    print("  Max iterations: 5")
    print("  Learning: From successes (builds on best)")
    print("  Why good for Q23: Can iteratively fix issues and stack optimizations")

    sample_db, full_db = get_db_paths()

    print("\n[Running...]")
    start_time = time.time()

    try:
        best, full_result, iterations = optimize_v5_evolutionary(
            sql=Q23_SQL,
            full_db=full_db,
            query_id='q23',
            max_iterations=5,
            target_speedup=2.0,
            provider='deepseek',
            model=None,
            output_dir=output_dir,
        )

        elapsed = time.time() - start_time

        print(f"\n[Results] (completed in {elapsed:.1f}s)")
        print(f"  Iterations: {len(iterations)}")

        for it in iterations:
            if it['status'] == 'success':
                improved = "🔼" if it.get('improved') else "→"
                print(f"  {improved} Iteration {it['iteration']}: {it['speedup']:.2f}x")
            else:
                print(f"  ✗ Iteration {it['iteration']}: {it['status']}")
                if it.get('error'):
                    print(f"    Error: {it['error'][:100]}...")

        if best and full_result:
            success = full_result.full_speedup >= 2.0
            if success:
                print(f"\n🏆 BEST: Iteration {best.worker_id}")
            else:
                print(f"\n⚠️  BEST (below target): Iteration {best.worker_id}")

            print(f"  Final speedup: {full_result.full_speedup:.2f}x")
            print(f"  Target met: {'Yes' if success else 'No'}")

            return {
                'mode': 'evolutionary',
                'success': success,
                'iterations': len(iterations),
                'best_iteration': best.worker_id,
                'full_speedup': full_result.full_speedup,
                'time': elapsed,
                'sql': best.optimized_sql,  # Include SQL even if below target
            }
        else:
            print(f"\n❌ FAILED: No successful optimization")
            return {
                'mode': 'evolutionary',
                'success': False,
                'iterations': len(iterations),
                'time': elapsed,
            }

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {
            'mode': 'evolutionary',
            'success': False,
            'error': str(e),
            'time': elapsed,
        }


def compare_results(results):
    """Compare results from all three modes."""
    print_header("COMPARISON: All Three Modes")

    print("\n[Summary]")

    # Create comparison table
    modes = ['retry', 'parallel', 'evolutionary']

    print(f"\n{'Mode':<15} {'Success':<10} {'Speedup':<10} {'Time':<10} {'Notes':<30}")
    print("-" * 80)

    for mode in modes:
        if mode in results:
            r = results[mode]
            success = "✅ Yes" if r.get('success') else "❌ No"
            speedup = f"{r.get('full_speedup', 0):.2f}x" if r.get('full_speedup') else "N/A"
            time_str = f"{r.get('time', 0):.1f}s"

            notes = ""
            if mode == 'retry':
                notes = f"{r.get('attempts', 0)} attempts"
            elif mode == 'parallel':
                notes = f"{r.get('valid_workers', 0)} valid workers"
            elif mode == 'evolutionary':
                notes = f"{r.get('iterations', 0)} iterations"

            print(f"{mode:<15} {success:<10} {speedup:<10} {time_str:<10} {notes:<30}")

    # Best mode
    print("\n[Analysis]")

    successful = [m for m in modes if results.get(m, {}).get('success')]
    if successful:
        best = max(successful, key=lambda m: results[m].get('full_speedup', 0))
        print(f"  Best mode: {best}")
        print(f"  Best speedup: {results[best].get('full_speedup'):.2f}x")
        print(f"  Time: {results[best].get('time'):.1f}s")

        # Save best result
        output_dir = Path('test_results/q23')
        output_dir.mkdir(parents=True, exist_ok=True)

        output_file = output_dir / f'{best}_optimized.sql'
        with open(output_file, 'w') as f:
            f.write(results[best].get('sql', ''))
        print(f"\n  ✓ Best SQL saved to: {output_file}")

        # Save full results
        results_file = output_dir / 'comparison_results.json'
        with open(results_file, 'w') as f:
            # Remove SQL from JSON (too large)
            json_results = {k: {kk: vv for kk, vv in v.items() if kk != 'sql'}
                           for k, v in results.items()}
            json.dump(json_results, f, indent=2)
        print(f"  ✓ Full results saved to: {results_file}")
    else:
        print("  ❌ No mode succeeded")

    print("\n[Key Insights]")
    print("  • Mode 1 (Retry): Can learn from semantic errors")
    print("  • Mode 2 (Parallel): Explores multiple strategies")
    print("  • Mode 3 (Evolutionary): Iteratively improves")
    print("\n  Q23 previously achieved 2.33x but failed validation.")
    print("  Testing if error feedback can fix semantic issues...")


def main():
    """Run all three modes on Q23 and compare."""
    import json
    from datetime import datetime

    print_header("Q23 THREE-MODE COMPREHENSIVE TEST")

    print("\n[Query Info]")
    print("  Query: TPC-DS Q23 (Best customers by sales)")
    print("  Complexity: 3 CTEs, UNION ALL, multiple subqueries")
    print("  Previous result: 2.33x speedup, but FAILED validation (semantic error)")
    print("  Goal: Test if V5 modes can achieve speedup while preserving correctness")

    # Check prerequisites
    if not check_prerequisites():
        print("\n❌ Prerequisites not met. Exiting.")
        return 1

    # Create results directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = Path(f'test_results/q23_{timestamp}')
    results_dir.mkdir(parents=True, exist_ok=True)
    print(f"\n✓ Results will be saved to: {results_dir}")

    # Save original query
    (results_dir / 'original_q23.sql').write_text(Q23_SQL)

    # Run all three modes
    results = {}
    start_time = time.time()

    print("\n" + "=" * 80)
    print("STARTING ALL 3 MODES")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Estimated duration: 10-22 minutes")
    print(f"Estimated API calls: 7-18 calls")
    print("=" * 80)

    # Mode 1: Retry
    results['retry'] = test_mode1_retry(output_dir=str(results_dir))
    if results['retry'].get('sql'):
        (results_dir / 'retry_optimized.sql').write_text(results['retry']['sql'])

    # Mode 2: Parallel
    results['parallel'] = test_mode2_parallel(output_dir=str(results_dir))
    if results['parallel'].get('sql'):
        (results_dir / 'parallel_optimized.sql').write_text(results['parallel']['sql'])

    # Mode 3: Evolutionary
    results['evolutionary'] = test_mode3_evolutionary(output_dir=str(results_dir))
    if results['evolutionary'].get('sql'):
        (results_dir / 'evolutionary_optimized.sql').write_text(results['evolutionary']['sql'])

    total_time = time.time() - start_time

    # Save detailed results
    detailed_results = {
        'query': 'q23',
        'timestamp': timestamp,
        'total_time': total_time,
        'modes': {}
    }

    for mode, result in results.items():
        # Remove SQL from JSON (save separately)
        result_copy = {k: v for k, v in result.items() if k != 'sql'}
        detailed_results['modes'][mode] = result_copy

    (results_dir / 'detailed_results.json').write_text(json.dumps(detailed_results, indent=2))

    # Compare results
    compare_results(results)

    print_header("TEST COMPLETE")
    print(f"\nTotal execution time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"\n✓ All results saved to: {results_dir}")
    print(f"  - original_q23.sql")
    print(f"  - detailed_results.json")
    for mode in ['retry', 'parallel', 'evolutionary']:
        if results.get(mode, {}).get('sql'):
            print(f"  - {mode}_optimized.sql")

    # Check if any mode succeeded
    if any(r.get('success') for r in results.values()):
        print("\n✅ At least one mode succeeded!")
        return 0
    else:
        print("\n❌ All modes failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
