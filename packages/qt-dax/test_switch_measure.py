"""Test optimization of a SWITCH-based measure with filter context."""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

adomd_path = r"C:\Windows\Microsoft.NET\assembly\GAC_MSIL\Microsoft.AnalysisServices.AdomdClient\v4.0_15.0.0.0__89845dcd8080cc91"
if os.path.exists(adomd_path):
    os.environ["PATH"] = adomd_path + os.pathsep + os.environ.get("PATH", "")
    sys.path.insert(0, adomd_path)
    import clr
    clr.AddReference("Microsoft.AnalysisServices.AdomdClient")


def configure_api_key() -> bool:
    """Load DeepSeek key from environment and normalize legacy var name."""
    key = os.environ.get("QT_DEEPSEEK_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        print("ERROR: Set QT_DEEPSEEK_API_KEY (or DEEPSEEK_API_KEY) before running.")
        return False
    os.environ["DEEPSEEK_API_KEY"] = key
    return True


def main():
    if not configure_api_key():
        return 1

    from qt_dax.connections import PBIDesktopConnection, find_pbi_instances

    print("=" * 70)
    print("SWITCH MEASURE OPTIMIZATION TEST")
    print("=" * 70)

    instances = find_pbi_instances()
    if not instances:
        print("ERROR: No Power BI Desktop instances found.")
        return 1

    inst = instances[0]
    print(f"\nConnected to PBI Desktop on port {inst.port}")

    with PBIDesktopConnection(inst.port) as conn:
        # Get the target measure
        measures = conn.get_measures()
        target_name = "Matrix MV Apportioned Carbon Switch_BM"

        measure_def = None
        for m in measures:
            if m.get("Measure") == target_name:
                measure_def = m.get("Expression", "")
                break

        if not measure_def:
            print(f"ERROR: Measure '{target_name}' not found")
            return 1

        print(f"\nMeasure: {target_name}")
        print(f"Length: {len(measure_def)} chars")
        print(f"\nExpression:\n{measure_def}")

        # Create test query with filter context (simulated visual filters)
        test_query_template = '''
DEFINE
    VAR __FilterDate = TREATAS({{ DATE(2025, 1, 31) }}, 'Benchmark Portfolio Mapping'[Valuation_Date])
    VAR __FilterSector = TREATAS({{ "Listed Equities" }}, 'Benchmark Portfolio Mapping'[Sector])
    VAR __FilterScope = TREATAS({{ "Scope 1 + 2" }}, 'Scope Emission Types'[Scope_Type_Desc])
    {measure_override}
EVALUATE
    SUMMARIZECOLUMNS(
        'GS Asset'[ISIN],
        __FilterDate,
        __FilterSector,
        __FilterScope,
        "Result", [{measure_name}]
    )
'''

        # Original query (no override)
        original_query = test_query_template.format(
            measure_override="",
            measure_name=target_name
        )

        print("\n" + "-" * 70)
        print("EXECUTE ORIGINAL")
        print("-" * 70)

        # Warmup
        print("\nWarmup (discarded)...")
        try:
            conn.execute_dax(original_query)
        except Exception as e:
            print(f"ERROR: {e}")
            return 1

        # Timed runs
        print("Timed runs...")
        original_times = []
        original_result = None
        for run in range(3):
            start = time.perf_counter()
            original_result = conn.execute_dax(original_query)
            elapsed = (time.perf_counter() - start) * 1000
            original_times.append(elapsed)
            print(f"  Run {run + 1}: {elapsed:.1f}ms")

        original_min = min(original_times)
        print(f"\nMin: {original_min:.1f}ms, Rows: {len(original_result)}")

        # Optimize with DeepSeek
        print("\n" + "-" * 70)
        print("LLM OPTIMIZATION (DeepSeek)")
        print("-" * 70)

        try:
            import dspy
            from qt_dax.optimization.dspy_optimizer import configure_lm, DAXOptimizer

            configure_lm(provider="deepseek")

            issues = """
- SWITCH statement with measure references - each branch is evaluated
- Multiple SELECTEDVALUE calls may be redundant
- Consider pre-computing scope type once with VAR
"""

            print("\nCalling DeepSeek...")

            optimizer = dspy.ChainOfThought(DAXOptimizer)
            opt_result = optimizer(
                measure_name=target_name,
                original_dax=measure_def,
                issues=issues,
                constraints="Preserve semantics EXACTLY. Return only the DAX expression.",
            )

            optimized_expr = opt_result.optimized_dax
            print(f"\nOptimized expression:\n{optimized_expr[:800]}")
            if len(optimized_expr) > 800:
                print("...")
            print(f"\nRationale: {opt_result.rationale[:300]}")

        except Exception as e:
            print(f"LLM error: {e}")
            import traceback
            traceback.print_exc()
            return 1

        # Execute optimized
        print("\n" + "-" * 70)
        print("EXECUTE OPTIMIZED (same filter context)")
        print("-" * 70)

        measure_override = f"    MEASURE 'ESG Trucost Climate'[{target_name}] = {optimized_expr}"
        optimized_query = test_query_template.format(
            measure_override=measure_override,
            measure_name=target_name
        )

        # Warmup
        print("\nWarmup (discarded)...")
        try:
            conn.execute_dax(optimized_query)
        except Exception as e:
            print(f"ERROR: Optimized query failed: {e}")
            print("\nOptimized measure:")
            print(optimized_expr)
            return 1

        # Timed runs
        print("Timed runs...")
        optimized_times = []
        optimized_result = None
        for run in range(3):
            start = time.perf_counter()
            optimized_result = conn.execute_dax(optimized_query)
            elapsed = (time.perf_counter() - start) * 1000
            optimized_times.append(elapsed)
            print(f"  Run {run + 1}: {elapsed:.1f}ms")

        optimized_min = min(optimized_times)
        print(f"\nMin: {optimized_min:.1f}ms, Rows: {len(optimized_result)}")

        # Validate EXACT match
        print("\n" + "-" * 70)
        print("VALIDATION (exact match)")
        print("-" * 70)

        if len(original_result) != len(optimized_result):
            print(f"\nFAIL: Row count mismatch ({len(original_result)} vs {len(optimized_result)})")
            return 1

        mismatches = []
        for i, (orig, opt) in enumerate(zip(original_result, optimized_result)):
            for key in orig:
                if orig[key] != opt[key]:
                    mismatches.append(f"Row {i}, {key}: {orig[key]} vs {opt[key]}")

        if mismatches:
            print(f"\nFAIL: {len(mismatches)} mismatches")
            for mm in mismatches[:5]:
                print(f"  {mm}")
            return 1

        print(f"\nPASS: All {len(original_result)} rows match exactly")

        # Results
        speedup = original_min / optimized_min if optimized_min > 0 else 0
        print(f"\n" + "=" * 70)
        print("RESULTS")
        print("=" * 70)
        print(f"Original:  {original_min:.1f}ms")
        print(f"Optimized: {optimized_min:.1f}ms")
        print(f"Speedup:   {speedup:.2f}x")
        if speedup > 1:
            print(f"Improvement: {((speedup-1)/speedup)*100:.1f}% faster")
        elif speedup < 1:
            print(f"Regression: {((1-speedup))*100:.1f}% slower")

    return 0


if __name__ == "__main__":
    sys.exit(main())
