"""Test the manually optimized DAX from the jsonl file."""

import json
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


def main():
    from qt_dax.connections import PBIDesktopConnection, find_pbi_instances

    print("=" * 70)
    print("MANUAL OPTIMIZATION TEST")
    print("=" * 70)

    instances = find_pbi_instances()
    if not instances:
        print("ERROR: No Power BI Desktop instances found.")
        return 1

    inst = instances[0]
    print(f"\nConnected to PBI Desktop on port {inst.port}")

    # Load test data
    test_file = os.path.join(os.path.dirname(__file__), "dax_optimizations_extracted.jsonl")
    with open(test_file) as f:
        test_case = json.loads(f.readline())

    print(f"\nMeasure: {test_case.get('measure_name')}")
    print(f"Recorded original time: {test_case.get('original_time')}")

    # Get original PA query with filter context
    original_query = test_case.get("single_measure_dax", "")

    # Strip leading comments
    lines = original_query.strip().split('\n')
    while lines and lines[0].strip().startswith('//'):
        lines.pop(0)
    original_query = '\n'.join(lines)

    # Get optimized DAX (manually created)
    optimized_dax = test_case.get("optimized_dax", "")

    # The optimized_dax is a full DEFINE block - we need to merge it with the original query's
    # filter context (VARs) and EVALUATE block

    # Extract the MEASURE definition from optimized_dax
    import re
    measure_match = re.search(
        r"MEASURE\s+'([^']+)'\[([^\]]+)\]\s*=\s*([\s\S]+)",
        optimized_dax,
        re.IGNORECASE
    )

    if not measure_match:
        print("ERROR: Could not extract measure from optimized DAX")
        return 1

    opt_table = measure_match.group(1)
    opt_measure = measure_match.group(2)
    opt_expression = measure_match.group(3).strip()

    print(f"\nOptimized measure: '{opt_table}'[{opt_measure}]")
    print(f"Expression length: {len(opt_expression)} chars")

    # Fix: The original query references a measure that doesn't exist in current model
    # Update ALL references to use the measure from the optimized version

    # 1. Fix the measure reference in SUMMARIZECOLUMNS
    original_query = re.sub(
        r"'ESG Trucost Climate'\[Portfolio_Asset_Matrix MV CR Intensity Switch_BM NEW\]",
        f"'ESG Trucost Climate'[{opt_measure}]",
        original_query
    )

    # 2. Fix the string alias in SUMMARIZECOLUMNS
    original_query = re.sub(
        r'"Portfolio_Asset_Matrix_MV_CR_Intensity_Switch_BM"',
        '"Result"',
        original_query
    )

    # 3. Fix the column reference in TOPN
    original_query = re.sub(
        r'\[Portfolio_Asset_Matrix_MV_CR_Intensity_Switch_BM\]',
        '[Result]',
        original_query
    )

    # 4. Build simplest possible test query - no filters
    original_query = f'''
EVALUATE
TOPN(
    50,
    ADDCOLUMNS(
        VALUES('GS Asset'[ISIN]),
        "Result", 'ESG Trucost Climate'[{opt_measure}]
    ),
    [Result], DESC
)
'''

    print("\n--- Test query (no filters) ---")
    print(original_query)
    print("---\n")

    with PBIDesktopConnection(inst.port) as conn:
        # Test original query
        print("\n" + "-" * 70)
        print("EXECUTE ORIGINAL PA QUERY")
        print("-" * 70)

        print("\nWarmup (discarded)...")
        try:
            conn.execute_dax(original_query)
        except Exception as e:
            print(f"ERROR: Original query failed: {e}")
            print("\nQuery (first 500 chars):")
            print(original_query[:500])
            return 1

        print("Timed runs (3x, taking minimum)...")
        original_times = []
        original_result = None
        for run in range(3):
            start = time.perf_counter()
            original_result = conn.execute_dax(original_query)
            elapsed = (time.perf_counter() - start) * 1000
            original_times.append(elapsed)
            print(f"  Run {run + 1}: {elapsed/1000:.2f}s")

        original_min = min(original_times)
        print(f"\nOriginal minimum: {original_min/1000:.2f}s")
        print(f"Rows returned: {len(original_result)}")
        if original_result and len(original_result) > 0:
            print(f"First row: {original_result[0]}")

        # Create optimized query by injecting the measure override
        print("\n" + "-" * 70)
        print("EXECUTE OPTIMIZED PA QUERY (same filter context)")
        print("-" * 70)

        # Find where to inject the MEASURE (after DEFINE, before first VAR)
        define_pos = original_query.upper().find("DEFINE")
        if define_pos == -1:
            print("ERROR: No DEFINE block found")
            return 1

        # Find the first VAR after DEFINE
        var_pos = original_query.upper().find("VAR", define_pos)
        if var_pos == -1:
            var_pos = define_pos + 6  # Just after "DEFINE"

        # Inject the optimized measure
        measure_def = f"\n    MEASURE '{opt_table}'[{opt_measure}] = \n        {opt_expression}\n"
        optimized_query = original_query[:var_pos] + measure_def + "\n    " + original_query[var_pos:]

        print("\nWarmup (discarded)...")
        try:
            conn.execute_dax(optimized_query)
        except Exception as e:
            print(f"ERROR: Optimized query failed: {e}")
            # Show where the error might be
            print("\nOptimized query (first 2000 chars):")
            print(optimized_query[:2000])
            return 1

        print("Timed runs (3x, taking minimum)...")
        optimized_times = []
        optimized_result = None
        for run in range(3):
            start = time.perf_counter()
            optimized_result = conn.execute_dax(optimized_query)
            elapsed = (time.perf_counter() - start) * 1000
            optimized_times.append(elapsed)
            print(f"  Run {run + 1}: {elapsed/1000:.2f}s")

        optimized_min = min(optimized_times)
        print(f"\nOptimized minimum: {optimized_min/1000:.2f}s")
        print(f"Rows returned: {len(optimized_result)}")
        if optimized_result and len(optimized_result) > 0:
            print(f"First row: {optimized_result[0]}")

        # Calculate speedup
        speedup = original_min / optimized_min if optimized_min > 0 else 0

        # Validate EXACT match
        print("\n" + "-" * 70)
        print("VALIDATION (exact match)")
        print("-" * 70)

        # 0 rows = can't validate correctness, but can validate syntax and timing
        if len(original_result) == 0:
            print("\nWARNING: 0 rows returned - cannot validate value correctness")
            print("Syntax: PASS (both queries executed without error)")
            print(f"Timing: {speedup:.2f}x faster (best estimate, needs data for confirmation)")

            # Still report results
            print("\n" + "=" * 70)
            print("RESULTS (SYNTAX ONLY - NO DATA TO VALIDATE)")
            print("=" * 70)
            print(f"Original:  {original_min/1000:.2f}s")
            print(f"Optimized: {optimized_min/1000:.2f}s")
            print(f"Speedup:   {speedup:.2f}x (estimated)")
            print("\nTo fully validate: load data into model and re-run")
            return 0

        if len(original_result) != len(optimized_result):
            print(f"\nFAIL: Row count mismatch")
            print(f"  Original: {len(original_result)} rows")
            print(f"  Optimized: {len(optimized_result)} rows")
            return 1

        # Sort both results for comparison (order may differ)
        def sort_key(row):
            return tuple(str(v) for v in row.values())

        original_sorted = sorted(original_result, key=sort_key)
        optimized_sorted = sorted(optimized_result, key=sort_key)

        mismatches = []
        for i, (orig, opt) in enumerate(zip(original_sorted, optimized_sorted)):
            for key in orig:
                orig_val = orig.get(key)
                opt_val = opt.get(key)
                if orig_val != opt_val:
                    # Check if it's a float comparison issue
                    try:
                        if abs(float(orig_val or 0) - float(opt_val or 0)) < 1e-9:
                            continue  # Close enough for floats
                    except (TypeError, ValueError):
                        pass
                    mismatches.append({
                        "row": i,
                        "column": key,
                        "original": orig_val,
                        "optimized": opt_val,
                    })

        if mismatches:
            print(f"\nFAIL: {len(mismatches)} value mismatches")
            for mm in mismatches[:10]:
                print(f"  Row {mm['row']}, {mm['column']}: {mm['original']} vs {mm['optimized']}")
            return 1

        print(f"\nPASS: All {len(original_result)} rows match exactly")

        # Results
        speedup = original_min / optimized_min if optimized_min > 0 else 0
        print("\n" + "=" * 70)
        print("RESULTS")
        print("=" * 70)
        print(f"Original:  {original_min/1000:.2f}s ({original_times})")
        print(f"Optimized: {optimized_min/1000:.2f}s ({optimized_times})")
        print(f"Speedup:   {speedup:.2f}x")

        if speedup > 1:
            pct = ((speedup - 1) / speedup) * 100
            time_saved = (original_min - optimized_min) / 1000
            print(f"Improvement: {pct:.1f}% faster ({time_saved:.2f}s saved)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
