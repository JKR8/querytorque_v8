"""Test DAX optimization using actual Performance Analyzer query with filter context.

The PA query contains the visual's filter context - we must validate
using that SAME context to ensure results match exactly.

Run from Windows PowerShell:
    cd C:/Users/jakc9/Documents/QueryTorque_V8/packages/qt-dax
    python test_pa_query_optimization.py
"""

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Add ADOMD client DLL path
adomd_path = r"C:\Windows\Microsoft.NET\assembly\GAC_MSIL\Microsoft.AnalysisServices.AdomdClient\v4.0_15.0.0.0__89845dcd8080cc91"
if os.path.exists(adomd_path):
    os.environ["PATH"] = adomd_path + os.pathsep + os.environ.get("PATH", "")
    sys.path.insert(0, adomd_path)
    try:
        import clr
        clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
    except Exception:
        pass


def configure_api_key() -> bool:
    """Load DeepSeek key from environment and normalize legacy var name."""
    key = os.environ.get("QT_DEEPSEEK_API_KEY") or os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        print("ERROR: Set QT_DEEPSEEK_API_KEY (or DEEPSEEK_API_KEY) before running.")
        return False
    os.environ["DEEPSEEK_API_KEY"] = key
    return True


def extract_measure_from_pa_query(query: str) -> tuple[str, str, str]:
    """
    Extract the measure reference from a PA query's SUMMARIZECOLUMNS.

    Returns: (table_name, measure_name, full_reference)
    """
    # Look for 'Table'[Measure] pattern in SUMMARIZECOLUMNS
    pattern = r"'([^']+)'\[([^\]]+)\]"
    matches = list(re.finditer(pattern, query))

    # The measure is typically the last 'Table'[Name] in SUMMARIZECOLUMNS
    # that's used as a calculated column (after a string alias)
    for match in reversed(matches):
        table = match.group(1)
        name = match.group(2)
        # Skip common column tables
        if table not in ['GS Asset', 'Benchmark Portfolio Mapping', 'Market Cap Type', 'Scope Emission Types']:
            return table, name, f"'{table}'[{name}]"

    # Fallback - return last match
    if matches:
        match = matches[-1]
        return match.group(1), match.group(2), f"'{match.group(1)}'[{match.group(2)}]"

    return None, None, None


def create_optimized_pa_query(original_query: str, measure_table: str, measure_name: str, optimized_expr: str) -> str:
    """
    Create a modified PA query that uses the optimized measure expression.

    Injects the optimized measure as a MEASURE definition in the DEFINE block,
    keeping all the filter context intact.
    """
    # Find where DEFINE block ends (before first VAR or after DEFINE keyword)
    define_match = re.search(r'DEFINE\s*\n', original_query, re.IGNORECASE)
    if not define_match:
        return None

    insert_pos = define_match.end()

    # Create the MEASURE definition
    measure_def = f"    MEASURE '{measure_table}'[{measure_name}] = \n        {optimized_expr}\n\n"

    # Insert the measure definition
    optimized_query = original_query[:insert_pos] + measure_def + original_query[insert_pos:]

    return optimized_query


def main():
    if not configure_api_key():
        return 1

    from qt_dax.connections import PBIDesktopConnection, find_pbi_instances

    print("=" * 70)
    print("PERFORMANCE ANALYZER QUERY OPTIMIZATION TEST")
    print("=" * 70)

    # Find PBI Desktop
    instances = find_pbi_instances()
    if not instances:
        print("ERROR: No Power BI Desktop instances found.")
        return 1

    inst = instances[0]
    print(f"\nConnected to PBI Desktop on port {inst.port}")

    # Load a test PA query
    test_file = os.path.join(os.path.dirname(__file__), "dax_optimizations_extracted.jsonl")
    with open(test_file) as f:
        test_case = json.loads(f.readline())

    original_query = test_case.get("single_measure_dax") or test_case.get("original_dax", "")

    # Strip leading comments
    lines = original_query.strip().split('\n')
    while lines and lines[0].strip().startswith('//'):
        lines.pop(0)
    original_query = '\n'.join(lines)

    print(f"\nTest: {test_case.get('measure_name')}")
    print(f"Recorded time: {test_case.get('original_time')}")

    # Extract measure info
    measure_table, measure_name, measure_ref = extract_measure_from_pa_query(original_query)
    print(f"\nMeasure reference: {measure_ref}")

    # For testing, let's target a measure with more complexity
    # The referenced measure calls another measure - let's optimize that deeper measure instead
    target_deeper_measure = "Matrix MV CR Intensity Switch_BM"  # The inner measure
    print(f"\nTargeting deeper measure: {target_deeper_measure}")

    if not measure_name:
        print("ERROR: Could not extract measure from query")
        return 1

    with PBIDesktopConnection(inst.port) as conn:
        # Get the measure definition from the model
        measures = conn.get_measures()
        measure_def = None
        for m in measures:
            if m.get("Measure", "").lower() == measure_name.lower():
                measure_def = m.get("Expression", "")
                break

        if not measure_def:
            # Try without "NEW" suffix
            alt_name = measure_name.replace(" NEW", "").replace("_NEW", "")
            for m in measures:
                if m.get("Measure", "").lower() == alt_name.lower():
                    old_measure_name = measure_name
                    measure_name = m.get("Measure")
                    measure_def = m.get("Expression", "")
                    # Update the query to use the correct measure name
                    original_query = original_query.replace(
                        f"[{old_measure_name}]",
                        f"[{measure_name}]"
                    )
                    measure_ref = f"'{measure_table}'[{measure_name}]"
                    print(f"  (Using '{measure_name}' instead of '{old_measure_name}')")
                    break

        if not measure_def:
            print(f"ERROR: Measure '{measure_name}' not found in model")
            print("\nAvailable measures containing similar names:")
            for m in measures:
                if measure_name.split()[0].lower() in m.get("Measure", "").lower():
                    print(f"  - {m.get('Measure')}")
            return 1

        print(f"\nMeasure definition (first 300 chars):")
        print(measure_def[:300])
        if len(measure_def) > 300:
            print("...")

        # Show the filter context from the PA query
        print("\n" + "-" * 70)
        print("FILTER CONTEXT (from visual)")
        print("-" * 70)

        # Extract VAR definitions (filter tables)
        var_pattern = re.compile(r'VAR\s+(__DS\d+\w*)\s*=', re.IGNORECASE)
        filter_vars = var_pattern.findall(original_query)
        print(f"\nFilter variables: {len(filter_vars)}")
        for v in filter_vars[:8]:
            print(f"  - {v}")
        if len(filter_vars) > 8:
            print(f"  ... and {len(filter_vars) - 8} more")

        # Test original query execution
        print("\n" + "-" * 70)
        print("EXECUTE ORIGINAL PA QUERY")
        print("-" * 70)

        import time

        # Warmup run (discarded)
        print("\nWarmup run (discarded)...")
        try:
            conn.execute_dax(original_query)
        except Exception as e:
            print(f"ERROR: Original query failed: {e}")
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
        print(f"\nOriginal minimum: {original_min:.1f}ms")
        print(f"Result rows: {len(original_result)}")
        if original_result:
            print(f"Sample result: {original_result[0]}")

        # Now optimize the measure with DeepSeek
        print("\n" + "-" * 70)
        print("LLM OPTIMIZATION (DeepSeek)")
        print("-" * 70)

        try:
            import dspy
            from qt_dax.optimization.dspy_optimizer import configure_lm, DAXOptimizer

            configure_lm(provider="deepseek")

            # Simple issue detection
            issues = []
            expr_upper = measure_def.upper()
            if "FILTER(" in expr_upper:
                issues.append("FILTER function detected - potential table scan")
            if expr_upper.count("CALCULATE") > 2:
                issues.append(f"Deep CALCULATE nesting ({expr_upper.count('CALCULATE')} levels)")
            if "SUMX" in expr_upper and "FILTER" in expr_upper:
                issues.append("SUMX with FILTER - row-by-row iteration")
            if not issues:
                issues.append("Analyze for any optimization opportunities")

            issues_text = "\n".join(f"- {i}" for i in issues)
            print(f"\nDetected issues:\n{issues_text}")

            print("\nCalling DeepSeek...")

            optimizer = dspy.ChainOfThought(DAXOptimizer)
            opt_result = optimizer(
                measure_name=measure_name,
                original_dax=measure_def,
                issues=issues_text,
                constraints="Preserve semantics EXACTLY. Return only the DAX expression (no measure name, no EVALUATE).",
            )

            optimized_expr = opt_result.optimized_dax
            if not optimized_expr:
                print("ERROR: No optimization returned")
                return 1

            print(f"\nOptimized expression (first 500 chars):")
            print(optimized_expr[:500])
            if len(optimized_expr) > 500:
                print("...")

            print(f"\nRationale: {opt_result.rationale[:300]}")

        except Exception as e:
            print(f"LLM optimization failed: {e}")
            import traceback
            traceback.print_exc()
            return 1

        # Create optimized PA query with same filter context
        print("\n" + "-" * 70)
        print("EXECUTE OPTIMIZED PA QUERY (same filter context)")
        print("-" * 70)

        optimized_query = create_optimized_pa_query(
            original_query, measure_table, measure_name, optimized_expr
        )

        if not optimized_query:
            print("ERROR: Could not create optimized query")
            return 1

        # Warmup run (discarded)
        print("\nWarmup run (discarded)...")
        try:
            conn.execute_dax(optimized_query)
        except Exception as e:
            print(f"ERROR: Optimized query failed: {e}")
            print("\nQuery (first 1000 chars):")
            print(optimized_query[:1000])
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
        print(f"\nOptimized minimum: {optimized_min:.1f}ms")
        print(f"Result rows: {len(optimized_result)}")
        if optimized_result:
            print(f"Sample result: {optimized_result[0]}")

        # Validate results
        print("\n" + "-" * 70)
        print("VALIDATION (exact match)")
        print("-" * 70)

        # Compare row counts
        if len(original_result) != len(optimized_result):
            print(f"\nFAIL: Row count mismatch")
            print(f"  Original: {len(original_result)} rows")
            print(f"  Optimized: {len(optimized_result)} rows")
            return 1

        # Compare values exactly
        mismatches = []
        for i, (orig_row, opt_row) in enumerate(zip(original_result, optimized_result)):
            for key in orig_row:
                orig_val = orig_row.get(key)
                opt_val = opt_row.get(key)
                if orig_val != opt_val:
                    mismatches.append({
                        "row": i,
                        "column": key,
                        "original": orig_val,
                        "optimized": opt_val,
                    })

        if mismatches:
            print(f"\nFAIL: {len(mismatches)} value mismatches")
            for mm in mismatches[:5]:
                print(f"  Row {mm['row']}, {mm['column']}: {mm['original']} vs {mm['optimized']}")
            return 1

        # Success!
        print(f"\nPASS: All {len(original_result)} rows match exactly")

        # Calculate speedup
        speedup = original_min / optimized_min if optimized_min > 0 else 0
        print(f"\n" + "=" * 70)
        print("RESULTS")
        print("=" * 70)
        print(f"Original:  {original_min:.1f}ms (min of {original_times})")
        print(f"Optimized: {optimized_min:.1f}ms (min of {optimized_times})")
        print(f"Speedup:   {speedup:.2f}x")
        if speedup > 1:
            pct = ((speedup - 1) / speedup) * 100
            print(f"Improvement: {pct:.1f}% faster")

    return 0


if __name__ == "__main__":
    sys.exit(main())
