"""Test live DAX optimization with a real measure from the model.

Run from Windows PowerShell:
    cd C:/Users/jakc9/Documents/QueryTorque_V8/packages/qt-dax
    python test_live_optimization.py
"""

import os
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


def main():
    os.environ["DEEPSEEK_API_KEY"] = "sk-fdc7c2f4536742f6a322a03c04ce79dd"

    from qt_dax.connections import PBIDesktopConnection, find_pbi_instances
    from qt_dax.analyzers.visual_query_analyzer import VisualQueryAnalyzer
    from qt_dax.analyzers.measure_dependencies import MeasureDependencyAnalyzer

    print("=" * 70)
    print("LIVE DAX OPTIMIZATION TEST")
    print("=" * 70)

    # Find PBI Desktop
    instances = find_pbi_instances()
    if not instances:
        print("ERROR: No Power BI Desktop instances found.")
        return 1

    inst = instances[0]
    print(f"\nConnected to PBI Desktop on port {inst.port}")

    with PBIDesktopConnection(inst.port) as conn:
        # Get a complex measure from the model
        measures = conn.get_measures()

        # Find a measure with dependencies (contains [MeasureName] references)
        complex_measures = []
        for m in measures:
            expr = m.get("Expression", "")
            name = m.get("Measure", "")
            # Look for measures that reference other measures
            if "[" in expr and name and "Matrix" in name:
                # Count measure references (approximate)
                ref_count = expr.count("[")
                complex_measures.append((name, m.get("Table", ""), expr, ref_count))

        # Sort by reference count
        complex_measures.sort(key=lambda x: x[3], reverse=True)

        if not complex_measures:
            print("No complex measures found")
            return 1

        # Pick one of the most complex
        target_name, target_table, target_expr, ref_count = complex_measures[0]

        print(f"\nTarget measure: {target_table}[{target_name}]")
        print(f"Approximate references: {ref_count}")
        print(f"\nExpression (first 500 chars):")
        print(target_expr[:500])
        if len(target_expr) > 500:
            print("...")

        # Build dependency analysis
        print("\n" + "-" * 70)
        print("DEPENDENCY ANALYSIS")
        print("-" * 70)

        # Convert measures to format expected by analyzer
        measure_list = [
            {"name": m.get("Measure", ""), "table": m.get("Table", ""), "expression": m.get("Expression", "")}
            for m in measures
        ]

        dep_analyzer = MeasureDependencyAnalyzer()
        dep_result = dep_analyzer.analyze(measure_list)

        # Get dependency chain for our target measure
        chain = dep_analyzer.get_dependency_chain(dep_result, target_name)

        print(f"\nDependency chain for {target_name}:")
        print(f"  Total measures in chain: {len(chain)}")
        print(f"  Max depth: {dep_result.max_depth}")

        if chain:
            print("\n  Chain (leaf to root):")
            for i, m in enumerate(chain[:20], 1):
                node = dep_result.nodes.get(m.lower())
                deps = len(node.depends_on) if node else 0
                print(f"    {i}. {m} (depends on {deps})")
            if len(chain) > 20:
                print(f"    ... and {len(chain) - 20} more")

        # Test execution
        print("\n" + "-" * 70)
        print("EXECUTION TEST")
        print("-" * 70)

        # Create a simple EVALUATE query for the measure
        test_query = f"EVALUATE ROW(\"Result\", [{target_name}])"

        print(f"\nQuery: {test_query}")
        print("\nExecuting...")

        try:
            import time
            start = time.perf_counter()
            result = conn.execute_dax(test_query)
            elapsed = (time.perf_counter() - start) * 1000

            print(f"  Result: {result}")
            print(f"  Time: {elapsed:.1f}ms")

        except Exception as e:
            print(f"  Error: {e}")

        # Now test with DeepSeek optimization
        print("\n" + "-" * 70)
        print("LLM OPTIMIZATION TEST (DeepSeek)")
        print("-" * 70)

        try:
            import dspy
            from qt_dax.optimization.dspy_optimizer import configure_lm, DAXOptimizer

            configure_lm(provider="deepseek")

            # Analyze for issues
            issues = []
            expr_upper = target_expr.upper()
            if "FILTER(" in expr_upper and ("SUMX" in expr_upper or "CALCULATE" in expr_upper):
                issues.append("Potential FILTER table iterator - may cause slow row-by-row evaluation")
            if expr_upper.count("CALCULATE") > 2:
                issues.append(f"Deep CALCULATE nesting ({expr_upper.count('CALCULATE')} levels)")
            if "SWITCH" in expr_upper and "[" in target_expr:
                issues.append("SWITCH with measure references - evaluate order matters")
            if not issues:
                issues.append("No obvious anti-patterns detected, but LLM may find optimizations")

            issues_text = "\n".join(f"- {i}" for i in issues)

            print(f"\nDetected issues:")
            print(issues_text)

            print("\nCalling DeepSeek for optimization...")

            optimizer = dspy.ChainOfThought(DAXOptimizer)
            result = optimizer(
                measure_name=target_name,
                original_dax=target_expr,
                issues=issues_text,
                constraints="Preserve semantics exactly. Return only the DAX expression.",
            )

            print(f"\n=== OPTIMIZED DAX ===")
            print(result.optimized_dax[:1000] if result.optimized_dax else "No optimization returned")
            if result.optimized_dax and len(result.optimized_dax) > 1000:
                print("...")

            print(f"\n=== RATIONALE ===")
            print(result.rationale[:500] if result.rationale else "No rationale")

            # Validate the optimization
            if result.optimized_dax:
                print("\n" + "-" * 70)
                print("VALIDATION")
                print("-" * 70)

                from qt_dax.validation import DAXEquivalenceValidator

                validator = DAXEquivalenceValidator(
                    connection=conn,
                    max_rows_to_compare=10000,
                    warmup_runs=3,       # 3 timed runs, take minimum
                    discard_first=True,  # Discard first run (cache warmup)
                    exact_match=True,    # Require EXACT value match
                )

                print("\nValidation methodology:")
                print("  - Discard first run (cache warmup)")
                print("  - Run 3 timed executions per query")
                print("  - Use MINIMUM time from timed runs")
                print("  - Require EXACT value match")
                print("\nValidating...")

                try:
                    validation = validator.validate(target_expr, result.optimized_dax)

                    # Results
                    status_str = "PASS" if validation.status == "pass" else "FAIL"
                    print(f"\n  Status: {status_str}")
                    print(f"  Row count match: {validation.row_count_match}")
                    print(f"    Original rows: {validation.original_row_count}")
                    print(f"    Optimized rows: {validation.optimized_row_count}")

                    print(f"\n  Timing (minimum of {len(validation.original_run_times_ms)} runs):")
                    print(f"    Original:  {validation.original_execution_time_ms:.1f}ms")
                    print(f"    Optimized: {validation.optimized_execution_time_ms:.1f}ms")
                    print(f"    Speedup:   {validation.speedup_ratio:.2f}x")

                    if validation.speedup_ratio > 1:
                        pct = ((validation.speedup_ratio - 1) / validation.speedup_ratio) * 100
                        print(f"    Improvement: {pct:.1f}% faster")
                    elif validation.speedup_ratio < 1:
                        pct = ((1 - validation.speedup_ratio) / 1) * 100
                        print(f"    Regression: {pct:.1f}% slower")

                    # Show all run times
                    print(f"\n  All run times (ms):")
                    print(f"    Original:  {', '.join(f'{t:.1f}' for t in validation.original_run_times_ms)}")
                    print(f"    Optimized: {', '.join(f'{t:.1f}' for t in validation.optimized_run_times_ms)}")

                    if validation.errors:
                        print(f"\n  Errors:")
                        for err in validation.errors[:3]:
                            print(f"    - {err[:200]}")

                    if validation.sample_mismatches:
                        print(f"\n  Sample mismatches:")
                        for mm in validation.sample_mismatches[:3]:
                            print(f"    Row {mm['row']}, Col '{mm['column']}': {mm['original']} vs {mm['optimized']}")

                except Exception as e:
                    print(f"\n  Validation error: {e}")
                    import traceback
                    traceback.print_exc()

        except Exception as e:
            print(f"LLM optimization failed: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
