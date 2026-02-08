"""Test DAX optimization against open Power BI Desktop.

Run from Windows PowerShell:
    cd C:/Users/jakc9/Documents/QueryTorque_V8/packages/qt-dax
    python test_optimization.py
"""

import json
import os
import sys

# Add package to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Add ADOMD client DLL path (required for pyadomd)
adomd_path = r"C:\Windows\Microsoft.NET\assembly\GAC_MSIL\Microsoft.AnalysisServices.AdomdClient\v4.0_15.0.0.0__89845dcd8080cc91"
if os.path.exists(adomd_path):
    os.environ["PATH"] = adomd_path + os.pathsep + os.environ.get("PATH", "")
    sys.path.insert(0, adomd_path)
    # Load the .NET assembly via pythonnet
    try:
        import clr
        clr.AddReference("Microsoft.AnalysisServices.AdomdClient")
    except Exception as e:
        print(f"Warning: Could not pre-load ADOMD client: {e}")


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
    from qt_dax.validation import DAXEquivalenceValidator

    # Find PBI Desktop
    print("=" * 60)
    print("DAX OPTIMIZATION TEST")
    print("=" * 60)

    instances = find_pbi_instances()
    if not instances:
        print("ERROR: No Power BI Desktop instances found.")
        print("Please open Power BI Desktop with a model loaded.")
        return 1

    inst = instances[0]
    print(f"\nFound PBI Desktop on port {inst.port}")
    print(f"Workspace: {inst.name}")

    # Load test data
    test_file = os.path.join(os.path.dirname(__file__), "dax_optimizations_extracted.jsonl")
    with open(test_file) as f:
        test_cases = [json.loads(line) for line in f]

    print(f"\nLoaded {len(test_cases)} test cases")

    # Connect and validate
    with PBIDesktopConnection(inst.port) as conn:
        print(f"\nConnected to Power BI Desktop")

        # Get model info
        summary = conn.get_model_summary()
        print(f"Model has {summary['table_count']} tables, {summary['measure_count']} measures")

        validator = DAXEquivalenceValidator(
            connection=conn,
            tolerance=1e-9,
            max_rows_to_compare=10000,
            warmup_runs=2,
        )

        print("\n" + "-" * 60)
        print("VALIDATION RESULTS")
        print("-" * 60)

        results = []
        for i, tc in enumerate(test_cases[:3]):  # Test first 3
            measure = tc["measure_name"]
            original = tc.get("single_measure_dax") or tc.get("original_dax", "")
            optimized = tc.get("optimized_dax", "")

            # Strip leading comments to help detection
            def strip_comments(dax):
                lines = dax.strip().split('\n')
                while lines and lines[0].strip().startswith('//'):
                    lines.pop(0)
                return '\n'.join(lines)

            original = strip_comments(original)
            optimized = strip_comments(optimized)

            if not original or not optimized:
                print(f"\n[{i+1}] {measure}: SKIP (missing DAX)")
                continue

            print(f"\n[{i+1}] {measure}")
            print(f"    Original time (recorded): {tc.get('original_time', 'N/A')}")

            try:
                result = validator.validate(original, optimized)

                status_icon = "PASS" if result.status == "pass" else "FAIL"
                print(f"    Status: {status_icon}")
                print(f"    Original: {result.original_execution_time_ms:.1f}ms")
                print(f"    Optimized: {result.optimized_execution_time_ms:.1f}ms")
                print(f"    Speedup: {result.speedup_ratio:.2f}x")

                if result.errors:
                    for err in result.errors:
                        print(f"    Error: {err}")

                results.append({
                    "measure": measure,
                    "status": result.status,
                    "speedup": result.speedup_ratio,
                    "original_ms": result.original_execution_time_ms,
                    "optimized_ms": result.optimized_execution_time_ms,
                })

            except Exception as e:
                print(f"    ERROR: {e}")
                results.append({
                    "measure": measure,
                    "status": "error",
                    "error": str(e),
                })

        # Summary
        print("\n" + "=" * 60)
        print("SUMMARY")
        print("=" * 60)
        passed = sum(1 for r in results if r.get("status") == "pass")
        failed = sum(1 for r in results if r.get("status") == "fail")
        errors = sum(1 for r in results if r.get("status") == "error")
        print(f"Passed: {passed}, Failed: {failed}, Errors: {errors}")

        if passed > 0:
            avg_speedup = sum(r["speedup"] for r in results if r.get("status") == "pass") / passed
            print(f"Average speedup: {avg_speedup:.2f}x")

    return 0

if __name__ == "__main__":
    sys.exit(main())
