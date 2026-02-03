"""Test Visual Query Analyzer against open Power BI Desktop.

Run from Windows PowerShell:
    cd C:/Users/jakc9/Documents/QueryTorque_V8/packages/qt-dax
    python test_visual_query.py
"""

import json
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

    print("=" * 70)
    print("VISUAL QUERY ANALYZER TEST")
    print("=" * 70)

    # Find PBI Desktop
    instances = find_pbi_instances()
    if not instances:
        print("ERROR: No Power BI Desktop instances found.")
        return 1

    inst = instances[0]
    print(f"\nConnected to PBI Desktop on port {inst.port}")

    # Load a test query from the extracted data
    test_file = os.path.join(os.path.dirname(__file__), "dax_optimizations_extracted.jsonl")
    with open(test_file) as f:
        test_case = json.loads(f.readline())

    query = test_case.get("single_measure_dax") or test_case.get("original_dax", "")

    # Strip leading comments
    lines = query.strip().split('\n')
    while lines and lines[0].strip().startswith('//'):
        lines.pop(0)
    query = '\n'.join(lines)

    print(f"\nTest measure: {test_case.get('measure_name')}")
    print(f"Recorded original time: {test_case.get('original_time')}")

    with PBIDesktopConnection(inst.port) as conn:
        analyzer = VisualQueryAnalyzer(conn)

        print("\n" + "-" * 70)
        print("PARSING QUERY")
        print("-" * 70)

        # Debug: show what we're looking for
        import re
        table_ref_pattern = re.compile(r"'([^']+)'\s*\[\s*([^\]]+)\s*\]")
        print("\nMeasure references found in query:")
        for match in table_ref_pattern.finditer(query):
            table = match.group(1)
            name = match.group(2)
            print(f"  - '{table}'[{name}]")

        result = analyzer.analyze(query)

        print(f"\nInline measures (from query): {len(result.inline_measures)}")
        for m in result.inline_measures[:5]:
            print(f"  - {m.table}[{m.name}]")
        if len(result.inline_measures) > 5:
            print(f"  ... and {len(result.inline_measures) - 5} more")

        print(f"\nModel measures (fetched): {len(result.model_measures)}")
        for m in result.model_measures[:5]:
            print(f"  - {m.table}[{m.name}]")
        if len(result.model_measures) > 5:
            print(f"  ... and {len(result.model_measures) - 5} more")

        print(f"\nTotal measures in dependency chain: {len(result.all_measures)}")
        print(f"Max dependency depth: {result.max_depth}")

        print("\n" + "-" * 70)
        print("DEPENDENCY ORDER (optimize in this order)")
        print("-" * 70)
        for i, key in enumerate(result.dependency_order[:15], 1):
            m = result.all_measures.get(key)
            if m:
                deps = result.measure_dependencies.get(key, set())
                print(f"  {i}. {m.name} (depends on {len(deps)} measures)")
        if len(result.dependency_order) > 15:
            print(f"  ... and {len(result.dependency_order) - 15} more")

        print("\n" + "-" * 70)
        print("FILTER CONTEXT")
        print("-" * 70)
        print(f"Filter tables: {', '.join(result.filter_tables[:10]) or 'None'}")
        print(f"Output columns: {', '.join(result.output_columns[:5]) or 'None'}")

        # Show one measure's expression as example
        if result.inline_measures:
            print("\n" + "-" * 70)
            print("SAMPLE MEASURE EXPRESSION")
            print("-" * 70)
            sample = result.inline_measures[0]
            print(f"Measure: {sample.table}[{sample.name}]")
            print(f"Expression (first 500 chars):")
            print(sample.expression[:500])
            if len(sample.expression) > 500:
                print("...")

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
