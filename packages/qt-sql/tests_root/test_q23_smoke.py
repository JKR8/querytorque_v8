#!/usr/bin/env python3
"""Quick smoke test before running full Q23 test."""

import sys
import os
from pathlib import Path

# Set paths
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

def test_imports():
    """Test that all imports work."""
    print("=" * 80)
    print("SMOKE TEST: Imports")
    print("=" * 80)

    try:
        from qt_sql.optimization.adaptive_rewriter_v5 import (
            optimize_v5_retry,
            optimize_v5_json_queue,
            optimize_v5_evolutionary,
        )
        print("✅ All mode functions imported successfully")
        return True
    except Exception as e:
        print(f"❌ Import failed: {e}")
        return False


def test_environment():
    """Test that environment is configured."""
    print("\n" + "=" * 80)
    print("SMOKE TEST: Environment")
    print("=" * 80)

    api_key = os.getenv('QT_DEEPSEEK_API_KEY') or os.getenv('DEEPSEEK_API_KEY')
    if not api_key:
        print("❌ No API key found")
        return False
    print(f"✅ API key configured: {api_key[:20]}...")

    sample_db = os.getenv('QT_SAMPLE_DB', '/mnt/d/TPC-DS/tpcds_sf100.duckdb')
    if not Path(sample_db).exists():
        print(f"❌ Sample DB not found: {sample_db}")
        return False
    print(f"✅ Sample DB found: {sample_db}")

    full_db = os.getenv('QT_FULL_DB', '/mnt/d/TPC-DS/tpcds_sf100.duckdb')
    if not Path(full_db).exists():
        print(f"❌ Full DB not found: {full_db}")
        return False
    print(f"✅ Full DB found: {full_db}")

    return True


def test_database_query():
    """Test that we can query the database."""
    print("\n" + "=" * 80)
    print("SMOKE TEST: Database Query")
    print("=" * 80)

    try:
        import duckdb

        db_path = os.getenv('QT_SAMPLE_DB', '/mnt/d/TPC-DS/tpcds_sf100.duckdb')
        conn = duckdb.connect(db_path, read_only=True)

        # Simple query
        result = conn.execute("SELECT COUNT(*) as cnt FROM store_sales LIMIT 1").fetchone()
        print(f"✅ Database query successful: store_sales has {result[0]:,} rows")

        conn.close()
        return True

    except Exception as e:
        print(f"❌ Database query failed: {e}")
        return False


def test_llm_connection():
    """Test that we can connect to LLM."""
    print("\n" + "=" * 80)
    print("SMOKE TEST: LLM Connection")
    print("=" * 80)

    try:
        from qt_shared.llm import create_llm_client

        client = create_llm_client(provider='deepseek', model='deepseek-reasoner')
        if client is None:
            print("❌ LLM client is None")
            return False

        print(f"✅ LLM client created: {type(client).__name__}")
        print("✅ LLM connection ready (actual calls will be made during optimization)")

        return True

    except Exception as e:
        print(f"❌ LLM connection failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all smoke tests."""
    print("\n" + "=" * 80)
    print("Q23 SMOKE TEST - Pre-flight checks")
    print("=" * 80)

    tests = [
        ("Imports", test_imports),
        ("Environment", test_environment),
        ("Database", test_database_query),
        ("LLM Connection", test_llm_connection),
    ]

    results = []
    for name, test_func in tests:
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"\n❌ {name} ERROR: {e}")
            results.append((name, False))

    # Summary
    print("\n" + "=" * 80)
    print("SMOKE TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, result in results if result)
    failed = len(results) - passed

    for name, result in results:
        icon = "✅" if result else "❌"
        print(f"  {icon} {name}")

    print(f"\nTotal: {len(results)} | Passed: {passed} | Failed: {failed}")

    if failed == 0:
        print("\n🎉 ALL SMOKE TESTS PASSED!")
        print("\n✅ Ready to run full Q23 test:")
        print("   python3 test_q23_all_modes.py")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed - fix issues before running full test")
        return 1


if __name__ == '__main__':
    sys.exit(main())
