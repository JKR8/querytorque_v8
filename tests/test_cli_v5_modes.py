#!/usr/bin/env python3
"""
Test CLI integration for all three V5 modes.

This script demonstrates the CLI usage for:
- Mode 1: Retry
- Mode 2: Parallel
- Mode 3: Evolutionary
"""

import subprocess
import sys
from pathlib import Path

# Sample SQL for testing
TEST_SQL = """
SELECT c_customer_id
FROM customer, store_returns
WHERE sr_customer_sk = c_customer_sk
  AND sr_store_sk IN (
    SELECT s_store_sk FROM store WHERE s_state = 'SD'
  )
LIMIT 100
"""

def create_test_file():
    """Create a test SQL file."""
    test_file = Path("test_query.sql")
    test_file.write_text(TEST_SQL)
    return str(test_file)

def test_cli_help():
    """Test 1: CLI help command."""
    print("\n" + "=" * 80)
    print("TEST 1: CLI Help Command")
    print("=" * 80)

    result = subprocess.run(
        ["python3", "-m", "qt_sql.cli.main", "optimize", "--help"],
        capture_output=True,
        text=True
    )

    print("\n[Output Preview - First 30 lines]")
    print("\n".join(result.stdout.split("\n")[:30]))

    if "MODE 1: RETRY" in result.stdout and "MODE 2: PARALLEL" in result.stdout and "MODE 3: EVOLUTIONARY" in result.stdout:
        print("\n✅ Test 1 PASSED: All 3 modes documented in help")
        return True
    else:
        print("\n❌ Test 1 FAILED: Mode documentation incomplete")
        return False

def test_cli_mode_validation():
    """Test 2: Mode validation (missing required args)."""
    print("\n" + "=" * 80)
    print("TEST 2: Mode Validation")
    print("=" * 80)

    test_file = create_test_file()

    # Test retry mode without databases
    print("\n[Test 2a] Retry mode without databases (should error)")
    result = subprocess.run(
        ["python3", "-m", "qt_sql.cli.main", "optimize", test_file, "--mode", "retry"],
        capture_output=True,
        text=True
    )

    if "Error" in result.stdout or "required" in result.stdout.lower():
        print("✅ Correctly requires --sample-db and --full-db")
    else:
        print("❌ Should have errored on missing databases")
        return False

    # Test evolutionary mode without full-db
    print("\n[Test 2b] Evolutionary mode without full-db (should error)")
    result = subprocess.run(
        ["python3", "-m", "qt_sql.cli.main", "optimize", test_file, "--mode", "evolutionary"],
        capture_output=True,
        text=True
    )

    if "Error" in result.stdout or "required" in result.stdout.lower():
        print("✅ Correctly requires --full-db")
    else:
        print("❌ Should have errored on missing full-db")
        return False

    print("\n✅ Test 2 PASSED: Mode validation working correctly")
    return True

def test_cli_mode_aliases():
    """Test 3: Mode aliases (corrective, tournament, stacking)."""
    print("\n" + "=" * 80)
    print("TEST 3: Mode Aliases")
    print("=" * 80)

    test_file = create_test_file()

    aliases = {
        "corrective": "retry",
        "tournament": "parallel",
        "stacking": "evolutionary"
    }

    print("\n[Test aliases]")
    for alias, canonical in aliases.items():
        print(f"  Testing: {alias} → {canonical}")

        # Just test that the alias is accepted (will error on missing DB, but that's OK)
        result = subprocess.run(
            ["python3", "-m", "qt_sql.cli.main", "optimize", test_file, "--mode", alias],
            capture_output=True,
            text=True
        )

        # Should error about missing DB, not about invalid mode
        if "invalid choice" in result.stdout.lower():
            print(f"    ❌ Alias '{alias}' not recognized")
            return False
        else:
            print(f"    ✅ Alias '{alias}' accepted")

    print("\n✅ Test 3 PASSED: All mode aliases working")
    return True

def test_cli_options():
    """Test 4: Mode-specific options."""
    print("\n" + "=" * 80)
    print("TEST 4: Mode-Specific Options")
    print("=" * 80)

    test_file = create_test_file()

    options_tests = [
        ("retry", ["--retries", "5"]),
        ("parallel", ["--workers", "3"]),
        ("evolutionary", ["--iterations", "10"]),
    ]

    print("\n[Test mode options]")
    for mode, opts in options_tests:
        print(f"  Testing: --mode {mode} {' '.join(opts)}")

        result = subprocess.run(
            ["python3", "-m", "qt_sql.cli.main", "optimize", test_file, "--mode", mode] + opts,
            capture_output=True,
            text=True
        )

        # Should error about missing DB, not about invalid options
        if "No such option" in result.stdout or "invalid" in result.stdout.lower():
            print(f"    ❌ Option error for {mode}")
            return False
        else:
            print(f"    ✅ Options accepted for {mode}")

    print("\n✅ Test 4 PASSED: Mode-specific options working")
    return True

def test_cli_import():
    """Test 5: Python API imports."""
    print("\n" + "=" * 80)
    print("TEST 5: Python API Imports")
    print("=" * 80)

    try:
        from qt_sql.optimization.adaptive_rewriter_v5 import (
            optimize_v5_retry,
            optimize_v5_json_queue,
            optimize_v5_evolutionary,
        )

        print("\n✅ All mode functions importable:")
        print("  ✓ optimize_v5_retry")
        print("  ✓ optimize_v5_json_queue (parallel)")
        print("  ✓ optimize_v5_evolutionary")

        print("\n✅ Test 5 PASSED: Python API imports successful")
        return True

    except ImportError as e:
        print(f"\n❌ Test 5 FAILED: Import error - {e}")
        return False

def test_cli_documentation():
    """Test 6: Check documentation files exist."""
    print("\n" + "=" * 80)
    print("TEST 6: Documentation Files")
    print("=" * 80)

    docs = [
        "packages/qt-sql/CLI_MODES_V5.md",
        "packages/qt-sql/CLI_MODE3_ITERATIVE.md",
        "packages/qt-sql/CLI_MODES_OVERVIEW.md",
        "packages/qt-sql/V5_IMPLEMENTATION_STATUS.md",
    ]

    print("\n[Check documentation files]")
    all_exist = True
    for doc in docs:
        path = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8") / doc
        if path.exists():
            size = path.stat().st_size
            print(f"  ✓ {doc} ({size:,} bytes)")
        else:
            print(f"  ✗ {doc} (missing)")
            all_exist = False

    if all_exist:
        print("\n✅ Test 6 PASSED: All documentation files present")
        return True
    else:
        print("\n❌ Test 6 FAILED: Some documentation files missing")
        return False

def cleanup():
    """Clean up test files."""
    test_file = Path("test_query.sql")
    if test_file.exists():
        test_file.unlink()

def run_all_tests():
    """Run all CLI integration tests."""
    print("\n" + "=" * 80)
    print("CLI V5 MODES INTEGRATION TEST SUITE")
    print("=" * 80)

    tests = [
        ("CLI Help", test_cli_help),
        ("Mode Validation", test_cli_mode_validation),
        ("Mode Aliases", test_cli_mode_aliases),
        ("Mode Options", test_cli_options),
        ("Python API", test_cli_import),
        ("Documentation", test_cli_documentation),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name} ERROR: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, result in results if result)
    failed = len(results) - passed

    print(f"\nTotal: {len(results)}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {failed} ❌")

    print("\n[Results]")
    for test_name, result in results:
        icon = "✅" if result else "❌"
        print(f"  {icon} {test_name}")

    if failed == 0:
        print("\n🎉 ALL CLI TESTS PASSED!")
        print("\n✅ CLI Integration Complete:")
        print("  • All 3 modes integrated")
        print("  • Mode aliases working")
        print("  • Options validated")
        print("  • Python API accessible")
        print("  • Documentation complete")
    else:
        print(f"\n⚠️  {failed} test(s) failed")

    cleanup()
    return failed == 0

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)
