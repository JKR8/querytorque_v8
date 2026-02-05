#!/usr/bin/env bash
#
# Quick test runner for v5 single query
#
# Usage:
#   ./scripts/run_v5_test.sh [query_number]
#
# Examples:
#   ./scripts/run_v5_test.sh 1       # Test query 1
#   ./scripts/run_v5_test.sh 15      # Test query 15
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Read API key from file if not set
if [ -z "$DEEPSEEK_API_KEY" ]; then
    if [ -f "$PROJECT_ROOT/DeepseekV3.txt" ]; then
        export DEEPSEEK_API_KEY=$(cat "$PROJECT_ROOT/DeepseekV3.txt" | tr -d '\n\r')
        echo "✅ Loaded API key from DeepseekV3.txt"
    else
        echo "❌ DEEPSEEK_API_KEY not set and DeepseekV3.txt not found"
        exit 1
    fi
fi

# Query number (default: 1)
QUERY=${1:-1}

echo "======================================"
echo "V5 Single Query Test - Q${QUERY}"
echo "======================================"
echo ""

cd "$PROJECT_ROOT"

# Check if packages are installed
if ! python3 -c "from qt_sql.optimization import optimize_v5_json_queue" 2>/dev/null; then
    echo "Installing packages..."
    pip install -e packages/qt-shared packages/qt-sql -q
fi

echo "Running robust test with incremental output saving..."
echo "All worker outputs will be saved as they complete."
echo ""

python3 scripts/test_v5_single_query_robust.py "$QUERY"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ Test completed successfully"
    echo ""
    # Find and show the output directory
    LATEST_OUTPUT=$(ls -td research/experiments/v5_test_runs/q${QUERY}_* 2>/dev/null | head -1)
    if [ -n "$LATEST_OUTPUT" ]; then
        echo "Results saved to: $LATEST_OUTPUT"
        echo ""
        echo "View summary:"
        echo "  cat $LATEST_OUTPUT/summary.txt"
        echo ""
        echo "Explore worker outputs:"
        echo "  ls -la $LATEST_OUTPUT/"
    fi
elif [ $EXIT_CODE -eq 130 ]; then
    echo ""
    echo "⚠️  Test interrupted (Ctrl+C)"
    echo "Partial results have been saved."
else
    echo ""
    echo "❌ Test failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE
