#!/usr/bin/env bash
#
# Run V5 parallel test with 20 concurrent workers
#
# Usage:
#   ./scripts/run_v5_20workers.sh [query_number]
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
echo "V5 Parallel - 20 Workers - Q${QUERY}"
echo "======================================"
echo ""

cd "$PROJECT_ROOT"

# Check if packages are installed
if ! python3 -c "from qt_sql.optimization import optimize_v5_json_queue" 2>/dev/null; then
    echo "Installing packages..."
    pip install -e packages/qt-shared packages/qt-sql -q
fi

echo "Running 20 concurrent API calls..."
echo "Validation on 1% sample DB only"
echo "All generations saved incrementally"
echo ""

python3 scripts/test_v5_parallel_20.py "$QUERY"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "✅ Test completed successfully"
    echo ""
    # Find and show the output directory
    LATEST_OUTPUT=$(ls -td research/experiments/v5_parallel_20/q${QUERY}_* 2>/dev/null | head -1)
    if [ -n "$LATEST_OUTPUT" ]; then
        echo "Results: $LATEST_OUTPUT"
        echo ""
        echo "View summary:"
        echo "  cat $LATEST_OUTPUT/summary.txt"
        echo ""
        echo "View all generations:"
        echo "  ls -la $LATEST_OUTPUT/"
        echo ""
        echo "View best generation SQL:"
        BEST=$(jq -r '.best_worker' $LATEST_OUTPUT/summary.json 2>/dev/null)
        if [ -n "$BEST" ] && [ "$BEST" != "null" ]; then
            echo "  cat $LATEST_OUTPUT/gen_$(printf '%02d' $BEST)/optimized.sql"
        fi
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
