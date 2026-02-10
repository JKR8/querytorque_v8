#!/usr/bin/env bash
#
# Run V5 benchmark on all 99 TPC-DS queries with 20 workers each
#
# Usage:
#   ./research/benchmarks/qt-sql/scripts/run_benchmark_all.sh [--start-from N] [--end-at N]
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

# Read API key
if [ -z "$DEEPSEEK_API_KEY" ]; then
    if [ -f "$PROJECT_ROOT/DeepseekV3.txt" ]; then
        export DEEPSEEK_API_KEY=$(cat "$PROJECT_ROOT/DeepseekV3.txt" | tr -d '\n\r')
        echo "✅ Loaded API key from DeepseekV3.txt"
    else
        echo "❌ DEEPSEEK_API_KEY not set and DeepseekV3.txt not found"
        exit 1
    fi
fi

cd "$PROJECT_ROOT"

# Check packages
if ! python3 -c "from qt_sql.optimization import optimize_v5_json_queue" 2>/dev/null; then
    echo "Installing packages..."
    pip install -e packages/qt-shared packages/qt-sql -q
fi

echo ""
echo "======================================"
echo "V5 Benchmark - All 99 Queries"
echo "======================================"
echo ""
echo "Configuration:"
echo "  - 20 workers per query"
echo "  - 99 queries total"
echo "  - 1,980 total LLM API calls"
echo "  - Validation on 1% sample DB only"
echo "  - All generations saved"
echo ""
echo "Expected time: 1-2 hours"
echo ""

read -p "Start benchmark? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 1
fi

echo ""
echo "Starting benchmark at $(date)..."
echo ""

START_TIME=$(date +%s)

# Run benchmark
python3 "$SCRIPT_DIR/benchmark_v5_all_queries.py" "$@"

EXIT_CODE=$?
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
HOURS=$((ELAPSED / 3600))
MINS=$(((ELAPSED % 3600) / 60))

echo ""
echo "======================================"
echo "Benchmark finished at $(date)"
echo "Total time: ${HOURS}h ${MINS}m"
echo "======================================"
echo ""

if [ $EXIT_CODE -eq 0 ]; then
    LATEST_RUN=$(ls -td research/experiments/v5_benchmark_20workers/run_* 2>/dev/null | head -1)
    if [ -n "$LATEST_RUN" ]; then
        echo "Results:"
        echo "  CSV: $LATEST_RUN/results.csv"
        echo "  Full: $LATEST_RUN/"
        echo ""
        echo "Quick summary:"
        cat "$LATEST_RUN/final_summary.json" 2>/dev/null | jq -r '
            "  Queries: \(.successful)/\(.total_queries) successful",
            "  Valid gens: \(.total_valid)/\(.total_workers) (\(.valid_rate)%)",
            "  Best speedup: \(.best_overall.best_speedup)x (Q\(.best_overall.query))",
            "  Avg best: \(.avg_best_speedup)x"
        ' 2>/dev/null || echo "  (Summary not available)"
        echo ""
    fi
fi

exit $EXIT_CODE
