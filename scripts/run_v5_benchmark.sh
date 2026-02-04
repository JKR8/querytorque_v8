#!/usr/bin/env bash
#
# Run v5 benchmark on TPC-DS queries
#
# Usage:
#   ./scripts/run_v5_benchmark.sh [--all]  # --all to include Q2 and Q9
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check API key
if [ -z "$DEEPSEEK_API_KEY" ]; then
    echo -e "${RED}❌ DEEPSEEK_API_KEY not set${NC}"
    echo "Please run: export DEEPSEEK_API_KEY=your_key_here"
    exit 1
fi

# Determine exclusions
EXCLUDE="2,9"
if [ "$1" == "--all" ]; then
    EXCLUDE=""
    echo -e "${YELLOW}Running ALL queries (including Q2, Q9)${NC}"
else
    echo -e "${YELLOW}Excluding Q2, Q9 (prefilled)${NC}"
    echo "Use --all flag to include them"
fi
echo ""

# Generate timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_CSV="$PROJECT_ROOT/research/experiments/benchmarks/v5_parallel_${TIMESTAMP}.csv"
SUMMARY_FILE="$PROJECT_ROOT/research/experiments/benchmarks/v5_parallel_${TIMESTAMP}_summary.txt"

echo "======================================"
echo "V5 Benchmark Run"
echo "======================================"
echo ""
echo "Timestamp: $TIMESTAMP"
echo "Output CSV: $OUTPUT_CSV"
echo "Summary: $SUMMARY_FILE"
echo ""
echo "Sample DB: /mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
echo "Full DB: /mnt/d/TPC-DS/tpcds_sf100.duckdb"
echo "Workers: 5 (4 coverage + 1 explore)"
echo "Target speedup: 2.0x"
echo ""

# Confirm
read -p "Start benchmark? This may take 4-6 hours. [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 1
fi

# Run benchmark
cd "$PROJECT_ROOT"

START_TIME=$(date +%s)

echo ""
echo "Starting benchmark at $(date)..."
echo ""

if [ -z "$EXCLUDE" ]; then
    python3 packages/qt-sql/scripts/run_v5_benchmark.py \
        --sample-db /mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb \
        --full-db /mnt/d/TPC-DS/tpcds_sf100.duckdb \
        --queries-dir /mnt/d/TPC-DS/queries_duckdb_converted \
        --output-csv "$OUTPUT_CSV" \
        --max-workers 5 \
        --exclude ""
else
    python3 packages/qt-sql/scripts/run_v5_benchmark.py \
        --sample-db /mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb \
        --full-db /mnt/d/TPC-DS/tpcds_sf100.duckdb \
        --queries-dir /mnt/d/TPC-DS/queries_duckdb_converted \
        --output-csv "$OUTPUT_CSV" \
        --max-workers 5 \
        --exclude "$EXCLUDE"
fi

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
ELAPSED_HOURS=$((ELAPSED / 3600))
ELAPSED_MINS=$(((ELAPSED % 3600) / 60))

echo ""
echo "======================================"
echo "Benchmark Complete!"
echo "======================================"
echo ""
echo "Completed at: $(date)"
echo "Total time: ${ELAPSED_HOURS}h ${ELAPSED_MINS}m"
echo "Results saved to: $OUTPUT_CSV"
echo ""

# Generate summary
echo "Generating summary..."

python3 << EOFPYTHON
import pandas as pd
from pathlib import Path

csv_path = Path("$OUTPUT_CSV")
summary_path = Path("$SUMMARY_FILE")

df = pd.read_csv(csv_path)

# Calculate stats
total_queries = len(df)
winners = df['winner_found'].sum()
valid_sample = df[df['valid_sample_count'] > 0]
prefilled = df[df['prefilled'] == True]

# Get top speedups
top_winners = df[df['winner_found'] == True].copy()
if len(top_winners) > 0:
    top_winners['winner_full_speedup'] = pd.to_numeric(top_winners['winner_full_speedup'])
    top_winners = top_winners.nlargest(10, 'winner_full_speedup')
    avg_winner_speedup = top_winners['winner_full_speedup'].mean()
else:
    avg_winner_speedup = 0.0

# Write summary
with summary_path.open('w') as f:
    f.write("V5 Parallel Benchmark - TPC-DS SF100\\n")
    f.write("=" * 50 + "\\n\\n")
    f.write(f"Date: $(date +'%Y-%m-%d %H:%M:%S')\\n")
    f.write(f"Elapsed: ${ELAPSED_HOURS}h ${ELAPSED_MINS}m\\n")
    f.write("Model: DeepSeek V3\\n")
    f.write("Strategy: v5 parallel (5 workers)\\n")
    f.write("Sample DB: tpcds_sf100_sampled_1pct.duckdb (1%)\\n")
    f.write("Full DB: tpcds_sf100.duckdb (SF100)\\n")
    f.write("\\n")
    f.write("Results\\n")
    f.write("-" * 50 + "\\n")
    f.write(f"Total queries: {total_queries}\\n")
    f.write(f"Prefilled: {len(prefilled)}\\n")
    f.write(f"Winners found: {winners}\\n")
    f.write(f"Valid on sample: {len(valid_sample)}\\n")
    f.write(f"Failed: {total_queries - len(valid_sample) - len(prefilled)}\\n")
    f.write(f"Win rate: {winners / (total_queries - len(prefilled)) * 100:.1f}%\\n")
    f.write("\\n")

    if len(top_winners) > 0:
        f.write(f"Top {len(top_winners)} Speedups\\n")
        f.write("-" * 50 + "\\n")
        for idx, row in top_winners.iterrows():
            f.write(f"Q{int(row['query'])}: {row['winner_full_speedup']:.2f}x (worker {int(row['winner_worker'])})\\n")
        f.write("\\n")
        f.write(f"Average speedup (winners only): {avg_winner_speedup:.2f}x\\n")
    else:
        f.write("No winners found\\n")

print(f"Summary saved to: {summary_path}")

# Print summary to console
print("\\n" + summary_path.read_text())
EOFPYTHON

echo ""
echo "Next steps:"
echo "  1. Review results: less $OUTPUT_CSV"
echo "  2. View summary: cat $SUMMARY_FILE"
echo "  3. Update BENCHMARKS.md with these results"
echo ""
