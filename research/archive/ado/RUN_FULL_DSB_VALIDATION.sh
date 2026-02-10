#!/bin/bash
# Full DSB Validation Runner
# Validates all 52 DSB queries on PostgreSQL SF10

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_DIR="$SCRIPT_DIR/batch_results_$TIMESTAMP"

echo "╔════════════════════════════════════════════════════════════╗"
echo "║       DSB PostgreSQL Full Validation (52 Queries)         ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Timestamp:  $TIMESTAMP"
echo "Output Dir: $OUTPUT_DIR"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check PostgreSQL connection
echo "[1/4] Checking PostgreSQL connection..."
if psql -h 127.0.0.1 -p 5433 -U jakc9 -d dsb_sf10 -c "SELECT 1" &>/dev/null; then
    echo "✅ PostgreSQL connection OK"
else
    echo "❌ Cannot connect to PostgreSQL DSB"
    echo "   Start with: docker-compose up -d dsb-postgres"
    exit 1
fi

# Run validation
echo ""
echo "[2/4] Running batch validation (3x: discard warmup, avg last 2)..."
python3 "$SCRIPT_DIR/validate_all_dsb.py" \
    --runs 3 \
    --output "$OUTPUT_DIR/full_results.json" \
    2>&1 | tee "$OUTPUT_DIR/validation.log"

# Check results
echo ""
echo "[3/4] Processing results..."
if [ -f "$OUTPUT_DIR/full_results.json" ]; then
    echo "✅ Results saved: $OUTPUT_DIR/full_results.json"

    # Print summary
    echo ""
    echo "Results Summary:"
    cat "$OUTPUT_DIR/full_results.json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f\"  Total Tested:  {data['discovered']}/{data['total_queries']}\")
print(f\"  Wins (≥1.1x):  {data['wins']}\")
print(f\"  Passes:        {data['passes']}\")
print(f\"  Regressions:   {data['regressions']}\")
print(f\"  Errors:        {data['errors']}\")
print(f\"  Avg Speedup:   {data['average_speedup']:.2f}x\")
" || true
else
    echo "❌ No results file found"
    exit 1
fi

# Generate leaderboard
echo ""
echo "[4/4] Generating leaderboard..."
python3 - << 'PYTHON_EOF'
import json
from pathlib import Path

output_dir = Path("$OUTPUT_DIR")
results_file = output_dir / "full_results.json"

if not results_file.exists():
    print("❌ Results file not found")
    exit(1)

with open(results_file) as f:
    data = json.load(f)

# Sort by speedup descending
results = sorted(
    [r for r in data["results"] if r.get("status") == "PASS"],
    key=lambda x: x.get("speedup", 0),
    reverse=True
)

# Print leaderboard
leaderboard_file = output_dir / "LEADERBOARD.txt"
with open(leaderboard_file, "w") as f:
    f.write("DSB PostgreSQL Leaderboard\n")
    f.write("=" * 100 + "\n")
    f.write(f"Rank | Query ID | Speedup | Status | Original (ms) | Optimized (ms) | Type | Transform\n")
    f.write("-" * 100 + "\n")

    for i, r in enumerate(results, 1):
        speedup = r.get("speedup", 0)
        status = "WIN" if speedup >= 1.1 else "PASS"
        if speedup < 0.95:
            status = "REG"
        f.write(f"{i:3d} | {r.get('query_id', 'N/A'):15s} | {speedup:6.2f}x | {status:6s} | {r.get('original_ms', 0):13.2f} | {r.get('optimized_ms', 0):14.2f} | {r.get('type', 'N/A'):6s} | {', '.join(r.get('transforms', [])[:2])}\n")

print(f"✅ Leaderboard saved: {leaderboard_file}")

PYTHON_EOF

echo ""
echo "╔════════════════════════════════════════════════════════════╗"
echo "║                  Validation Complete!                      ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""
echo "Output files:"
echo "  - $OUTPUT_DIR/full_results.json       (Machine-readable results)"
echo "  - $OUTPUT_DIR/LEADERBOARD.txt         (Human-readable leaderboard)"
echo "  - $OUTPUT_DIR/validation.log          (Detailed log)"
echo ""
echo "Next steps:"
echo "  1. Review LEADERBOARD.txt for top performers"
echo "  2. Compare with research/DSB_LEADERBOARD.md"
echo "  3. Update DSB_LEADERBOARD.md with new results"
