#!/usr/bin/env bash
#
# Setup script for v5 benchmark
#
# This script:
# 1. Verifies environment and dependencies
# 2. Tests single query
# 3. Prepares for full benchmark run
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "======================================"
echo "V5 Benchmark Setup"
echo "======================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check API key
echo "Step 1: Checking API key..."
if [ -z "$DEEPSEEK_API_KEY" ]; then
    echo -e "${YELLOW}⚠️  DEEPSEEK_API_KEY not set${NC}"
    echo "Please run: export DEEPSEEK_API_KEY=your_key_here"
    exit 1
else
    echo -e "${GREEN}✅ API key configured${NC}"
fi
echo ""

# Check Python
echo "Step 2: Checking Python..."
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ python3 not found${NC}"
    exit 1
fi
PYTHON_VERSION=$(python3 --version)
echo -e "${GREEN}✅ $PYTHON_VERSION${NC}"
echo ""

# Check packages
echo "Step 3: Checking package installation..."
cd "$PROJECT_ROOT"

if python3 -c "from qt_sql.optimization import optimize_v5_json_queue" 2>/dev/null; then
    echo -e "${GREEN}✅ qt-sql package available${NC}"
else
    echo -e "${YELLOW}⚠️  qt-sql package not installed${NC}"
    echo "Installing in development mode..."
    pip install -e packages/qt-shared packages/qt-sql
    echo -e "${GREEN}✅ Packages installed${NC}"
fi
echo ""

# Check DSPy
echo "Step 4: Checking DSPy..."
if python3 -c "import dspy" 2>/dev/null; then
    echo -e "${GREEN}✅ DSPy available${NC}"
else
    echo -e "${RED}❌ DSPy not found${NC}"
    echo "Installing DSPy..."
    pip install dspy-ai
    echo -e "${GREEN}✅ DSPy installed${NC}"
fi
echo ""

# Check databases
echo "Step 5: Checking databases..."
SAMPLE_DB="/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
FULL_DB="/mnt/d/TPC-DS/tpcds_sf100.duckdb"
QUERIES_DIR="/mnt/d/TPC-DS/queries_duckdb_converted"

if [ -f "$SAMPLE_DB" ]; then
    SAMPLE_SIZE=$(du -h "$SAMPLE_DB" | cut -f1)
    echo -e "${GREEN}✅ Sample DB found ($SAMPLE_SIZE)${NC}"
else
    echo -e "${RED}❌ Sample DB not found: $SAMPLE_DB${NC}"
    exit 1
fi

if [ -f "$FULL_DB" ]; then
    FULL_SIZE=$(du -h "$FULL_DB" | cut -f1)
    echo -e "${GREEN}✅ Full DB found ($FULL_SIZE)${NC}"
else
    echo -e "${RED}❌ Full DB not found: $FULL_DB${NC}"
    exit 1
fi

if [ -d "$QUERIES_DIR" ]; then
    QUERY_COUNT=$(ls -1 "$QUERIES_DIR"/*.sql 2>/dev/null | wc -l)
    echo -e "${GREEN}✅ Query directory found ($QUERY_COUNT queries)${NC}"
else
    echo -e "${RED}❌ Query directory not found: $QUERIES_DIR${NC}"
    exit 1
fi
echo ""

# Create output directory
echo "Step 6: Preparing output directory..."
OUTPUT_DIR="$PROJECT_ROOT/research/experiments/benchmarks"
mkdir -p "$OUTPUT_DIR"
echo -e "${GREEN}✅ Output directory ready: $OUTPUT_DIR${NC}"
echo ""

# Test single query
echo "Step 7: Testing with Query 1..."
TEST_SCRIPT=$(cat << 'EOFPYTHON'
import sys
from pathlib import Path

# Ensure imports work
from qt_sql.optimization import optimize_v5_json_queue

# Load Q1
sql = Path("/mnt/d/TPC-DS/queries_duckdb_converted/query_1.sql").read_text()

print("Running v5 optimization on Q1 (this may take 1-2 minutes)...")

try:
    valid, full_results, winner = optimize_v5_json_queue(
        sql=sql,
        sample_db="/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb",
        full_db="/mnt/d/TPC-DS/tpcds_sf100.duckdb",
        max_workers=5,
        target_speedup=2.0,
    )

    print(f"\n✅ Test successful!")
    print(f"   Valid candidates: {len(valid)}")
    print(f"   Full validations: {len(full_results)}")
    print(f"   Winner found: {bool(winner)}")
    if winner:
        print(f"   Winner speedup: {winner.full_speedup:.2f}x (worker {winner.sample.worker_id})")

    sys.exit(0)

except Exception as e:
    print(f"\n❌ Test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
EOFPYTHON
)

if python3 -c "$TEST_SCRIPT"; then
    echo -e "${GREEN}✅ Single query test passed${NC}"
else
    echo -e "${RED}❌ Single query test failed${NC}"
    exit 1
fi
echo ""

# Summary
echo "======================================"
echo "Setup Complete!"
echo "======================================"
echo ""
echo "You can now run the full benchmark:"
echo ""
echo "  cd $PROJECT_ROOT"
echo "  ./scripts/run_v5_benchmark.sh"
echo ""
echo "Or run manually:"
echo ""
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
echo "  python3 packages/qt-sql/scripts/run_v5_benchmark.py \\"
echo "    --sample-db $SAMPLE_DB \\"
echo "    --full-db $FULL_DB \\"
echo "    --queries-dir $QUERIES_DIR \\"
echo "    --output-csv research/experiments/benchmarks/v5_parallel_${TIMESTAMP}.csv \\"
echo "    --max-workers 5 \\"
echo "    --exclude \"2,9\""
echo ""
