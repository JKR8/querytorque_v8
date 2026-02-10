#!/bin/bash
set -e

# PostgreSQL DSB Benchmark Runner
# Validates 52 ADO-optimized queries against originals

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"

echo "=================================="
echo "PostgreSQL DSB Benchmark Validator"
echo "=================================="
echo ""

# Check database connectivity
echo "🔍 Checking database connectivity..."
if ! $PYTHON << 'EOF'
import psycopg2
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5433,
        database="dsb_sf10",
        user="jakc9",
        password="jakc9"
    )
    conn.close()
    print("✅ Database connection OK")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)
EOF
then
    echo "Please ensure PostgreSQL is running at 127.0.0.1:5433"
    exit 1
fi

echo ""
echo "📊 Running benchmarks (this will take ~1 hour)..."
echo "   • 53 queries"
echo "   • 3 runs per query (original + optimized)"
echo "   • Validation: 3-run method (discard warmup, avg last 2)"
echo ""

# Run benchmark
$PYTHON "$SCRIPT_DIR/validate_postgresql_dsb.py"

echo ""
echo "✅ Benchmark complete!"
echo "📁 Results: $SCRIPT_DIR/validation_results/postgresql_dsb_validation.json"
