#!/bin/bash
set -e

# PostgreSQL DSB Benchmark Runner with C: Drive Monitoring
# Stops if C: drive space drops below 1GB

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-python3}"

echo "=================================="
echo "PostgreSQL DSB Benchmark Validator"
echo "(with C: Drive Monitoring - stops at 1GB)"
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
echo "📊 Running benchmarks with C: drive monitoring..."
echo "   • 53 queries"
echo "   • 3 runs per query (discard warmup, avg last 2)"
echo "   • Safety threshold: 1GB C: drive space remaining"
echo "   • Checkpoint after each query (resumable)"
echo ""

# Run benchmark
$PYTHON "$SCRIPT_DIR/validate_postgresql_dsb_monitored.py"

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Benchmark complete!"
else
    echo "⚠️  Benchmark interrupted (exit code: $EXIT_CODE)"
    echo "   Results checkpoint saved - can resume by running again"
fi

echo "📁 Results: $SCRIPT_DIR/validation_results/postgresql_dsb_validation.json"
echo "📋 Checkpoint: $SCRIPT_DIR/validation_results/checkpoint.json"
