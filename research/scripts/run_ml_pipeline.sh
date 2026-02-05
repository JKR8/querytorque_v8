#!/bin/bash
# Run complete ML pipeline with venv activated

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "========================================"
echo "ML Pipeline Runner"
echo "========================================"

# Check if venv exists
if [ ! -d "$PROJECT_ROOT/.venv" ]; then
    echo "❌ Error: Virtual environment not found at $PROJECT_ROOT/.venv"
    echo "Please create it first: python3 -m venv .venv"
    exit 1
fi

# Activate venv
echo "✓ Activating virtual environment..."
source "$PROJECT_ROOT/.venv/bin/activate"

# Verify activation
if [ -z "$VIRTUAL_ENV" ]; then
    echo "❌ Error: Failed to activate virtual environment"
    exit 1
fi

echo "✓ Virtual environment active: $VIRTUAL_ENV"
echo ""

# Run pipeline steps
echo "========================================"
echo "Step 1/3: Generate Training Data"
echo "========================================"
python3 "$SCRIPT_DIR/generate_ml_training_data.py"

echo ""
echo "========================================"
echo "Step 2/3: Normalize SQL Queries"
echo "========================================"
python3 "$SCRIPT_DIR/normalize_sql.py"

echo ""
echo "========================================"
echo "Step 3/3: Vectorize Queries"
echo "========================================"
python3 "$SCRIPT_DIR/vectorize_queries.py"

echo ""
echo "========================================"
echo "✅ ML Pipeline Complete!"
echo "========================================"
echo ""
echo "Output location: research/ml_pipeline/"
echo "  - data/ml_training_data.csv"
echo "  - data/normalized_queries.json"
echo "  - vectors/query_vectors.npz"
echo "  - vectors/query_vectors_metadata.json"
echo ""
echo "Next: Train models with these datasets"
