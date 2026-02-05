#!/bin/bash
# Run complete ML training pipeline (data prep + model training)

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "========================================"
echo "ML TRAINING PIPELINE"
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

# Install faiss if not present
echo "Checking dependencies..."
python3 -c "import faiss" 2>/dev/null || {
    echo "Installing faiss-cpu..."
    pip install faiss-cpu
}

# ==============================================================================
# PHASE 1: DATA PREPARATION
# ==============================================================================

echo ""
echo "========================================"
echo "PHASE 1: DATA PREPARATION"
echo "========================================"

echo ""
echo "Step 1/3: Generate Training Data"
echo "----------------------------------------"
python3 "$SCRIPT_DIR/generate_ml_training_data.py"

echo ""
echo "Step 2/3: Normalize SQL Queries"
echo "----------------------------------------"
python3 "$SCRIPT_DIR/normalize_sql.py"

echo ""
echo "Step 3/3: Vectorize Queries"
echo "----------------------------------------"
python3 "$SCRIPT_DIR/vectorize_queries.py"

# ==============================================================================
# PHASE 2: ANALYSIS
# ==============================================================================

echo ""
echo "========================================"
echo "PHASE 2: DETECTOR ANALYSIS"
echo "========================================"
python3 "$SCRIPT_DIR/analyze_detector_effectiveness.py"

# ==============================================================================
# PHASE 3: MODEL TRAINING
# ==============================================================================

echo ""
echo "========================================"
echo "PHASE 3: MODEL TRAINING"
echo "========================================"

echo ""
echo "Step 1/2: Train Pattern Weight Matrix"
echo "----------------------------------------"
python3 "$SCRIPT_DIR/train_pattern_weights.py"

echo ""
echo "Step 2/2: Build FAISS Similarity Index"
echo "----------------------------------------"
python3 "$SCRIPT_DIR/train_faiss_index.py"

# ==============================================================================
# SUMMARY
# ==============================================================================

echo ""
echo "========================================"
echo "✅ ML TRAINING COMPLETE!"
echo "========================================"
echo ""
echo "Output location: research/ml_pipeline/"
echo ""
echo "Data:"
echo "  ✓ data/ml_training_data.csv"
echo "  ✓ data/normalized_queries.json"
echo "  ✓ vectors/query_vectors.npz"
echo ""
echo "Analysis:"
echo "  ✓ analysis/detector_effectiveness.json"
echo ""
echo "Models:"
echo "  ✓ models/pattern_weights.json"
echo "  ✓ models/similarity_index.faiss"
echo "  ✓ models/similarity_metadata.json"
echo ""
echo "Usage:"
echo "  from qt_sql.optimization.ml_recommender import load_recommender"
echo "  recommender = load_recommender()"
echo "  recs = recommender.recommend(sql, gold_detections)"
echo ""
