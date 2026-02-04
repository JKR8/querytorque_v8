#!/usr/bin/env python3
"""Build FAISS similarity index for query pattern matching.

Output: research/ml_pipeline/models/similarity_index.faiss
"""

import json
import sys
import numpy as np
from pathlib import Path

# Check virtual environment
if not (hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)):
    print("❌ ERROR: Not running in virtual environment!")
    print("Please run: source .venv/bin/activate")
    sys.exit(1)

try:
    import faiss
except ImportError:
    print("❌ ERROR: faiss-cpu not installed")
    print("Install: pip install faiss-cpu")
    sys.exit(1)

BASE = Path(__file__).parent.parent
VECTORS_FILE = BASE / "research" / "ml_pipeline" / "vectors" / "query_vectors.npz"
METADATA_FILE = BASE / "research" / "ml_pipeline" / "vectors" / "query_vectors_metadata.json"
TRAINING_DATA = BASE / "research" / "ml_pipeline" / "data" / "ml_training_data.csv"
OUTPUT_DIR = BASE / "research" / "ml_pipeline" / "models"
OUTPUT_INDEX = OUTPUT_DIR / "similarity_index.faiss"
OUTPUT_METADATA = OUTPUT_DIR / "similarity_metadata.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


class FAISSIndexBuilder:
    """Build FAISS index for query similarity search."""

    def __init__(self, vectors_file: Path, metadata_file: Path, training_data_file: Path):
        self.vectors_data = np.load(vectors_file)
        self.vectors = self.vectors_data["vectors"]
        self.query_ids = self.vectors_data["query_ids"]

        with open(metadata_file) as f:
            self.metadata = json.load(f)

        # Load training data for speedup info
        import csv
        self.speedup_map = {}
        self.transform_map = {}
        with open(training_data_file) as f:
            reader = csv.DictReader(f)
            for row in reader:
                qid = row["query_id"]
                self.speedup_map[qid] = float(row["speedup"])
                self.transform_map[qid] = row["winning_transform"]

    def build(self) -> tuple:
        """Build FAISS index."""
        print("=" * 80)
        print("BUILDING FAISS SIMILARITY INDEX")
        print("=" * 80)
        print(f"Vectors: {self.vectors.shape}")
        print(f"Queries: {len(self.query_ids)}")
        print(f"Dimensions: {self.vectors.shape[1]}")
        print()

        # Normalize vectors for cosine similarity
        # FAISS L2 distance on normalized vectors = cosine distance
        vectors_normalized = self.vectors.astype('float32')
        faiss.normalize_L2(vectors_normalized)

        # Build index
        dimension = vectors_normalized.shape[1]
        index = faiss.IndexFlatL2(dimension)
        index.add(vectors_normalized)

        print(f"✓ Index built with {index.ntotal} vectors")

        # Test index with a few queries
        self._test_index(index, vectors_normalized)

        # Create metadata for retrieval
        metadata = {
            "query_metadata": {},
            "index_stats": {
                "total_vectors": int(index.ntotal),
                "dimensions": dimension,
                "index_type": "IndexFlatL2 (cosine similarity via normalization)",
            }
        }

        for i, qid in enumerate(self.query_ids):
            metadata["query_metadata"][str(qid)] = {
                "vector_index": i,
                "speedup": self.speedup_map.get(qid, 1.0),
                "winning_transform": self.transform_map.get(qid, ""),
                "has_win": self.speedup_map.get(qid, 1.0) >= 1.2,
            }

        return index, metadata

    def _test_index(self, index: faiss.Index, vectors: np.ndarray):
        """Test index with sample queries."""
        print("\n" + "=" * 80)
        print("TESTING INDEX")
        print("=" * 80)

        # Test with queries that have known good speedups
        test_queries = ["q1", "q15", "q93"]  # Known winners

        for test_qid in test_queries:
            if test_qid not in self.query_ids:
                continue

            # Get vector for test query
            idx = np.where(self.query_ids == test_qid)[0][0]
            query_vector = vectors[idx:idx+1]

            # Search for k=6 nearest (first will be itself)
            k = 6
            distances, indices = index.search(query_vector, k)

            print(f"\n{test_qid} (speedup={self.speedup_map.get(test_qid, 1.0):.2f}x, "
                  f"transform={self.transform_map.get(test_qid, 'none')}):")
            print(f"  {'Rank':<6} {'Query':<8} {'Distance':<12} {'Speedup':<10} {'Transform':<20}")
            print("  " + "-" * 70)

            for rank, (dist, idx) in enumerate(zip(distances[0], indices[0])):
                similar_qid = str(self.query_ids[idx])
                speedup = self.speedup_map.get(similar_qid, 1.0)
                transform = self.transform_map.get(similar_qid, "")

                if rank == 0:  # Skip self
                    continue

                print(f"  {rank:<6} {similar_qid:<8} {dist:<12.4f} {speedup:<10.2f}x {transform:<20}")

    def save(self, index: faiss.Index, metadata: dict):
        """Save index and metadata."""
        print("\n" + "=" * 80)
        print("SAVING INDEX")
        print("=" * 80)

        # Save FAISS index
        faiss.write_index(index, str(OUTPUT_INDEX))
        print(f"✓ FAISS index saved: {OUTPUT_INDEX}")

        # Save metadata
        with open(OUTPUT_METADATA, 'w') as f:
            json.dump(metadata, f, indent=2)
        print(f"✓ Metadata saved: {OUTPUT_METADATA}")

        # Print usage instructions
        print("\n" + "=" * 80)
        print("USAGE")
        print("=" * 80)
        print("""
# Load index
import faiss
import json

index = faiss.read_index("research/ml_pipeline/models/similarity_index.faiss")
with open("research/ml_pipeline/models/similarity_metadata.json") as f:
    metadata = json.load(f)

# Query for similar patterns
query_vector = vectorizer.vectorize(sql)  # 90-dim vector
faiss.normalize_L2(query_vector.reshape(1, -1).astype('float32'))
distances, indices = index.search(query_vector.reshape(1, -1), k=5)

# Get metadata for results
for dist, idx in zip(distances[0], indices[0]):
    qid = list(metadata["query_metadata"].keys())[idx]
    info = metadata["query_metadata"][qid]
    print(f"{qid}: {info['speedup']:.2f}x with {info['winning_transform']}")
""")


def main():
    """Build and save FAISS index."""

    # Check dependencies
    if not VECTORS_FILE.exists():
        print(f"Error: Vectors not found at {VECTORS_FILE}")
        print("Run: python scripts/vectorize_queries.py")
        return

    if not METADATA_FILE.exists():
        print(f"Error: Metadata not found at {METADATA_FILE}")
        return

    if not TRAINING_DATA.exists():
        print(f"Error: Training data not found at {TRAINING_DATA}")
        print("Run: python scripts/generate_ml_training_data.py")
        return

    # Build index
    builder = FAISSIndexBuilder(VECTORS_FILE, METADATA_FILE, TRAINING_DATA)
    index, metadata = builder.build()
    builder.save(index, metadata)

    print("\n" + "=" * 80)
    print("✅ FAISS INDEX BUILT!")
    print("=" * 80)


if __name__ == "__main__":
    main()
