#!/usr/bin/env python3
"""
MIPROv2 Training Script for SQL Query Optimizer

This script trains the DSPy SQL optimizer using MIPROv2 to auto-tune
prompts based on the training set of successful optimizations.

Usage:
    # First create the training set
    python research/scripts/create_training_set.py

    # Then run training
    python research/scripts/train_mipro.py [--auto light|medium|heavy]

Requirements:
    - DEEPSEEK_API_KEY environment variable
    - Training set at research/models/trainset.json
    - Database at D:/TPC-DS/tpcds_sf100_sampled_1pct.duckdb (for validation metric)
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import dspy


def main():
    parser = argparse.ArgumentParser(description="Train DSPy optimizer with MIPROv2")
    parser.add_argument(
        "--trainset",
        type=Path,
        default=Path("research/models/trainset.json"),
        help="Path to training set JSON"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output path for trained model (default: research/models/mipro_<timestamp>.json)"
    )
    parser.add_argument(
        "--auto",
        choices=["light", "medium", "heavy"],
        default="light",
        help="MIPROv2 auto mode: light (fast), medium (balanced), heavy (thorough)"
    )
    parser.add_argument(
        "--max-demos",
        type=int,
        default=3,
        help="Maximum bootstrapped demos"
    )
    parser.add_argument(
        "--max-labeled",
        type=int,
        default=3,
        help="Maximum labeled demos"
    )
    parser.add_argument(
        "--val-split",
        type=float,
        default=0.2,
        help="Validation split ratio"
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"),
        help="Database path for validation metric"
    )
    parser.add_argument(
        "--use-speedup-metric",
        action="store_true",
        help="Use lightweight speedup metric instead of full validation"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Load data but don't run training"
    )

    args = parser.parse_args()

    # Check API key
    if not os.getenv("DEEPSEEK_API_KEY"):
        print("ERROR: Set DEEPSEEK_API_KEY environment variable")
        sys.exit(1)

    # Import after path setup
    from research.scripts.create_training_set import load_trainset
    from qt_sql.optimization.dspy_optimizer import (
        ValidatedOptimizationPipeline,
        create_duckdb_validator,
        create_optimization_metric,
        speedup_metric,
    )

    print("=" * 60)
    print("MIPROv2 Training for SQL Query Optimizer")
    print("=" * 60)
    print(f"Training set: {args.trainset}")
    print(f"Auto mode: {args.auto}")
    print(f"Max demos: {args.max_demos}")
    print(f"Max labeled: {args.max_labeled}")
    print(f"Val split: {args.val_split}")
    print()

    # Load training set
    print("Loading training set...")
    if not args.trainset.exists():
        print(f"ERROR: Training set not found at {args.trainset}")
        print("Run: python research/scripts/create_training_set.py")
        sys.exit(1)

    trainset = load_trainset(args.trainset)
    print(f"Loaded {len(trainset)} examples")

    # Split into train/val
    split_idx = int(len(trainset) * (1 - args.val_split))
    train_examples = trainset[:split_idx]
    val_examples = trainset[split_idx:]

    print(f"Training: {len(train_examples)} examples")
    print(f"Validation: {len(val_examples)} examples")

    if args.dry_run:
        print("\n[DRY RUN] Stopping before training")
        return

    # Configure LM
    print("\nConfiguring DeepSeek LM...")
    lm = dspy.LM(
        "openai/deepseek-chat",
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        api_base="https://api.deepseek.com"
    )
    dspy.configure(lm=lm)

    # Create pipeline
    print("Creating pipeline...")
    if args.use_speedup_metric or not args.db_path.exists():
        print("Using lightweight speedup metric")
        metric = speedup_metric
        validator_fn = None
    else:
        print(f"Using full validation metric with DB: {args.db_path}")
        validator_fn = create_duckdb_validator(str(args.db_path))
        metric = create_optimization_metric(str(args.db_path))

    pipeline = ValidatedOptimizationPipeline(
        validator_fn=validator_fn,
        max_retries=2,
        model_name="deepseek",
        db_name="duckdb",
        use_few_shot=True,
        use_assertions=True  # Assertions are handled internally
    )

    # Configure MIPROv2
    print(f"\nConfiguring MIPROv2 (auto={args.auto})...")
    from dspy.teleprompt import MIPROv2

    optimizer = MIPROv2(
        metric=metric,
        auto=args.auto,
        max_bootstrapped_demos=args.max_demos,
        max_labeled_demos=args.max_labeled,
        verbose=True
    )

    # Run training
    print("\n" + "=" * 60)
    print("Starting MIPROv2 training...")
    print("=" * 60)
    print()

    try:
        optimized_pipeline = optimizer.compile(
            pipeline,
            trainset=train_examples,
            valset=val_examples
        )

        # Save result
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = args.output or Path(f"research/models/mipro_{timestamp}.json")
        output_path.parent.mkdir(parents=True, exist_ok=True)

        optimized_pipeline.save(str(output_path))
        print(f"\n{'=' * 60}")
        print(f"Training complete!")
        print(f"Model saved to: {output_path}")
        print(f"{'=' * 60}")

        # Print some stats
        print("\nTo use the trained model:")
        print(f"""
from qt_sql.optimization.dspy_optimizer import load_pipeline

pipeline = load_pipeline(
    "{output_path}",
    validator_fn=validator,  # Your validator
    model_name="deepseek",
    db_name="duckdb"
)

result = pipeline(query=sql, plan=plan, rows=scans)
""")

    except KeyboardInterrupt:
        print("\n\nTraining interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nERROR during training: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
