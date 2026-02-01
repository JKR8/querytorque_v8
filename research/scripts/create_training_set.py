#!/usr/bin/env python3
"""
Create Training Set for MIPROv2

Extracts successful optimizations from DSPy experiment runs
to build a training dataset for MIPROv2 prompt optimization.

Usage:
    python research/scripts/create_training_set.py [--results-dir PATH] [--output PATH]
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import List, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import dspy


def extract_from_prompt(prompt_text: str) -> tuple:
    """Extract SQL, plan, and scans from a prompt file.

    Args:
        prompt_text: Content of a prompt file

    Returns:
        Tuple of (sql, plan, scans)
    """
    sql_match = re.search(r'```sql\n(.*?)```', prompt_text, re.DOTALL)
    sql = sql_match.group(1).strip() if sql_match else ""

    plan_match = re.search(
        r'\*\*Operators by cost:\*\*(.*?)(?=\*\*Table scans|\n---)',
        prompt_text,
        re.DOTALL
    )
    plan = plan_match.group(1).strip() if plan_match else ""

    scans_match = re.search(r'\*\*Table scans:\*\*(.*?)(?=\n---)', prompt_text, re.DOTALL)
    scans = scans_match.group(1).strip() if scans_match else ""

    return sql, plan, scans


def create_trainset(
    results_dir: Path,
    prompts_dir: Optional[Path] = None,
    min_speedup: float = 1.0,
    max_examples: Optional[int] = None
) -> List[dspy.Example]:
    """Create training set from experiment results.

    Args:
        results_dir: Directory containing query result folders (q1/, q2/, etc.)
        prompts_dir: Directory containing prompt files (for plan/scans extraction)
        min_speedup: Minimum speedup to include (default 1.0 = all successful)
        max_examples: Maximum number of examples to include

    Returns:
        List of dspy.Example objects
    """
    trainset = []
    results_json = results_dir / "results.json"

    # Load results if available
    speedups = {}
    if results_json.exists():
        with open(results_json) as f:
            results = json.load(f)
            for r in results:
                if r.get("status") == "success":
                    speedups[r["query"]] = r.get("speedup", 1.0)

    # Find all query directories
    query_dirs = sorted(
        [d for d in results_dir.iterdir() if d.is_dir() and d.name.startswith("q")],
        key=lambda x: int(x.name[1:]) if x.name[1:].isdigit() else 0
    )

    print(f"Found {len(query_dirs)} query directories")

    for qdir in query_dirs:
        qname = qdir.name
        optimized_file = qdir / "optimized.sql"
        original_file = qdir / "original.sql"
        rationale_file = qdir / "rationale.txt"

        # Skip if no optimized result
        if not optimized_file.exists():
            continue

        # Check speedup threshold
        speedup = speedups.get(qname, 1.0)
        if speedup < min_speedup:
            continue

        # Read files
        try:
            original_sql = original_file.read_text() if original_file.exists() else ""
            optimized_sql = optimized_file.read_text()
            rationale = rationale_file.read_text() if rationale_file.exists() else ""

            # Try to get plan/scans from prompts
            plan = ""
            scans = ""
            if prompts_dir:
                prompt_file = prompts_dir / f"{qname}_prompt.txt"
                if prompt_file.exists():
                    _, plan, scans = extract_from_prompt(prompt_file.read_text())

            # Create example
            example = dspy.Example(
                original_query=original_sql,
                execution_plan=plan,
                row_estimates=scans,
                optimized_query=optimized_sql,
                optimization_rationale=rationale
            ).with_inputs("original_query", "execution_plan", "row_estimates")

            trainset.append(example)
            print(f"  {qname}: speedup={speedup:.2f}x")

        except Exception as e:
            print(f"  {qname}: ERROR - {e}")
            continue

    # Sort by speedup (highest first) and limit
    # Build a list with speedups attached for sorting
    if speedups:
        # Create (example, speedup) pairs using query name from original_query
        def get_speedup(ex):
            # Try to extract query name from comment in SQL
            import re
            match = re.search(r'query (\d+)', ex.original_query)
            if match:
                qname = f"q{match.group(1)}"
                return speedups.get(qname, 1.0)
            return 1.0

        trainset.sort(key=get_speedup, reverse=True)

    if max_examples:
        trainset = trainset[:max_examples]

    return trainset


def save_trainset(trainset: List[dspy.Example], output_path: Path) -> None:
    """Save training set to JSON.

    Args:
        trainset: List of dspy.Example objects
        output_path: Path to save JSON file
    """
    data = []
    for ex in trainset:
        data.append({
            "original_query": ex.original_query,
            "execution_plan": getattr(ex, "execution_plan", ""),
            "row_estimates": getattr(ex, "row_estimates", ""),
            "optimized_query": ex.optimized_query,
            "optimization_rationale": getattr(ex, "optimization_rationale", "")
        })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\nSaved {len(trainset)} examples to {output_path}")


def load_trainset(input_path: Path) -> List[dspy.Example]:
    """Load training set from JSON.

    Maps stored field names to pipeline forward() argument names:
    - original_query -> query
    - execution_plan -> plan
    - row_estimates -> rows
    - optimized_query -> optimized_sql
    - optimization_rationale -> rationale

    Args:
        input_path: Path to JSON file

    Returns:
        List of dspy.Example objects
    """
    with open(input_path) as f:
        data = json.load(f)

    trainset = []
    for item in data:
        # Map to pipeline's forward() argument names
        example = dspy.Example(
            query=item["original_query"],
            plan=item.get("execution_plan", ""),
            rows=item.get("row_estimates", ""),
            optimized_sql=item["optimized_query"],
            rationale=item.get("optimization_rationale", "")
        ).with_inputs("query", "plan", "rows")
        trainset.append(example)

    return trainset


def main():
    parser = argparse.ArgumentParser(description="Create training set for MIPROv2")
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=Path("research/experiments/dspy_runs/all_20260201_205640"),
        help="Directory containing DSPy run results"
    )
    parser.add_argument(
        "--prompts-dir",
        type=Path,
        default=Path("research/prompts/batch"),
        help="Directory containing prompt files"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("research/models/trainset.json"),
        help="Output path for training set JSON"
    )
    parser.add_argument(
        "--min-speedup",
        type=float,
        default=1.0,
        help="Minimum speedup threshold"
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=None,
        help="Maximum number of examples"
    )

    args = parser.parse_args()

    print(f"Creating training set from: {args.results_dir}")
    print(f"Prompts directory: {args.prompts_dir}")
    print(f"Min speedup: {args.min_speedup}x")
    print(f"Max examples: {args.max_examples or 'unlimited'}")
    print()

    trainset = create_trainset(
        results_dir=args.results_dir,
        prompts_dir=args.prompts_dir,
        min_speedup=args.min_speedup,
        max_examples=args.max_examples
    )

    print(f"\nTotal examples: {len(trainset)}")

    if trainset:
        save_trainset(trainset, args.output)


if __name__ == "__main__":
    main()
