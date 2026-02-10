"""Scanner knowledge pipeline: blackboard → findings.

Usage:
  cd <repo-root>
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.scanner_knowledge.build_all \
      packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76

Flags:
  --force-findings   Re-extract findings even if scanner_findings.json exists
  --prompt-only      Print the findings prompt without calling LLM (for review)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Scanner knowledge pipeline: blackboard → findings"
    )
    parser.add_argument(
        "benchmark_dir",
        type=Path,
        help="Path to benchmark directory",
    )
    parser.add_argument(
        "--force-findings",
        action="store_true",
        help="Re-extract findings even if scanner_findings.json exists",
    )
    parser.add_argument(
        "--prompt-only",
        action="store_true",
        help="Print the findings prompt without calling LLM",
    )
    parser.add_argument(
        "--provider",
        type=str,
        default=None,
        help="LLM provider override",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Pass 1 model override",
    )
    parser.add_argument(
        "--structuring-model",
        type=str,
        default=None,
        help="Pass 2 model override",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )

    benchmark_dir = Path(args.benchmark_dir)
    blackboard_path = benchmark_dir / "scanner_blackboard.jsonl"
    findings_path = benchmark_dir / "scanner_findings.json"

    # ── Step 1: Populate blackboard ─────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"  STEP 1: Populate blackboard")
    print(f"{'='*70}\n")

    from .blackboard import populate_blackboard
    blackboard_path = populate_blackboard(benchmark_dir)

    if args.prompt_only:
        print(f"\n{'='*70}")
        print(f"  PROMPT PREVIEW (--prompt-only)")
        print(f"{'='*70}\n")

        from .findings import build_findings_prompt
        prompt = build_findings_prompt(blackboard_path)
        print(prompt)
        return

    # ── Step 2: Extract findings ────────────────────────────────────────
    if findings_path.exists() and not args.force_findings:
        print(f"\n  Findings already exist: {findings_path}")
        print(f"  Use --force-findings to re-extract.")
    else:
        print(f"\n{'='*70}")
        print(f"  STEP 2: Extract findings via LLM")
        print(f"{'='*70}\n")

        from .findings import extract_findings
        findings = extract_findings(
            blackboard_path,
            findings_path,
            provider=args.provider,
            model=args.model,
            structuring_model=args.structuring_model,
        )

        if not findings:
            print("\n  ERROR: No findings extracted. Aborting.")
            sys.exit(1)

        # Print summary for review
        print(f"\n{'='*70}")
        print(f"  FINDINGS SUMMARY (review before proceeding)")
        print(f"{'='*70}\n")

        for f in findings:
            print(f"  {f.id} [{f.confidence}] {f.claim}")
            print(f"    Evidence: {f.evidence_count} supporting, {f.contradicting_count} contradicting")
            if f.boundaries:
                print(f"    Boundary: {f.boundaries[0]}")
            print()

    print(f"\n{'='*70}")
    print(f"  DONE: Scanner knowledge pipeline complete")
    print(f"  Blackboard:  {blackboard_path}")
    print(f"  Findings:    {findings_path}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
