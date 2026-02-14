#!/usr/bin/env python3
"""build_library.py -- One-time migration/validation script for the Pattern Library.

Reads transforms.json and gold example files, cross-references them with
library.yaml, and reports coverage gaps.

Usage (standalone):
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.patterns.build_library

This script is NOT required for library.yaml to function.  It is a
developer tool for:
  1. Auditing which transforms.json entries have a corresponding library
     pattern.
  2. Auditing which gold example files are referenced by at least one
     pattern.
  3. Detecting broken file references in library.yaml.
  4. Printing a summary of engine/family coverage.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parents[1]  # qt_sql/
_TRANSFORMS_PATH = _PROJECT_ROOT / "knowledge" / "transforms.json"
_LIBRARY_PATH = Path(__file__).resolve().parent / "library.yaml"
_EXAMPLES_DIR = _PROJECT_ROOT / "examples"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _load_transforms() -> List[Dict[str, Any]]:
    """Load transforms.json catalog."""
    if not _TRANSFORMS_PATH.exists():
        print(f"[WARN] transforms.json not found at {_TRANSFORMS_PATH}")
        return []
    with open(_TRANSFORMS_PATH, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _load_library() -> Dict[str, Any]:
    """Load library.yaml."""
    if not _LIBRARY_PATH.exists():
        print(f"[WARN] library.yaml not found at {_LIBRARY_PATH}")
        return {"patterns": []}
    with open(_LIBRARY_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _discover_example_files() -> Dict[str, List[str]]:
    """Discover gold example JSON files by engine.

    Returns a dict mapping engine name to a list of relative paths
    (relative to qt_sql/examples/).
    """
    result: Dict[str, List[str]] = {}
    if not _EXAMPLES_DIR.exists():
        print(f"[WARN] Examples directory not found at {_EXAMPLES_DIR}")
        return result

    for engine_dir in sorted(_EXAMPLES_DIR.iterdir()):
        if not engine_dir.is_dir():
            continue
        engine = engine_dir.name
        files = []
        for json_file in sorted(engine_dir.rglob("*.json")):
            rel = json_file.relative_to(_EXAMPLES_DIR.parent)
            files.append(str(rel))
        if files:
            result[engine] = files
    return result


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_library(
    library: Dict[str, Any],
    transforms: List[Dict[str, Any]],
    example_files: Dict[str, List[str]],
) -> Tuple[List[str], List[str], List[str]]:
    """Cross-reference library against transforms and examples.

    Returns (warnings, transform_gaps, example_gaps).
    """
    warnings: List[str] = []
    transform_gaps: List[str] = []
    example_gaps: List[str] = []

    patterns = library.get("patterns", [])
    pattern_names = {p["name"] for p in patterns}

    # --- Transform coverage ---
    transform_ids = {t["id"] for t in transforms}
    # Many transforms map to a single library pattern (e.g., early_filter,
    # dimension_cte_isolate, multi_dimension_prefetch all map to
    # dimension_cte_isolate or early_filter).  We do a fuzzy check: if a
    # transform id is a substring of any pattern name (or vice versa), it
    # counts as covered.
    for tid in sorted(transform_ids):
        covered = False
        for pname in pattern_names:
            if tid in pname or pname in tid:
                covered = True
                break
            # Also check if they share significant words
            tid_words = set(tid.split("_"))
            pname_words = set(pname.split("_"))
            if len(tid_words & pname_words) >= 2:
                covered = True
                break
        if not covered:
            transform_gaps.append(tid)

    # --- Example file reference validation ---
    all_example_files_flat: Set[str] = set()
    for engine_files in example_files.values():
        all_example_files_flat.update(engine_files)

    referenced_files: Set[str] = set()
    for pat in patterns:
        for ex in pat.get("gold_examples", []):
            ref = ex.get("file", "")
            referenced_files.add(ref)
            # Check if the file actually exists
            full_path = _PROJECT_ROOT / ref
            if not full_path.exists():
                warnings.append(
                    f"Pattern '{pat['name']}' references non-existent file: {ref}"
                )

    # Example files not referenced by any pattern
    for fpath in sorted(all_example_files_flat):
        # Normalize to examples/... path
        if not any(fpath.endswith(r.lstrip("examples/")) or r.endswith(fpath.split("/")[-1]) for r in referenced_files):
            # Check by filename match
            fname = fpath.split("/")[-1]
            if not any(fname in r for r in referenced_files):
                example_gaps.append(fpath)

    # --- Structural validation ---
    families_seen: Set[str] = set()
    engines_seen: Set[str] = set()
    for pat in patterns:
        name = pat.get("name", "<unnamed>")
        if not pat.get("family"):
            warnings.append(f"Pattern '{name}' missing 'family' field")
        else:
            families_seen.add(pat["family"])
        if not pat.get("engine_tags"):
            warnings.append(f"Pattern '{name}' missing 'engine_tags' field")
        else:
            for et in pat["engine_tags"]:
                engines_seen.add(et)
        if not pat.get("motivation_variants"):
            warnings.append(f"Pattern '{name}' missing 'motivation_variants'")
        if not pat.get("gold_examples"):
            warnings.append(f"Pattern '{name}' has no gold_examples")
        if not pat.get("anti_pattern"):
            warnings.append(f"Pattern '{name}' missing 'anti_pattern' description")
        if not pat.get("canonical_rewrite"):
            warnings.append(f"Pattern '{name}' missing 'canonical_rewrite' description")

    return warnings, transform_gaps, example_gaps


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def _print_report(
    library: Dict[str, Any],
    transforms: List[Dict[str, Any]],
    warnings: List[str],
    transform_gaps: List[str],
    example_gaps: List[str],
) -> None:
    """Print a human-readable audit report."""
    patterns = library.get("patterns", [])

    print("=" * 72)
    print("  QueryTorque V8 Pattern Library Audit")
    print("=" * 72)
    print()

    # Summary
    print(f"  Patterns in library:     {len(patterns)}")
    print(f"  Transforms in catalog:   {len(transforms)}")
    print()

    # Family distribution
    family_counts: Dict[str, int] = {}
    for pat in patterns:
        f = pat.get("family", "?")
        family_counts[f] = family_counts.get(f, 0) + 1
    print("  Family distribution:")
    family_labels = {
        "A": "Early Filtering",
        "B": "Decorrelation",
        "C": "Aggregation",
        "D": "Set Ops",
        "E": "Materialization",
        "F": "Join Transform",
    }
    for fam in sorted(family_counts.keys()):
        label = family_labels.get(fam, "Unknown")
        print(f"    {fam} ({label}): {family_counts[fam]}")
    print()

    # Engine distribution
    engine_counts: Dict[str, int] = {}
    for pat in patterns:
        for et in pat.get("engine_tags", []):
            engine_counts[et] = engine_counts.get(et, 0) + 1
    print("  Engine coverage:")
    for eng in sorted(engine_counts.keys()):
        print(f"    {eng}: {engine_counts[eng]}")
    print()

    # Total gold examples
    total_examples = sum(
        len(pat.get("gold_examples", [])) for pat in patterns
    )
    print(f"  Total gold example references: {total_examples}")
    print()

    # Patterns listing
    print("-" * 72)
    print("  Patterns:")
    print("-" * 72)
    for pat in patterns:
        engines = ", ".join(pat.get("engine_tags", []))
        n_examples = len(pat.get("gold_examples", []))
        print(f"    {pat['name']:<35} family={pat.get('family', '?')}  engines=[{engines}]  examples={n_examples}")
    print()

    # Warnings
    if warnings:
        print("-" * 72)
        print(f"  WARNINGS ({len(warnings)}):")
        print("-" * 72)
        for w in warnings:
            print(f"    [!] {w}")
        print()

    # Transform gaps
    if transform_gaps:
        print("-" * 72)
        print(f"  UNCOVERED TRANSFORMS ({len(transform_gaps)}):")
        print("  (transforms.json entries with no matching library pattern)")
        print("-" * 72)
        for tg in transform_gaps:
            print(f"    - {tg}")
        print()

    # Example gaps
    if example_gaps:
        print("-" * 72)
        print(f"  UNREFERENCED EXAMPLE FILES ({len(example_gaps)}):")
        print("  (example JSON files not referenced by any pattern)")
        print("-" * 72)
        for eg in example_gaps:
            print(f"    - {eg}")
        print()

    # Final verdict
    print("=" * 72)
    total_issues = len(warnings) + len(transform_gaps) + len(example_gaps)
    if total_issues == 0:
        print("  PASS: All transforms covered, all examples referenced, no warnings.")
    else:
        print(f"  {total_issues} issue(s) found. Review above for details.")
    print("=" * 72)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Run the audit and print the report. Returns 0 on success, 1 on issues."""
    transforms = _load_transforms()
    library = _load_library()
    example_files = _discover_example_files()

    warnings, transform_gaps, example_gaps = _validate_library(
        library, transforms, example_files
    )

    _print_report(library, transforms, warnings, transform_gaps, example_gaps)

    return 1 if (warnings or transform_gaps or example_gaps) else 0


if __name__ == "__main__":
    sys.exit(main())
