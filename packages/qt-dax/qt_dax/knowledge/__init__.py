"""QueryTorque DAX Knowledge Base.

Layered knowledge system for DAX optimization:
  - trials.jsonl: Raw trial records from case studies (12 trials, 1 validated)
  - transforms.json: Structured transform catalog (10 transforms, 7 trial-backed)
  - examples/: Gold examples with verified before/after DAX and timing (5 examples)
  - dax.md: Pathology-based rewrite playbook (distilled from above, 5 pathologies)
  - dax_rules.md: Detection rule catalog (49 rules: 29 DAX + 15 Model + 5 CG)
  - rules/dax/: Individual YAML rule definitions for detection engine

Data flows UP: trials.jsonl → transforms.json → examples/ → dax.md (playbook)
"""

from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent
RULES_DIR = KNOWLEDGE_DIR / "rules"
DAX_RULES_DIR = RULES_DIR / "dax"
EXAMPLES_DIR = KNOWLEDGE_DIR / "examples"
PLAYBOOK_PATH = KNOWLEDGE_DIR / "dax.md"
TRANSFORMS_PATH = KNOWLEDGE_DIR / "transforms.json"
TRIALS_PATH = KNOWLEDGE_DIR / "trials.jsonl"


def load_playbook() -> str:
    """Load the DAX rewrite playbook (equivalent to SQL's load_exploit_algorithm)."""
    return PLAYBOOK_PATH.read_text(encoding="utf-8")


def load_examples() -> list[dict]:
    """Load all gold examples from knowledge/examples/."""
    import json

    examples = []
    if not EXAMPLES_DIR.is_dir():
        return examples
    for path in sorted(EXAMPLES_DIR.glob("*.json")):
        examples.append(json.loads(path.read_text(encoding="utf-8")))
    return examples


def match_examples(
    issue_pathologies: list[str],
    max_examples: int = 2,
) -> list[dict]:
    """Match examples by pathology overlap with detected issues.

    Args:
        issue_pathologies: Pathology IDs detected in the target measure (e.g. ["P1", "P4"]).
        max_examples: Maximum number of examples to return.

    Returns:
        Top-N examples sorted by pathology overlap count (descending).
    """
    if not issue_pathologies:
        return []

    target = set(issue_pathologies)
    scored = []
    for ex in load_examples():
        addressed = set(ex.get("pathologies_addressed", []))
        overlap = len(target & addressed)
        if overlap > 0:
            scored.append((overlap, ex))

    scored.sort(key=lambda t: t[0], reverse=True)
    return [ex for _, ex in scored[:max_examples]]


__all__ = [
    "KNOWLEDGE_DIR",
    "RULES_DIR",
    "DAX_RULES_DIR",
    "EXAMPLES_DIR",
    "PLAYBOOK_PATH",
    "TRANSFORMS_PATH",
    "TRIALS_PATH",
    "load_playbook",
    "load_examples",
    "match_examples",
]
