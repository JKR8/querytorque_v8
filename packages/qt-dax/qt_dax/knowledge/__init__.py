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


__all__ = [
    "KNOWLEDGE_DIR",
    "RULES_DIR",
    "DAX_RULES_DIR",
    "EXAMPLES_DIR",
    "PLAYBOOK_PATH",
    "TRANSFORMS_PATH",
    "TRIALS_PATH",
    "load_playbook",
]
