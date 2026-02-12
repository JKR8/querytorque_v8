"""QueryTorque DAX Knowledge Base.

Layered knowledge system for DAX optimization:
  - dax.md: Pathology-based rewrite playbook (loaded into optimization prompts)
  - dax_rules.md: Detection rule catalog (49 rules: 29 DAX + 15 Model + 5 CG)
  - dax_rulebook.yaml: Confirmed optimization rules with evidence
  - examples/: Gold examples with verified before/after DAX and timing
  - trials.jsonl: Raw trial records from case studies
  - rules/dax/: Individual YAML rule definitions for detection engine

Data flows UP: trials → gold examples → distilled playbook (dax.md)
"""

from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent
RULES_DIR = KNOWLEDGE_DIR / "rules"
DAX_RULES_DIR = RULES_DIR / "dax"
EXAMPLES_DIR = KNOWLEDGE_DIR / "examples"
PLAYBOOK_PATH = KNOWLEDGE_DIR / "dax.md"
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
    "TRIALS_PATH",
    "load_playbook",
]
