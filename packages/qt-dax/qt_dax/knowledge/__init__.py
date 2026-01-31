"""QueryTorque DAX Knowledge Base.

Contains DAX, Model, and Calculation Group rule definitions
for anti-pattern detection and optimization guidance.
"""

from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).parent
RULES_DIR = KNOWLEDGE_DIR / "rules"
DAX_RULES_DIR = RULES_DIR / "dax"

__all__ = ["KNOWLEDGE_DIR", "RULES_DIR", "DAX_RULES_DIR"]
