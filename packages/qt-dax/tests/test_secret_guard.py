"""Security guard tests for accidental key commits."""

from pathlib import Path
import re


def test_no_hardcoded_openai_style_keys_in_python_files():
    root = Path(__file__).resolve().parents[1]
    python_files = list(root.rglob("*.py"))
    # Match quoted key literals such as "sk-abc123..."
    key_literal_re = re.compile(r"""["']sk-[A-Za-z0-9]{20,}["']""")

    offenders: list[str] = []
    for path in python_files:
        text = path.read_text(encoding="utf-8")
        if key_literal_re.search(text):
            offenders.append(str(path.relative_to(root)))

    assert offenders == [], f"Hardcoded key-like literals found: {offenders}"

