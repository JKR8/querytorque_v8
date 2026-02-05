#!/usr/bin/env python3
"""Check if running in virtual environment and provide helpful message."""

import sys
from pathlib import Path

def check_venv():
    """Verify we're running in a virtual environment."""

    in_venv = hasattr(sys, 'real_prefix') or (
        hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix
    )

    if not in_venv:
        print("❌ ERROR: Not running in virtual environment!")
        print()
        print("Please activate the venv first:")
        print("  source .venv/bin/activate")
        print()
        print("Or use the helper script:")
        print("  ./scripts/run_ml_pipeline.sh")
        sys.exit(1)

    print(f"✓ Virtual environment active: {sys.prefix}")
    return True

if __name__ == "__main__":
    check_venv()
    print("✓ Environment check passed")
