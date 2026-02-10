#!/usr/bin/env python3
"""
Update all 99 state history YAMLs with DSR1 (deepseek-reasoner round 1) results.

Reads:
- research/state/validation/qN_validation.json (speedup, status)
- research/state/responses/qN_response.txt (extract transforms from JSON)

Writes:
- research/state_histories_all_99/qN_state_history.yaml (appends dsr1 state)
"""

import json
import re
import yaml
from pathlib import Path

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
VALIDATION_DIR = PROJECT / "research" / "state" / "validation"
RESPONSES_DIR = PROJECT / "research" / "state" / "responses"
STATE_DIR = PROJECT / "research" / "state_histories_all_99"


def extract_transforms_from_response(response_text: str) -> list:
    """Extract transform names from a DSR1 response JSON."""
    # Find JSON block in response
    json_match = re.search(r'```json\s*(\{.*?\})\s*```', response_text, re.DOTALL)
    if not json_match:
        # Try without code fences
        json_match = re.search(r'(\{\s*"rewrite_sets".*\})', response_text, re.DOTALL)

    if not json_match:
        return []

    try:
        data = json.loads(json_match.group(1))
        transforms = []
        for rs in data.get("rewrite_sets", []):
            t = rs.get("transform", "")
            if t and t not in transforms:
                transforms.append(t)
        return transforms
    except json.JSONDecodeError:
        # Try to extract transform field with regex as fallback
        transform_matches = re.findall(r'"transform"\s*:\s*"([^"]+)"', response_text)
        return list(dict.fromkeys(transform_matches))  # dedupe preserving order


def main():
    updated = 0
    skipped = 0
    errors = 0

    for q in range(1, 100):
        # Load validation result
        val_path = VALIDATION_DIR / f"q{q}_validation.json"
        if not val_path.exists():
            print(f"Q{q}: SKIP - no validation file")
            skipped += 1
            continue

        val = json.loads(val_path.read_text())
        speedup = val.get("speedup", 1.0)
        status_raw = val.get("status", "unknown")

        # Map validation status to standard classification
        if status_raw in ("error", "parse_error"):
            status = "error"
        elif speedup >= 1.1:
            status = "success"
        elif speedup >= 0.95:
            status = "neutral"
        else:
            status = "regression"

        # Extract transforms from response
        resp_path = RESPONSES_DIR / f"q{q}_response.txt"
        transforms = []
        if resp_path.exists():
            transforms = extract_transforms_from_response(resp_path.read_text())

        # Load existing YAML
        yaml_path = STATE_DIR / f"q{q}_state_history.yaml"
        if not yaml_path.exists():
            print(f"Q{q}: SKIP - no state YAML")
            skipped += 1
            continue

        with open(yaml_path) as f:
            data = yaml.safe_load(f)

        # Check if dsr1 already exists
        existing_ids = [s.get("state_id") for s in data.get("states", [])]
        if "dsr1" in existing_ids:
            print(f"Q{q}: SKIP - dsr1 already in YAML")
            skipped += 1
            continue

        # Build new state entry
        dsr1_state = {
            "state_id": "dsr1",
            "speedup": round(speedup, 4),
            "model": "deepseek-reasoner",
            "transforms": transforms if transforms else ["unknown"],
            "status": status,
            "error": None if status != "error" else f"DSR1 {status_raw}",
            "description": "DeepSeek Reasoner round 1",
        }

        # Append to states
        data["states"].append(dsr1_state)

        # Update best_speedup
        current_best = data.get("best_speedup", 1.0)
        if speedup > current_best:
            data["best_speedup"] = round(speedup, 4)

        # Update classification based on best speedup
        best = data.get("best_speedup", 1.0)
        if best >= 1.5:
            data["classification"] = "WIN"
        elif best >= 1.1:
            data["classification"] = "IMPROVED"
        elif best >= 0.95:
            data["classification"] = "NEUTRAL"
        else:
            data["classification"] = "REGRESSION"

        # Write updated YAML
        with open(yaml_path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)

        t_str = ", ".join(transforms) if transforms else "none"
        print(f"Q{q}: {speedup:.2f}x [{t_str}] {status} -> UPDATED")
        updated += 1

    print(f"\nDone: {updated} updated, {skipped} skipped, {errors} errors")


if __name__ == "__main__":
    main()
