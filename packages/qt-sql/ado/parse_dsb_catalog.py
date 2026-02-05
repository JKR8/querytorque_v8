#!/usr/bin/env python3
"""Parse PostgreSQL DSB Query Rewrite Rules Catalog and generate JSON examples.

Converts the catalog.txt file into individual rule JSON files for FAISS indexing.
Each rule becomes a searchable example with before_sql, after_sql, and metadata.
"""

import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any

# Paths
BASE_DIR = Path(__file__).resolve().parent
CATALOG_FILE = BASE_DIR / "PostgreSQL DSB Query Rewrite Rules Catalog.txt"
EXAMPLES_DIR = BASE_DIR / "examples"


def extract_yaml_field(text: str, field_name: str) -> str:
    """Extract a YAML field value from text."""
    pattern = rf'^\s*{re.escape(field_name)}:\s*(.+?)$'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        value = match.group(1).strip()
        # Remove quotes if present
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        return value
    return ""


def extract_multiline_field(text: str, field_name: str) -> str:
    """Extract multiline YAML field (with | or >)."""
    # Match field name followed by | or > and capture everything until next field
    pattern = rf'^\s*{re.escape(field_name)}:\s*[|>]\s*\n((?:(?!^\s*\w+:).*\n?)*)'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        return match.group(1).strip()
    return ""


def extract_sql_block(text: str, field_name: str) -> str:
    """Extract SQL block from YAML."""
    # Match field name followed by |, then capture SQL until next field or blank line
    pattern = rf'^\s*{re.escape(field_name)}:\s*\|\s*\n((?:(?!^\s*[a-z_]+:).*\n?)*)'
    match = re.search(pattern, text, re.MULTILINE)
    if match:
        sql = match.group(1).rstrip()
        return sql.strip()
    return ""


def parse_rules_from_catalog(catalog_path: Path) -> List[Dict[str, Any]]:
    """Parse all rules from the DSB catalog file."""
    if not catalog_path.exists():
        print(f"Error: Catalog not found at {catalog_path}")
        return []

    content = catalog_path.read_text(encoding='utf-8')

    # Split by "- id:" to find individual rules
    rule_blocks = re.split(r'(?=\n  - id:)', content)

    rules = []
    for block in rule_blocks:
        if not block.strip().startswith('- id:'):
            continue

        rule = parse_single_rule(block)
        if rule:
            rules.append(rule)

    return rules


def parse_single_rule(block: str) -> Optional[Dict[str, Any]]:
    """Parse a single rule block."""
    try:
        # Extract rule ID
        id_match = re.search(r'^\s*- id:\s*(.+?)$', block, re.MULTILINE)
        if not id_match:
            return None
        rule_id = id_match.group(1).strip()

        # Extract fields
        rule = {
            "id": rule_id,
            "category": extract_yaml_field(block, "category"),
            "description": extract_multiline_field(block, "description"),
            "when_to_apply": extract_multiline_field(block, "when_to_apply"),
            "pg_version": extract_yaml_field(block, "pg_version"),
            "auto_applied_by_pg": extract_yaml_field(block, "auto_applied_by_pg"),
            "performance_impact": extract_yaml_field(block, "performance_impact"),
            "research_source": extract_yaml_field(block, "research_source"),
            "before_sql": extract_sql_block(block, "before_sql"),
            "after_sql": extract_sql_block(block, "after_sql"),
            "alternative_sql": extract_sql_block(block, "alternative_sql"),
            "notes": extract_multiline_field(block, "notes"),
        }

        # Validate that we have at least id and category
        if not rule["category"]:
            print(f"Warning: Rule {rule_id} missing category")
            return None

        # Clean up empty fields
        rule = {k: v for k, v in rule.items() if v}

        return rule

    except Exception as e:
        print(f"Error parsing rule block: {e}")
        return None


def create_example_json(rule: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a parsed rule into example JSON format for FAISS indexing."""
    rule_id = rule["id"]
    category = rule.get("category", "unknown")

    # Determine priority based on category
    is_dsb_specific = category == "dsb_specific"
    priority = "high" if is_dsb_specific else "medium"

    example_json = {
        "id": rule_id,
        "name": rule_id.replace("_", " ").title(),
        "description": rule.get("description", ""),
        "category": category,
        "priority": priority,
        "verified_speedup": rule.get("performance_impact", "unknown"),
        "example": {
            "before_sql": rule.get("before_sql", ""),
            "after_sql": rule.get("after_sql", ""),
            "alternative_sql": rule.get("alternative_sql", ""),
            "key_insight": rule.get("when_to_apply", ""),
            "transforms": [rule_id.lower()],  # The transform is the rule itself
        },
        "metadata": {
            "pg_version": rule.get("pg_version", "All"),
            "auto_applied_by_pg": rule.get("auto_applied_by_pg", "false"),
            "research_source": rule.get("research_source", ""),
            "notes": rule.get("notes", ""),
        }
    }

    return example_json


def main():
    """Parse catalog and generate JSON files."""
    print("=" * 70)
    print("Parsing PostgreSQL DSB Query Rewrite Rules Catalog")
    print("=" * 70)

    # Parse rules
    rules = parse_rules_from_catalog(CATALOG_FILE)
    print(f"\nFound {len(rules)} rules")

    if not rules:
        print("No rules found. Check catalog format.")
        return False

    # Create examples directory
    EXAMPLES_DIR.mkdir(parents=True, exist_ok=True)

    # Generate JSON files
    dsb_specific_count = 0
    successful = 0

    for rule in rules:
        try:
            example_json = create_example_json(rule)

            # Save to file
            output_file = EXAMPLES_DIR / f"{rule['id'].lower()}.json"
            with open(output_file, 'w') as f:
                json.dump(example_json, f, indent=2)

            successful += 1
            category = rule.get("category", "unknown")
            if category == "dsb_specific":
                dsb_specific_count += 1
                marker = " [DSB-SPECIFIC]"
            else:
                marker = ""

            print(f"  ✓ {rule['id']}{marker}")

        except Exception as e:
            print(f"  ✗ {rule['id']}: {e}")

    print("\n" + "=" * 70)
    print(f"Successfully created {successful} example JSON files")
    print(f"  - DSB-specific patterns: {dsb_specific_count}")
    print(f"  - Other patterns: {successful - dsb_specific_count}")
    print(f"\nFiles saved to: {EXAMPLES_DIR}")
    print("=" * 70)

    return successful == len(rules)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
