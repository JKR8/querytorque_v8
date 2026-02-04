"""Optimization JSON schemas."""

OPTIMIZATION_PATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "patches": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["search", "replace"],
                "properties": {
                    "search": {"type": "string"},
                    "replace": {"type": "string"},
                    "description": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        "explanation": {"type": "string"},
    },
    "required": ["patches"],
    "additionalProperties": True,
}

OPTIMIZATION_SQL_SCHEMA = {
    "type": "object",
    "properties": {
        "rewrite_sets": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["id", "transform", "nodes"],
                "properties": {
                    "id": {"type": "string"},
                    "transform": {"type": "string"},
                    "nodes": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                    "invariants_kept": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "expected_speedup": {"type": "string"},
                    "risk": {"type": "string"},
                },
                "additionalProperties": True,
            },
        },
        "explanation": {"type": "string"},
    },
    "required": ["rewrite_sets"],
    "additionalProperties": True,
}
