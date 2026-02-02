"""JSON schemas for LLM structured output."""

# Schema for Gemini structured output mode
OPTIMIZATION_PATCH_SCHEMA = {
    "type": "object",
    "properties": {
        "patches": {
            "type": "array",
            "description": "List of patches to apply to the SQL query",
            "items": {
                "type": "object",
                "properties": {
                    "search": {
                        "type": "string",
                        "description": "Exact text from original SQL to find (including whitespace)"
                    },
                    "replace": {
                        "type": "string",
                        "description": "Replacement text"
                    },
                    "description": {
                        "type": "string",
                        "description": "Brief explanation of what this patch fixes"
                    }
                },
                "required": ["search", "replace", "description"]
            }
        },
        "explanation": {
            "type": "string",
            "description": "Summary of the optimization strategy"
        }
    },
    "required": ["patches", "explanation"]
}

# Schema for full SQL rewrite (simpler)
OPTIMIZATION_SQL_SCHEMA = {
    "type": "object",
    "properties": {
        "optimized_sql": {
            "type": "string",
            "description": "The optimized SQL query"
        },
        "explanation": {
            "type": "string",
            "description": "Summary of changes made"
        }
    },
    "required": ["optimized_sql", "explanation"]
}
