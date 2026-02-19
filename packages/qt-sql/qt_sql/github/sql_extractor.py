"""Extract SQL statements from GitHub PR diffs.

Parses unified diff format to find:
- New/modified .sql files
- SQL in migration files (e.g., alembic, flyway, liquibase)
- Raw SQL strings in Python/JS/TS code (best-effort)
"""

import logging
import re
from typing import List, Dict

logger = logging.getLogger(__name__)

# File patterns that contain SQL
SQL_FILE_PATTERNS = [
    r"\.sql$",
    r"migrations?/.*\.(py|sql)$",
    r"alembic/.*\.py$",
    r"flyway/.*\.sql$",
]

# Regex for extracting SQL from code strings
SQL_STRING_PATTERN = re.compile(
    r'(?:"""|\'\'\')([\s\S]*?(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)[\s\S]*?)(?:"""|\'\'\')',
    re.IGNORECASE,
)


def extract_sql_from_diff(diff_text: str) -> List[Dict[str, str]]:
    """Parse a unified diff and extract SQL content from changed files.

    Args:
        diff_text: Raw unified diff text from GitHub API.

    Returns:
        List of dicts with keys: path, sql, change_type (added/modified).
    """
    if not diff_text or not diff_text.strip():
        return []

    results: List[Dict[str, str]] = []
    current_file = None
    added_lines: List[str] = []
    change_type = "modified"

    for line in diff_text.split("\n"):
        # New file header
        if line.startswith("diff --git"):
            # Flush previous file
            if current_file and added_lines:
                sql = _extract_sql_content(current_file, added_lines)
                if sql:
                    results.append({
                        "path": current_file,
                        "sql": sql,
                        "change_type": change_type,
                    })
            current_file = None
            added_lines = []
            change_type = "modified"

        elif line.startswith("+++ b/"):
            current_file = line[6:]  # Strip "+++ b/" prefix

        elif line.startswith("new file mode"):
            change_type = "added"

        elif line.startswith("+") and not line.startswith("+++"):
            # Added line (strip leading +)
            added_lines.append(line[1:])

    # Flush last file
    if current_file and added_lines:
        sql = _extract_sql_content(current_file, added_lines)
        if sql:
            results.append({
                "path": current_file,
                "sql": sql,
                "change_type": change_type,
            })

    logger.info("Extracted %d SQL files from diff", len(results))
    return results


def _extract_sql_content(file_path: str, lines: List[str]) -> str:
    """Extract SQL from file content based on file type.

    For .sql files, returns the full content.
    For code files, extracts SQL from string literals.
    """
    content = "\n".join(lines).strip()
    if not content:
        return ""

    # Direct .sql files — return full content
    if file_path.endswith(".sql"):
        return content

    # Migration files with SQL
    if _is_sql_file(file_path):
        return _extract_sql_from_code(content)

    return ""


def _is_sql_file(file_path: str) -> bool:
    """Check if a file path matches known SQL file patterns."""
    for pattern in SQL_FILE_PATTERNS:
        if re.search(pattern, file_path, re.IGNORECASE):
            return True
    return False


def _extract_sql_from_code(content: str) -> str:
    """Best-effort extraction of SQL from code string literals.

    Looks for triple-quoted strings containing SQL keywords.
    """
    matches = SQL_STRING_PATTERN.findall(content)
    if not matches:
        # Try single-line strings with SQL keywords
        single_line = re.findall(
            r'["\']([^"\']*(?:SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP)[^"\']*)["\']',
            content,
            re.IGNORECASE,
        )
        matches = [m for m in single_line if len(m) > 20]

    if not matches:
        return ""

    # Return the longest match (most likely the main query)
    return max(matches, key=len).strip()
