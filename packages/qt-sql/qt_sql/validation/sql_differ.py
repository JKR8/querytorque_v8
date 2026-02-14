"""SQL diff utilities for error reporting."""

from __future__ import annotations

from typing import Any, List

from ..schemas import ValueDiff


class SQLDiffer:
    """Generate LLM-friendly SQL diffs."""

    @staticmethod
    def unified_diff(
        original_sql: str,
        rewrite_sql: str,
        context_lines: int = 3,
    ) -> str:
        """Generate unified diff (like git diff).

        Args:
            original_sql: Original SQL
            rewrite_sql: Rewritten SQL
            context_lines: Number of context lines around changes

        Returns:
            Unified diff string
        """
        import difflib

        orig_lines = [line.rstrip() for line in original_sql.split("\n")]
        rewrite_lines = [line.rstrip() for line in rewrite_sql.split("\n")]

        diff = difflib.unified_diff(
            orig_lines,
            rewrite_lines,
            fromfile="original.sql",
            tofile="rewrite.sql",
            lineterm="",
            n=context_lines,
        )
        return "\n".join(diff)

    @staticmethod
    def format_value_diffs(value_diffs: List[ValueDiff], max_per_column: int = 3) -> str:
        """Format value differences grouped by column.

        Args:
            value_diffs: List of ValueDiff objects
            max_per_column: Max diffs to show per column

        Returns:
            Formatted string for LLM consumption
        """
        if not value_diffs:
            return ""

        # Group by column
        by_column: dict[str, List[ValueDiff]] = {}
        for vd in value_diffs:
            if vd.column not in by_column:
                by_column[vd.column] = []
            by_column[vd.column].append(vd)

        lines = []
        for column in sorted(by_column.keys()):
            diffs = by_column[column]
            lines.append(f"\n**{column}** ({len(diffs)} differences):")
            for i, vd in enumerate(diffs[:max_per_column]):
                lines.append(f"  - Row {vd.row_index}:")
                lines.append(f"    Original: {repr(vd.original_value)}")
                lines.append(f"    Rewrite:  {repr(vd.rewrite_value)}")
            if len(diffs) > max_per_column:
                lines.append(f"  ... and {len(diffs) - max_per_column} more")

        return "\n".join(lines)
