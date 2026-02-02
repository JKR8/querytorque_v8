"""Registry for SQL semantic rewriters.

Links detection rule IDs to their corresponding rewriter implementations.
Provides lookup, registration, and discovery mechanisms.
"""

from typing import Optional, Type, Any

from .base import BaseRewriter, SchemaMetadata, RewriteResult


# Global registry: rule_id -> rewriter_class
_REWRITER_REGISTRY: dict[str, Type[BaseRewriter]] = {}

# Reverse mapping: rewriter_id -> rewriter_class
_REWRITER_BY_ID: dict[str, Type[BaseRewriter]] = {}


def register_rewriter(rewriter_class: Type[BaseRewriter]) -> Type[BaseRewriter]:
    """Decorator to register a rewriter class.

    Usage:
        @register_rewriter
        class CorrelatedSubqueryToJoinRewriter(BaseRewriter):
            linked_rule_ids = ("SQL-WHERE-007", "SQL-SEL-008")
            ...

    This registers the rewriter for all its linked rule IDs.
    """
    if not rewriter_class.rewriter_id:
        raise ValueError(f"Rewriter {rewriter_class.__name__} has no rewriter_id")

    if not rewriter_class.linked_rule_ids:
        raise ValueError(f"Rewriter {rewriter_class.__name__} has no linked_rule_ids")

    # Register by rewriter_id
    _REWRITER_BY_ID[rewriter_class.rewriter_id] = rewriter_class

    # Register for each linked rule
    for rule_id in rewriter_class.linked_rule_ids:
        if rule_id in _REWRITER_REGISTRY:
            existing = _REWRITER_REGISTRY[rule_id]
            # Allow overwrite in POC - just warn
            pass
        _REWRITER_REGISTRY[rule_id] = rewriter_class

    return rewriter_class


def get_rewriter_for_rule(
    rule_id: str,
    metadata: Optional[SchemaMetadata] = None,
) -> Optional[BaseRewriter]:
    """Get a rewriter instance for a detection rule.

    Args:
        rule_id: The detection rule ID (e.g., "SQL-DUCK-001")
        metadata: Optional schema metadata for safe rewrites

    Returns:
        Rewriter instance or None if no rewriter registered
    """
    rewriter_class = _REWRITER_REGISTRY.get(rule_id)
    if rewriter_class is None:
        return None
    return rewriter_class(metadata=metadata)


def get_rewriter_by_id(
    rewriter_id: str,
    metadata: Optional[SchemaMetadata] = None,
) -> Optional[BaseRewriter]:
    """Get a rewriter instance by its rewriter_id.

    Args:
        rewriter_id: The rewriter's unique ID
        metadata: Optional schema metadata

    Returns:
        Rewriter instance or None if not found
    """
    rewriter_class = _REWRITER_BY_ID.get(rewriter_id)
    if rewriter_class is None:
        return None
    return rewriter_class(metadata=metadata)


def list_registered_rules() -> list[str]:
    """List all rule IDs that have registered rewriters."""
    return list(_REWRITER_REGISTRY.keys())


def list_registered_rewriters() -> list[str]:
    """List all registered rewriter IDs."""
    return list(_REWRITER_BY_ID.keys())


def get_rewriter_class(rule_id: str) -> Optional[Type[BaseRewriter]]:
    """Get the rewriter class for a rule (not instantiated)."""
    return _REWRITER_REGISTRY.get(rule_id)


def has_rewriter(rule_id: str) -> bool:
    """Check if a rule has a registered rewriter."""
    return rule_id in _REWRITER_REGISTRY


def get_coverage_stats() -> dict[str, Any]:
    """Get statistics about rewriter coverage.

    Returns dict with:
    - total_rewriters: Number of registered rewriters
    - total_rules_covered: Number of rules with rewriters
    - rules_by_rewriter: Dict mapping rewriter_id -> list of rule_ids
    - uncovered_patterns: List of pattern families without rewriters
    """
    rules_by_rewriter: dict[str, list[str]] = {}

    for rule_id, rewriter_class in _REWRITER_REGISTRY.items():
        rewriter_id = rewriter_class.rewriter_id
        if rewriter_id not in rules_by_rewriter:
            rules_by_rewriter[rewriter_id] = []
        rules_by_rewriter[rewriter_id].append(rule_id)

    return {
        "total_rewriters": len(_REWRITER_BY_ID),
        "total_rules_covered": len(_REWRITER_REGISTRY),
        "rules_by_rewriter": rules_by_rewriter,
        "rewriter_ids": list(_REWRITER_BY_ID.keys()),
    }


def clear_registry() -> None:
    """Clear all registered rewriters. Mainly for testing."""
    _REWRITER_REGISTRY.clear()
    _REWRITER_BY_ID.clear()


class RewriterChain:
    """Chain multiple rewriters for a detection rule.

    Sometimes a single detection triggers multiple possible rewrites,
    and we want to try them in order of preference.
    """

    def __init__(
        self,
        rewriters: list[BaseRewriter],
        stop_on_success: bool = True,
    ):
        """
        Args:
            rewriters: List of rewriters to try in order
            stop_on_success: If True, stop after first successful rewrite
        """
        self.rewriters = rewriters
        self.stop_on_success = stop_on_success

    def rewrite(self, node, context=None, metadata: Optional[SchemaMetadata] = None):
        """Try each rewriter in sequence."""
        results = []
        for rewriter in self.rewriters:
            if metadata:
                rewriter.metadata = metadata

            if not rewriter.can_rewrite(node, context):
                continue

            result = rewriter.rewrite(node, context)
            results.append(result)

            if result.success and self.stop_on_success:
                return result

        # Return last result or failure
        if results:
            return results[-1]

        return RewriteResult(
            success=False,
            original_sql=node.sql() if hasattr(node, 'sql') else str(node),
            explanation="No rewriter in chain could handle this pattern",
        )
