"""Base classes for SQL semantic rewriters.

Rewriters transform SQL queries based on detected anti-patterns.
Each rewriter is linked to a detection rule and provides the actual
transformation logic that the detection rule identifies as needed.

Key concepts:
- Rewriters operate on sqlglot AST nodes
- Safety checks validate semantic equivalence
- Metadata (schema, constraints) enables safe rewrites
- Confidence levels indicate rewrite reliability
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Any

from sqlglot import exp


class RewriteConfidence(Enum):
    """Confidence level for a rewrite."""
    HIGH = "high"       # Proven equivalent (e.g., OR chain -> IN)
    MEDIUM = "medium"   # Likely equivalent, needs validation
    LOW = "low"         # May change semantics, requires review
    UNSAFE = "unsafe"   # Known semantic change (e.g., removes DISTINCT)


class SafetyCheckResult(Enum):
    """Result of a safety check."""
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"  # Missing metadata to check
    WARNING = "warning"  # Passed but with caveats


@dataclass
class SafetyCheck:
    """Individual safety check result."""
    name: str
    result: SafetyCheckResult
    message: str
    metadata_required: Optional[list[str]] = None

    @property
    def passed(self) -> bool:
        return self.result in (SafetyCheckResult.PASSED, SafetyCheckResult.SKIPPED)


@dataclass
class SchemaMetadata:
    """Schema information for safe rewrites.

    Semantic rewrites often require knowledge about:
    - Primary keys (for unique row identification)
    - Foreign keys (for join safety)
    - NOT NULL constraints (for outer join safety)
    - Unique constraints (for DISTINCT elimination)
    - Indexes (for performance considerations)
    """
    tables: dict[str, "TableMetadata"] = field(default_factory=dict)

    def get_table(self, name: str) -> Optional["TableMetadata"]:
        """Get table metadata by name (case-insensitive)."""
        name_lower = name.lower()
        return self.tables.get(name_lower)

    def has_primary_key(self, table: str, column: str) -> bool:
        """Check if column is part of primary key."""
        t = self.get_table(table)
        return t is not None and column.lower() in [c.lower() for c in t.primary_key]

    def has_unique_constraint(self, table: str, columns: list[str]) -> bool:
        """Check if columns have a unique constraint."""
        t = self.get_table(table)
        if not t:
            return False
        cols_lower = set(c.lower() for c in columns)
        for unique_cols in t.unique_constraints:
            if set(c.lower() for c in unique_cols) == cols_lower:
                return True
        return False

    def get_foreign_key_target(self, table: str, column: str) -> Optional[tuple[str, str]]:
        """Get the target table.column for a foreign key."""
        t = self.get_table(table)
        if not t:
            return None
        return t.foreign_keys.get(column.lower())


@dataclass
class TableMetadata:
    """Metadata for a single table."""
    name: str
    columns: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    foreign_keys: dict[str, tuple[str, str]] = field(default_factory=dict)
    unique_constraints: list[list[str]] = field(default_factory=list)
    not_null_columns: list[str] = field(default_factory=list)
    indexed_columns: list[list[str]] = field(default_factory=list)


@dataclass
class RewriteResult:
    """Result of a rewrite operation."""
    success: bool
    original_sql: str
    rewritten_sql: Optional[str] = None
    rewritten_node: Optional[exp.Expression] = None
    confidence: RewriteConfidence = RewriteConfidence.MEDIUM
    safety_checks: list[SafetyCheck] = field(default_factory=list)
    explanation: str = ""
    rule_id: str = ""
    rewriter_id: str = ""

    @property
    def all_safety_checks_passed(self) -> bool:
        """Check if all safety checks passed."""
        return all(check.passed for check in self.safety_checks)

    @property
    def has_warnings(self) -> bool:
        """Check if any safety checks have warnings."""
        return any(check.result == SafetyCheckResult.WARNING for check in self.safety_checks)

    def add_safety_check(
        self,
        name: str,
        result: SafetyCheckResult,
        message: str,
        metadata_required: Optional[list[str]] = None,
    ) -> None:
        """Add a safety check result."""
        self.safety_checks.append(SafetyCheck(
            name=name,
            result=result,
            message=message,
            metadata_required=metadata_required,
        ))


class BaseRewriter(ABC):
    """Base class for SQL semantic rewriters.

    Subclasses must implement:
    - rewriter_id: Unique identifier for the rewriter
    - linked_rule_ids: List of detection rule IDs this rewriter handles
    - rewrite(): The actual transformation logic
    - get_safety_checks(): Required safety validations

    Optional overrides:
    - can_rewrite(): Pre-check if rewrite is possible
    - get_required_metadata(): Metadata needed for safe rewrite
    """

    # Rewriter metadata - override in subclasses
    rewriter_id: str = ""
    name: str = ""
    description: str = ""

    # Detection rules this rewriter handles
    linked_rule_ids: tuple[str, ...] = ()

    # Default confidence for this rewriter type
    default_confidence: RewriteConfidence = RewriteConfidence.MEDIUM

    def __init__(self, metadata: Optional[SchemaMetadata] = None):
        """Initialize rewriter with optional schema metadata."""
        self.metadata = metadata or SchemaMetadata()

    @abstractmethod
    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Perform the rewrite transformation.

        Args:
            node: The AST node to rewrite
            context: Optional context (e.g., ASTContext from detection)

        Returns:
            RewriteResult with transformed SQL and safety info
        """
        ...

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if this rewriter can handle the given node.

        Override for complex pre-conditions. Default returns True.
        """
        return True

    def get_required_metadata(self) -> list[str]:
        """List metadata types required for safe rewrite.

        Returns:
            List of required metadata types (e.g., ['primary_key', 'foreign_keys'])
        """
        return []

    def has_required_metadata(self) -> bool:
        """Check if required metadata is available."""
        required = self.get_required_metadata()
        if not required:
            return True
        # Check metadata availability
        if 'primary_key' in required:
            if not any(t.primary_key for t in self.metadata.tables.values()):
                return False
        if 'foreign_keys' in required:
            if not any(t.foreign_keys for t in self.metadata.tables.values()):
                return False
        return True

    def _create_result(
        self,
        success: bool,
        original_sql: str,
        rewritten_sql: Optional[str] = None,
        rewritten_node: Optional[exp.Expression] = None,
        confidence: Optional[RewriteConfidence] = None,
        explanation: str = "",
    ) -> RewriteResult:
        """Helper to create a RewriteResult."""
        return RewriteResult(
            success=success,
            original_sql=original_sql,
            rewritten_sql=rewritten_sql,
            rewritten_node=rewritten_node,
            confidence=confidence or self.default_confidence,
            explanation=explanation,
            rule_id=self.linked_rule_ids[0] if self.linked_rule_ids else "",
            rewriter_id=self.rewriter_id,
        )

    def _create_failure(self, original_sql: str, reason: str) -> RewriteResult:
        """Helper to create a failed RewriteResult."""
        return self._create_result(
            success=False,
            original_sql=original_sql,
            explanation=f"Rewrite failed: {reason}",
        )


class CompositeRewriter(BaseRewriter):
    """Rewriter that combines multiple rewriters.

    Useful for patterns that require multiple transformations
    or for creating rewriter pipelines.
    """

    def __init__(
        self,
        rewriters: list[BaseRewriter],
        metadata: Optional[SchemaMetadata] = None,
    ):
        super().__init__(metadata)
        self.rewriters = rewriters

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Apply rewriters in sequence."""
        current_node = node
        original_sql = node.sql()
        all_checks: list[SafetyCheck] = []
        explanations: list[str] = []
        min_confidence = RewriteConfidence.HIGH

        for rewriter in self.rewriters:
            if not rewriter.can_rewrite(current_node, context):
                continue

            result = rewriter.rewrite(current_node, context)
            if not result.success:
                return result  # Fail fast

            all_checks.extend(result.safety_checks)
            explanations.append(result.explanation)

            # Track lowest confidence
            confidence_order = {
                RewriteConfidence.HIGH: 4,
                RewriteConfidence.MEDIUM: 3,
                RewriteConfidence.LOW: 2,
                RewriteConfidence.UNSAFE: 1,
            }
            if confidence_order.get(result.confidence, 0) < confidence_order.get(min_confidence, 0):
                min_confidence = result.confidence

            # Use rewritten node for next iteration
            if result.rewritten_node:
                current_node = result.rewritten_node

        return RewriteResult(
            success=True,
            original_sql=original_sql,
            rewritten_sql=current_node.sql(),
            rewritten_node=current_node,
            confidence=min_confidence,
            safety_checks=all_checks,
            explanation=" -> ".join(explanations),
            rewriter_id="composite",
        )
