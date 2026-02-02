"""Rule registry for AST-based detection.

Collects all rules and provides lookup utilities.

Rule Classification:
- STYLE rules: Low signal, high noise. Fire too often (SELECT *, implicit join, etc.)
- HIGH-PRECISION rules: DuckDB optimizer exploits, opportunity rules, optimization rules
- By default, only HIGH-PRECISION rules are used for audit. Use include_style=True for all.
"""

from typing import Optional

from .base import ASTRule
from .rules import (
    # SELECT
    SelectStarRule,
    ScalarSubqueryInSelectRule,
    MultipleScalarSubqueriesRule,
    CorrelatedSubqueryInSelectRule,
    DistinctCrutchRule,
    ScalarUDFInSelectRule,
    # WHERE
    FunctionOnColumnRule,
    LeadingWildcardRule,
    NotInSubqueryRule,
    ImplicitTypeConversionRule,
    OrInsteadOfInRule,
    DoubleNegativeRule,
    RedundantPredicateRule,
    CoalesceInFilterRule,
    NonSargableDateRule,
    OrPreventsIndexRule,
    NotInNullRiskRule,
    # JOIN
    CartesianJoinRule,
    ImplicitJoinRule,
    FunctionInJoinRule,
    OrInJoinRule,
    ExpressionInJoinRule,
    InequalityJoinRule,
    TooManyJoinsRule,
    SelfJoinCouldBeWindowRule,
    TriangleJoinPatternRule,
    JoinWithSubqueryCouldBeCTERule,
    TriangularJoinRule,
    # Subquery
    CorrelatedSubqueryInWhereRule,
    SubqueryInsteadOfJoinRule,
    DeeplyNestedSubqueryRule,
    RepeatedSubqueryRule,
    ScalarSubqueryToLateralRule,
    ExistsWithSelectStarRule,
    CorrelatedSubqueryCouldBeWindowRule,
    # UNION
    UnionWithoutAllRule,
    LargeUnionChainRule,
    UnionTypeMismatchRule,
    # ORDER BY
    OrderByInSubqueryRule,
    OrderByWithoutLimitRule,
    OrderByExpressionRule,
    OrderByOrdinalRule,
    OffsetPaginationRule,
    # CTE
    SelectStarInCTERule,
    MultiRefCTERule,
    RecursiveCTERule,
    DeeplyNestedCTERule,
    CTEWithAggregateReusedRule,
    CTEShouldBeSubqueryRule,
    # Window
    RowNumberWithoutOrderRule,
    MultipleWindowPartitionsRule,
    WindowWithoutPartitionRule,
    NestedWindowFunctionRule,
    # Aggregation
    GroupByOrdinalRule,
    HavingWithoutAggregateRule,
    GroupByExpressionRule,
    DistinctInsideAggregateRule,
    MissingGroupByColumnRule,
    RepeatedAggregationRule,
    AggregateOfAggregateRule,
    GroupByWithHavingCountRule,
    LargeCountDistinctRule,
    # Cursor
    CursorUsageRule,
    WhileLoopRule,
    DynamicSQLRule,
    # Data types
    StringNumericComparisonRule,
    DateAsStringRule,
    UnicodeMismatchRule,
    # Fabric
    TableVariableRule,
    TempTableWithoutIndexRule,
    MissingOptionRecompileRule,
    # PostgreSQL
    CountStarInsteadOfExistsRule,
    LargeInListRule,
    CurrentTimestampInWhereRule,
    MissingNullsOrderRule,
    ArrayAggWithoutOrderRule,
    SerialColumnRule,
    JsonbWithoutGinIndexHintRule,
    TextWithoutCollationRule,
    NotUsingLateralRule,
    RandomOrderByRule,
    # DuckDB
    NotUsingQualifyRule,
    NotUsingGroupByAllRule,
    ListAggWithoutOrderRule,
    TempTableInsteadOfCTERule,
    NotUsingSampleRule,
    NotUsingExcludeRule,
    SubqueryInsteadOfPivotRule,
    NotUsingUnpivotRule,
    DirectParquetReadRule,
    NotUsingAsOfJoinRule,
    # DuckDB optimizer weakness exploits
    CrossJoinUnnestWithWhereRule,
    WindowBlocksPredicatePushdownRule,
    ManyJoinsOnParquetRule,
    GroupedTopNPatternRule,
    MissingRedundantJoinFilterRule,
    LargePivotWithoutFilterRule,
    CountDistinctOnOrderedDataRule,
    NestedLoopJoinRiskRule,
    # Optimization rules (from POC rulebook)
    SingleUseCTEInlineRule,
    UnusedCTERule,
    ExistsToSemiJoinRule,
    LeftJoinAntiPatternRule,
    LeftJoinFilterRule,
    NonSargablePredicateRule,
    NotInNullTrapRule,
    HavingNonAggregateRule,
    DistinctGroupByRedundancyRule,
    OptimizationOffsetRule,
    TopNPerGroupRule,
    UnnecessaryDistinctRule,
    OrToUnionRule,
    WindowPushdownRule,
    PreAggregateRule,
    GroupByFunctionalDependencyRule,
    # Optimization opportunity rules (empirical - from TPC-DS wins)
    OrToUnionOpportunity,
    LateDateFilterOpportunity,
    RepeatedSubqueryOpportunity,
    CorrelatedSubqueryOpportunity,
    ImplicitCrossJoinOpportunity,
    CountToExistsOpportunity,
    # Snowflake
    CopyIntoWithoutFileFormatRule,
    SelectWithoutLimitOrSampleRule,
    NonDeterministicInClusteringKeyRule,
    ConsiderClusterByOnFilteredColumnsRule,
    VariantExtractionWithoutCastRule,
    FlattenWithoutLateralRule,
    TimeTravelWithoutRetentionCheckRule,
    CrossDatabaseMetadataWithoutAccountUsageRule,
    VariantPredicateWithoutSearchOptimizationRule,
    GetDdlWithoutSchemaQualificationRule,
    InefficientMicroPartitionPruningRule,
    MissingClusteringKeyMaintenanceRule,
    OverClusteringRule,
    StaleClusteringCheckRule,
    SuboptimalClusteringColumnOrderRule,
    InefficientDatePartitionPruningRule,
    MissingPartitionPruningHintRule,
    CrossPartitionScanRule,
    MissingMaterializedViewCandidateRule,
    StaleMaterializedViewCheckRule,
)


# All registered rules - instantiated once (119 static analysis rules: 71 generic + 20 Snowflake + 10 PostgreSQL + 18 DuckDB)
_ALL_RULES: list[ASTRule] = [
    # SELECT clause rules (6)
    SelectStarRule(),
    ScalarSubqueryInSelectRule(),
    MultipleScalarSubqueriesRule(),
    CorrelatedSubqueryInSelectRule(),
    DistinctCrutchRule(),
    ScalarUDFInSelectRule(),

    # WHERE clause rules (11)
    FunctionOnColumnRule(),
    LeadingWildcardRule(),
    NotInSubqueryRule(),
    ImplicitTypeConversionRule(),
    OrInsteadOfInRule(),
    DoubleNegativeRule(),
    RedundantPredicateRule(),
    CoalesceInFilterRule(),
    NonSargableDateRule(),
    # Structural rewrite patterns
    OrPreventsIndexRule(),
    NotInNullRiskRule(),

    # JOIN rules (11)
    CartesianJoinRule(),
    ImplicitJoinRule(),
    FunctionInJoinRule(),
    OrInJoinRule(),
    ExpressionInJoinRule(),
    InequalityJoinRule(),
    TooManyJoinsRule(),
    # Structural rewrite patterns
    SelfJoinCouldBeWindowRule(),
    TriangleJoinPatternRule(),
    JoinWithSubqueryCouldBeCTERule(),
    TriangularJoinRule(),

    # Subquery rules (7)
    CorrelatedSubqueryInWhereRule(),
    SubqueryInsteadOfJoinRule(),
    DeeplyNestedSubqueryRule(),
    RepeatedSubqueryRule(),
    # Structural rewrite patterns
    ScalarSubqueryToLateralRule(),
    ExistsWithSelectStarRule(),
    CorrelatedSubqueryCouldBeWindowRule(),

    # UNION rules (3)
    UnionWithoutAllRule(),
    LargeUnionChainRule(),
    UnionTypeMismatchRule(),

    # ORDER BY rules (5)
    OrderByInSubqueryRule(),
    OrderByWithoutLimitRule(),
    OrderByExpressionRule(),
    OrderByOrdinalRule(),
    OffsetPaginationRule(),

    # CTE rules (6)
    SelectStarInCTERule(),
    MultiRefCTERule(),
    RecursiveCTERule(),
    DeeplyNestedCTERule(),
    # Structural rewrite patterns
    CTEWithAggregateReusedRule(),
    CTEShouldBeSubqueryRule(),

    # Window function rules (4)
    RowNumberWithoutOrderRule(),
    MultipleWindowPartitionsRule(),
    WindowWithoutPartitionRule(),
    NestedWindowFunctionRule(),

    # Aggregation rules (9)
    GroupByOrdinalRule(),
    HavingWithoutAggregateRule(),
    GroupByExpressionRule(),
    DistinctInsideAggregateRule(),
    MissingGroupByColumnRule(),
    # Structural rewrite patterns
    RepeatedAggregationRule(),
    AggregateOfAggregateRule(),
    GroupByWithHavingCountRule(),
    LargeCountDistinctRule(),

    # Cursor/loop rules (3)
    CursorUsageRule(),
    WhileLoopRule(),
    DynamicSQLRule(),

    # Data type rules (3)
    StringNumericComparisonRule(),
    DateAsStringRule(),
    UnicodeMismatchRule(),

    # Fabric-specific rules (3)
    TableVariableRule(),
    TempTableWithoutIndexRule(),
    MissingOptionRecompileRule(),

    # Snowflake-specific rules (20)
    CopyIntoWithoutFileFormatRule(),
    SelectWithoutLimitOrSampleRule(),
    NonDeterministicInClusteringKeyRule(),
    ConsiderClusterByOnFilteredColumnsRule(),
    VariantExtractionWithoutCastRule(),
    FlattenWithoutLateralRule(),
    TimeTravelWithoutRetentionCheckRule(),
    CrossDatabaseMetadataWithoutAccountUsageRule(),
    VariantPredicateWithoutSearchOptimizationRule(),
    GetDdlWithoutSchemaQualificationRule(),
    InefficientMicroPartitionPruningRule(),
    MissingClusteringKeyMaintenanceRule(),
    OverClusteringRule(),
    StaleClusteringCheckRule(),
    SuboptimalClusteringColumnOrderRule(),
    InefficientDatePartitionPruningRule(),
    MissingPartitionPruningHintRule(),
    CrossPartitionScanRule(),
    MissingMaterializedViewCandidateRule(),
    StaleMaterializedViewCheckRule(),

    # PostgreSQL-specific rules (10)
    CountStarInsteadOfExistsRule(),
    LargeInListRule(),
    CurrentTimestampInWhereRule(),
    MissingNullsOrderRule(),
    ArrayAggWithoutOrderRule(),
    SerialColumnRule(),
    JsonbWithoutGinIndexHintRule(),
    TextWithoutCollationRule(),
    NotUsingLateralRule(),
    RandomOrderByRule(),

    # DuckDB-specific rules (18)
    NotUsingQualifyRule(),
    NotUsingGroupByAllRule(),
    ListAggWithoutOrderRule(),
    TempTableInsteadOfCTERule(),
    NotUsingSampleRule(),
    NotUsingExcludeRule(),
    SubqueryInsteadOfPivotRule(),
    NotUsingUnpivotRule(),
    DirectParquetReadRule(),
    NotUsingAsOfJoinRule(),
    # DuckDB optimizer weakness exploits (SQL-DUCK-011 to 018)
    CrossJoinUnnestWithWhereRule(),
    WindowBlocksPredicatePushdownRule(),
    ManyJoinsOnParquetRule(),
    GroupedTopNPatternRule(),
    MissingRedundantJoinFilterRule(),
    LargePivotWithoutFilterRule(),
    CountDistinctOnOrderedDataRule(),
    NestedLoopJoinRiskRule(),

    # Optimization rules from POC rulebook (16)
    SingleUseCTEInlineRule(),
    UnusedCTERule(),
    ExistsToSemiJoinRule(),
    LeftJoinAntiPatternRule(),
    LeftJoinFilterRule(),
    NonSargablePredicateRule(),
    NotInNullTrapRule(),
    HavingNonAggregateRule(),
    DistinctGroupByRedundancyRule(),
    OptimizationOffsetRule(),
    TopNPerGroupRule(),
    UnnecessaryDistinctRule(),
    OrToUnionRule(),
    WindowPushdownRule(),
    PreAggregateRule(),
    GroupByFunctionalDependencyRule(),

    # Optimization opportunity rules - empirical from TPC-DS wins (6)
    OrToUnionOpportunity(),
    LateDateFilterOpportunity(),
    RepeatedSubqueryOpportunity(),
    CorrelatedSubqueryOpportunity(),
    ImplicitCrossJoinOpportunity(),
    CountToExistsOpportunity(),
]

# Rule lookup by ID
_RULES_BY_ID: dict[str, ASTRule] = {rule.rule_id: rule for rule in _ALL_RULES}

# Rules by category
_RULES_BY_CATEGORY: dict[str, list[ASTRule]] = {}
for _rule in _ALL_RULES:
    if _rule.category not in _RULES_BY_CATEGORY:
        _RULES_BY_CATEGORY[_rule.category] = []
    _RULES_BY_CATEGORY[_rule.category].append(_rule)


def get_all_rules() -> list[ASTRule]:
    """Get all registered detection rules."""
    return list(_ALL_RULES)


def get_rule_by_id(rule_id: str) -> Optional[ASTRule]:
    """Get a specific rule by its ID."""
    return _RULES_BY_ID.get(rule_id)


def get_rules_by_category(category: str) -> list[ASTRule]:
    """Get all rules in a category."""
    return list(_RULES_BY_CATEGORY.get(category, []))


def get_categories() -> list[str]:
    """Get all rule categories."""
    return list(_RULES_BY_CATEGORY.keys())


def get_rule_count() -> int:
    """Get total number of registered rules."""
    return len(_ALL_RULES)


def get_opportunity_rules() -> list[ASTRule]:
    """Get optimization opportunity rules (empirical patterns from TPC-DS).

    These rules detect patterns that are likely to benefit from rewrites,
    based on measured speedups from TPC-DS SF100 benchmarks.
    """
    return get_rules_by_category("optimization_opportunity")


# ============================================================
# Style Rule Classification
# ============================================================

# Rule IDs that are considered "style" rules - low signal, high noise.
# These fire too often on TPC-DS queries (119 rules fire 841 times on 99 queries).
# They are useful for code review but not for optimization assessment.
STYLE_RULE_IDS = {
    # SELECT rules - subjective style preferences
    "SQL-SEL-001",  # SELECT * - too common, not always bad
    "SQL-SEL-005",  # DISTINCT crutch - often intentional
    "SQL-SEL-006",  # Scalar UDF in SELECT - may be necessary

    # JOIN rules - style preferences, not performance issues
    "SQL-JOIN-001",  # Cartesian join - often detected incorrectly
    "SQL-JOIN-002",  # Implicit join - style preference, not bug
    "SQL-JOIN-005",  # Expression in join - sometimes necessary
    "SQL-JOIN-006",  # Inequality join - sometimes necessary

    # Aggregation rules - style preferences
    "SQL-AGG-001",  # GROUP BY ordinal - sometimes cleaner
    "SQL-AGG-003",  # GROUP BY expression - sometimes necessary
    "SQL-AGG-005",  # Missing GROUP BY column - often false positive

    # ORDER BY rules - style preferences
    "SQL-ORD-002",  # ORDER BY without LIMIT - often intentional
    "SQL-ORD-003",  # ORDER BY expression - sometimes necessary
    "SQL-ORD-004",  # ORDER BY ordinal - sometimes cleaner

    # CTE rules - style preferences
    "SQL-CTE-001",  # SELECT * in CTE - sometimes intentional
    "SQL-CTE-004",  # Deeply nested CTE - complexity metric
    "SQL-CTE-006",  # CTE should be subquery - subjective

    # Window rules - style preferences
    "SQL-WIN-003",  # Window without partition - often intentional

    # Type rules - database-specific
    "SQL-TYPE-001",  # String/numeric comparison
    "SQL-TYPE-002",  # Date as string
    "SQL-TYPE-003",  # Unicode mismatch

    # Cursor rules - T-SQL specific
    "SQL-CURSOR-001",  # Cursor usage
    "SQL-CURSOR-002",  # While loop
    "SQL-CURSOR-003",  # Dynamic SQL

    # Fabric rules - SQL Server specific
    "SQL-FAB-001",  # Table variable
    "SQL-FAB-002",  # Temp table without index
    "SQL-FAB-003",  # Missing OPTION RECOMPILE
}


def get_high_precision_rules() -> list[ASTRule]:
    """Get only high-precision rules (excludes noisy style rules).

    Returns rules that:
    - Are DuckDB optimizer exploits (SQL-DUCK-011 to 018)
    - Are optimization opportunity rules (QT-OPT-*)
    - Are structural optimization rules (from POC rulebook)
    - Have proven value from TPC-DS benchmarks

    This is the default set for 'qt-sql audit' to reduce noise.
    """
    return [r for r in _ALL_RULES if r.rule_id not in STYLE_RULE_IDS]


def get_style_rules() -> list[ASTRule]:
    """Get only style rules (the noisy ones).

    These rules are useful for code review but fire too often
    to be useful for optimization assessment.
    """
    return [r for r in _ALL_RULES if r.rule_id in STYLE_RULE_IDS]


def get_rules_for_audit(include_style: bool = False) -> list[ASTRule]:
    """Get rules appropriate for audit command.

    Args:
        include_style: If True, include all rules including noisy style rules.
                       If False (default), only high-precision rules are returned.

    Returns:
        List of ASTRule objects for the audit.
    """
    if include_style:
        return get_all_rules()
    else:
        return get_high_precision_rules()
