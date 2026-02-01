"""Rule registry for AST-based detection.

Collects all rules and provides lookup utilities.
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
