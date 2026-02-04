"""AST detection rules organized by category."""

# SELECT rules
from .select_rules import (
    SelectStarRule,
    ScalarSubqueryInSelectRule,
    MultipleScalarSubqueriesRule,
    CorrelatedSubqueryInSelectRule,
    DistinctCrutchRule,
    ScalarUDFInSelectRule,
)

# WHERE rules
from .where_rules import (
    FunctionOnColumnRule,
    LeadingWildcardRule,
    NotInSubqueryRule,
    ImplicitTypeConversionRule,
    OrInsteadOfInRule,
    DoubleNegativeRule,
    RedundantPredicateRule,
    CoalesceInFilterRule,
    NonSargableDateRule,
    # Structural rewrite patterns
    OrPreventsIndexRule,
    NotInNullRiskRule,
)

# JOIN rules
from .join_rules import (
    CartesianJoinRule,
    ImplicitJoinRule,
    FunctionInJoinRule,
    OrInJoinRule,
    ExpressionInJoinRule,
    InequalityJoinRule,
    TooManyJoinsRule,
    # Structural rewrite patterns
    SelfJoinCouldBeWindowRule,
    TriangleJoinPatternRule,
    JoinWithSubqueryCouldBeCTERule,
    TriangularJoinRule,
)

# Subquery rules
from .subquery_rules import (
    CorrelatedSubqueryInWhereRule,
    SubqueryInsteadOfJoinRule,
    DeeplyNestedSubqueryRule,
    RepeatedSubqueryRule,
    # Structural rewrite patterns
    ScalarSubqueryToLateralRule,
    ExistsWithSelectStarRule,
    CorrelatedSubqueryCouldBeWindowRule,
)

# UNION rules
from .union_rules import UnionWithoutAllRule, LargeUnionChainRule, UnionTypeMismatchRule

# ORDER BY rules
from .order_rules import (
    OrderByInSubqueryRule,
    OrderByWithoutLimitRule,
    OrderByExpressionRule,
    OrderByOrdinalRule,
    OffsetPaginationRule,
)

# CTE rules
from .cte_rules import (
    SelectStarInCTERule,
    MultiRefCTERule,
    RecursiveCTERule,
    DeeplyNestedCTERule,
    # Structural rewrite patterns
    CTEWithAggregateReusedRule,
    CTEShouldBeSubqueryRule,
)

# Window function rules
from .window_rules import (
    RowNumberWithoutOrderRule,
    MultipleWindowPartitionsRule,
    WindowWithoutPartitionRule,
    NestedWindowFunctionRule,
)

# Aggregation rules
from .aggregation_rules import (
    GroupByOrdinalRule,
    HavingWithoutAggregateRule,
    GroupByExpressionRule,
    DistinctInsideAggregateRule,
    MissingGroupByColumnRule,
    # Structural rewrite patterns
    RepeatedAggregationRule,
    AggregateOfAggregateRule,
    GroupByWithHavingCountRule,
    LargeCountDistinctRule,
)

# Cursor/loop rules
from .cursor_rules import CursorUsageRule, WhileLoopRule, DynamicSQLRule

# Data type rules
from .type_rules import (
    StringNumericComparisonRule,
    DateAsStringRule,
    UnicodeMismatchRule,
)

# Fabric/SQL Server specific rules
from .fabric_rules import (
    TableVariableRule,
    TempTableWithoutIndexRule,
    MissingOptionRecompileRule,
)

# PostgreSQL-specific rules
from .postgres_rules import (
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
)

# DuckDB-specific rules
from .duckdb_rules import (
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
    # Optimizer weakness exploits (SQL-DUCK-011 to 018)
    CrossJoinUnnestWithWhereRule,
    WindowBlocksPredicatePushdownRule,
    ManyJoinsOnParquetRule,
    GroupedTopNPatternRule,
    MissingRedundantJoinFilterRule,
    LargePivotWithoutFilterRule,
    CountDistinctOnOrderedDataRule,
    NestedLoopJoinRiskRule,
)

# Optimization opportunity rules (from POC rulebook)
from .optimization_rules import (
    SingleUseCTEInlineRule,
    UnusedCTERule,
    ExistsToSemiJoinRule,
    LeftJoinAntiPatternRule,
    LeftJoinFilterRule,
    NonSargablePredicateRule,
    NotInNullTrapRule,
    HavingNonAggregateRule,
    DistinctGroupByRedundancyRule,
    OffsetPaginationRule as OptimizationOffsetRule,
    TopNPerGroupRule,
    UnnecessaryDistinctRule,
    OrToUnionRule,
    WindowPushdownRule,
    PreAggregateRule,
    GroupByFunctionalDependencyRule,
)

# Optimization opportunity rules - synced with knowledge_base.TRANSFORM_REGISTRY
# These 11 rules match the KB transform registry
from .opportunity_rules import (
    OrToUnionOpportunity,              # QT-OPT-001: or_to_union
    CorrelatedToPrecomputedCTEOpportunity,  # QT-OPT-002: correlated_to_cte
    LateDateFilterOpportunity,         # QT-OPT-003: date_cte_isolate
    PredicatePushdownOpportunity,      # QT-OPT-004: push_pred
    ScanConsolidationOpportunity,      # QT-OPT-005: consolidate_scans
    MultiPushPredicateOpportunity,     # QT-OPT-006: multi_push_pred
    RepeatedSubqueryOpportunity,       # QT-OPT-007: materialize_cte
    CountToExistsOpportunity,          # QT-OPT-008: flatten_subq
    JoinReorderOpportunity,            # QT-OPT-009: reorder_join
    InlineCTEOpportunity,              # QT-OPT-010: inline_cte
    RemoveRedundantOpportunity,        # QT-OPT-011: remove_redundant
)

# Gold Standard Rules - Verified transforms with proven speedups
from .gold_rules import (
    EarlyFilterPushdownGold,           # GLD-003: early_filter (2.71x, 1.84x, 1.24x, 1.23x)
    ProjectionPruningGold,             # GLD-004: projection_prune (1.21x)
    CorrelatedSubqueryGold,            # GLD-005: correlated_subquery (1.80x avg)
    DecorrelateSubqueryGold,           # GLD-001: decorrelate (2.81x)
    OrToUnionGold,                     # GLD-002: or_to_union (2.67x)
    UnionCTESpecializationGold,        # GLD-006: CTE UNION split (1.42x Q74)
    SubqueryMaterializationGold,       # GLD-007: subquery to CTE (1.24x Q73)
)

# Snowflake-specific rules
from .snowflake_rules import (
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

__all__ = [
    # SELECT
    "SelectStarRule",
    "ScalarSubqueryInSelectRule",
    "MultipleScalarSubqueriesRule",
    "CorrelatedSubqueryInSelectRule",
    "DistinctCrutchRule",
    "ScalarUDFInSelectRule",
    # WHERE
    "FunctionOnColumnRule",
    "LeadingWildcardRule",
    "NotInSubqueryRule",
    "ImplicitTypeConversionRule",
    "OrInsteadOfInRule",
    "DoubleNegativeRule",
    "RedundantPredicateRule",
    "CoalesceInFilterRule",
    "NonSargableDateRule",
    "OrPreventsIndexRule",
    "NotInNullRiskRule",
    # JOIN
    "CartesianJoinRule",
    "ImplicitJoinRule",
    "FunctionInJoinRule",
    "OrInJoinRule",
    "ExpressionInJoinRule",
    "InequalityJoinRule",
    "TooManyJoinsRule",
    "SelfJoinCouldBeWindowRule",
    "TriangleJoinPatternRule",
    "JoinWithSubqueryCouldBeCTERule",
    "TriangularJoinRule",
    # Subquery
    "CorrelatedSubqueryInWhereRule",
    "SubqueryInsteadOfJoinRule",
    "DeeplyNestedSubqueryRule",
    "RepeatedSubqueryRule",
    "ScalarSubqueryToLateralRule",
    "ExistsWithSelectStarRule",
    "CorrelatedSubqueryCouldBeWindowRule",
    # UNION
    "UnionWithoutAllRule",
    "LargeUnionChainRule",
    "UnionTypeMismatchRule",
    # ORDER BY
    "OrderByInSubqueryRule",
    "OrderByWithoutLimitRule",
    "OrderByExpressionRule",
    "OrderByOrdinalRule",
    "OffsetPaginationRule",
    # CTE
    "SelectStarInCTERule",
    "MultiRefCTERule",
    "RecursiveCTERule",
    "DeeplyNestedCTERule",
    "CTEWithAggregateReusedRule",
    "CTEShouldBeSubqueryRule",
    # Window
    "RowNumberWithoutOrderRule",
    "MultipleWindowPartitionsRule",
    "WindowWithoutPartitionRule",
    "NestedWindowFunctionRule",
    # Aggregation
    "GroupByOrdinalRule",
    "HavingWithoutAggregateRule",
    "GroupByExpressionRule",
    "DistinctInsideAggregateRule",
    "MissingGroupByColumnRule",
    "RepeatedAggregationRule",
    "AggregateOfAggregateRule",
    "GroupByWithHavingCountRule",
    "LargeCountDistinctRule",
    # Cursor
    "CursorUsageRule",
    "WhileLoopRule",
    "DynamicSQLRule",
    # Data types
    "StringNumericComparisonRule",
    "DateAsStringRule",
    "UnicodeMismatchRule",
    # Fabric
    "TableVariableRule",
    "TempTableWithoutIndexRule",
    "MissingOptionRecompileRule",
    # PostgreSQL
    "CountStarInsteadOfExistsRule",
    "LargeInListRule",
    "CurrentTimestampInWhereRule",
    "MissingNullsOrderRule",
    "ArrayAggWithoutOrderRule",
    "SerialColumnRule",
    "JsonbWithoutGinIndexHintRule",
    "TextWithoutCollationRule",
    "NotUsingLateralRule",
    "RandomOrderByRule",
    # DuckDB
    "NotUsingQualifyRule",
    "NotUsingGroupByAllRule",
    "ListAggWithoutOrderRule",
    "TempTableInsteadOfCTERule",
    "NotUsingSampleRule",
    "NotUsingExcludeRule",
    "SubqueryInsteadOfPivotRule",
    "NotUsingUnpivotRule",
    "DirectParquetReadRule",
    "NotUsingAsOfJoinRule",
    # DuckDB optimizer weakness exploits
    "CrossJoinUnnestWithWhereRule",
    "WindowBlocksPredicatePushdownRule",
    "ManyJoinsOnParquetRule",
    "GroupedTopNPatternRule",
    "MissingRedundantJoinFilterRule",
    "LargePivotWithoutFilterRule",
    "CountDistinctOnOrderedDataRule",
    "NestedLoopJoinRiskRule",
    # Snowflake
    "CopyIntoWithoutFileFormatRule",
    "SelectWithoutLimitOrSampleRule",
    "NonDeterministicInClusteringKeyRule",
    "ConsiderClusterByOnFilteredColumnsRule",
    "VariantExtractionWithoutCastRule",
    "FlattenWithoutLateralRule",
    "TimeTravelWithoutRetentionCheckRule",
    "CrossDatabaseMetadataWithoutAccountUsageRule",
    "VariantPredicateWithoutSearchOptimizationRule",
    "GetDdlWithoutSchemaQualificationRule",
    "InefficientMicroPartitionPruningRule",
    "MissingClusteringKeyMaintenanceRule",
    "OverClusteringRule",
    "StaleClusteringCheckRule",
    "SuboptimalClusteringColumnOrderRule",
    "InefficientDatePartitionPruningRule",
    "MissingPartitionPruningHintRule",
    "CrossPartitionScanRule",
    "MissingMaterializedViewCandidateRule",
    "StaleMaterializedViewCheckRule",
    # Optimization rules (from POC rulebook)
    "SingleUseCTEInlineRule",
    "UnusedCTERule",
    "ExistsToSemiJoinRule",
    "LeftJoinAntiPatternRule",
    "LeftJoinFilterRule",
    "NonSargablePredicateRule",
    "NotInNullTrapRule",
    "HavingNonAggregateRule",
    "DistinctGroupByRedundancyRule",
    "OptimizationOffsetRule",
    "TopNPerGroupRule",
    "UnnecessaryDistinctRule",
    "OrToUnionRule",
    "WindowPushdownRule",
    "PreAggregateRule",
    "GroupByFunctionalDependencyRule",
    # Optimization opportunities (synced with knowledge_base - 11 KB transforms)
    "OrToUnionOpportunity",              # QT-OPT-001
    "CorrelatedToPrecomputedCTEOpportunity",  # QT-OPT-002
    "LateDateFilterOpportunity",         # QT-OPT-003
    "PredicatePushdownOpportunity",      # QT-OPT-004
    "ScanConsolidationOpportunity",      # QT-OPT-005
    "MultiPushPredicateOpportunity",     # QT-OPT-006
    "RepeatedSubqueryOpportunity",       # QT-OPT-007
    "CountToExistsOpportunity",          # QT-OPT-008
    "JoinReorderOpportunity",            # QT-OPT-009
    "InlineCTEOpportunity",              # QT-OPT-010
    "RemoveRedundantOpportunity",        # QT-OPT-011
]
