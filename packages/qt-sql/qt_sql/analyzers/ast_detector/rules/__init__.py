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
]
