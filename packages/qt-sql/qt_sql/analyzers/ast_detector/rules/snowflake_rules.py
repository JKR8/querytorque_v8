"""Snowflake-specific anti-pattern detection rules."""

from __future__ import annotations

from typing import Iterator, Optional

from sqlglot import exp

from ..base import ASTRule, ASTContext, RuleMatch


def _upper_name(expression: Optional[exp.Expression]) -> str:
    if expression is None:
        return ""
    if isinstance(expression, exp.Identifier):
        return str(expression.this).upper()
    if isinstance(expression, exp.Var):
        return str(expression.this).upper()
    return str(expression).upper()


def _has_cast_ancestor(node: exp.Expression) -> bool:
    parent = node.parent
    while parent and not isinstance(parent, exp.Select):
        if isinstance(parent, (exp.Cast, exp.TryCast)):
            return True
        parent = parent.parent
    return False


def _is_account_usage_table(table: exp.Table) -> bool:
    catalog = _upper_name(table.args.get("catalog"))
    schema = _upper_name(table.args.get("db"))
    return catalog == "SNOWFLAKE" and schema == "ACCOUNT_USAGE"


class CopyIntoWithoutFileFormatRule(ASTRule):
    """SQL-SNOW-001: COPY INTO without FILE_FORMAT.

    Snowflake COPY can inherit a default file format from the stage, but it’s easy
    to accidentally load with the wrong format when stages change.

    Prefer specifying the file format explicitly:
        COPY INTO my_table
        FROM @my_stage/path
        FILE_FORMAT = (TYPE = CSV);

    Detection:
    - Find COPY statements lacking a FILE_FORMAT parameter.
    """

    rule_id = "SQL-SNOW-001"
    name = "COPY INTO Without FILE_FORMAT"
    description = "COPY INTO without explicit FILE_FORMAT can load unexpected formats"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Add FILE_FORMAT = (...) or FILE_FORMAT = (FORMAT_NAME = ...)"
    dialects = ("snowflake",)

    target_node_types = (exp.Copy,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        params = node.args.get("params") or []
        for param in params:
            if isinstance(param, exp.CopyParameter) and _upper_name(param.args.get("this")) == "FILE_FORMAT":
                return

        yield RuleMatch(
            node=node,
            context=context,
            message="COPY INTO without FILE_FORMAT - specify the format explicitly",
            matched_text=node.sql()[:120],
        )


class SelectWithoutLimitOrSampleRule(ASTRule):
    """SQL-SNOW-002: Top-level SELECT without LIMIT or SAMPLE.

    In Snowflake, exploratory queries that scan whole tables can be expensive.
    For quick inspection, use LIMIT or TABLESAMPLE/SAMPLE.

    Detection (heuristic):
    - Top-level SELECT
    - SELECT * (exploratory)
    - No WHERE, LIMIT/FETCH, or TABLESAMPLE
    """

    rule_id = "SQL-SNOW-002"
    name = "SELECT Without LIMIT or SAMPLE"
    description = "Top-level SELECT * without LIMIT/TABLESAMPLE can trigger full scans"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Add LIMIT or use TABLESAMPLE for exploration"
    dialects = ("snowflake",)

    target_node_types = (exp.Select,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        if node.find(exp.Where):
            return

        if node.find(exp.Limit) or node.find(exp.Fetch):
            return

        if node.find(exp.TableSample):
            return

        if not any(isinstance(e, exp.Star) for e in node.expressions):
            return

        if not node.args.get("from"):
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="Top-level SELECT * without LIMIT/TABLESAMPLE - consider limiting rows",
            matched_text="SELECT * without LIMIT",
        )


class NonDeterministicInClusteringKeyRule(ASTRule):
    """SQL-SNOW-003: Non-deterministic functions in clustering keys.

    Clustering keys should be deterministic; using RANDOM()/RAND() or UUID/UUID_STRING()
    prevents stable micro-partition pruning and can degrade maintenance.

    Detection:
    - Find CLUSTER BY expressions containing non-deterministic functions.
    """

    rule_id = "SQL-SNOW-003"
    name = "Non-deterministic Clustering Key"
    description = "Non-deterministic functions in CLUSTER BY prevent stable pruning"
    severity = "high"
    category = "snowflake"
    penalty = 15
    suggestion = "Remove RANDOM()/UUID_STRING() from CLUSTER BY and use deterministic columns"
    dialects = ("snowflake",)

    target_node_types = (exp.Cluster,)

    NON_DETERMINISTIC_FUNC_TYPES = (exp.Rand, exp.Uuid)
    NON_DETERMINISTIC_FUNC_NAMES = {"RANDOM", "RAND", "UUID_STRING", "UUID"}

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        for func in node.find_all(exp.Func):
            func_name = type(func).__name__.upper()
            if isinstance(func, self.NON_DETERMINISTIC_FUNC_TYPES) or func_name in self.NON_DETERMINISTIC_FUNC_NAMES:
                yield RuleMatch(
                    node=func,
                    context=context,
                    message="Non-deterministic function used in CLUSTER BY",
                    matched_text=func.sql()[:60],
                )

        for anon in node.find_all(exp.Anonymous):
            if _upper_name(anon.this) in self.NON_DETERMINISTIC_FUNC_NAMES:
                yield RuleMatch(
                    node=anon,
                    context=context,
                    message="Non-deterministic function used in CLUSTER BY",
                    matched_text=anon.sql()[:60],
                )


class ConsiderClusterByOnFilteredColumnsRule(ASTRule):
    """SQL-SNOW-004: Consider CLUSTER BY on frequently filtered columns.

    Snowflake clustering can improve micro-partition pruning for tables that are
    repeatedly filtered by the same columns (e.g., date ranges, tenant_id).

    Detection (heuristic):
    - Top-level SELECT
    - Single-table query with WHERE predicates
    - 2+ distinct columns referenced in WHERE
    """

    rule_id = "SQL-SNOW-004"
    name = "Consider CLUSTER BY"
    description = "Frequent filtering columns may benefit from CLUSTER BY for pruning"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "If this query pattern is common, consider CLUSTER BY on the filter columns"
    dialects = ("snowflake",)

    target_node_types = (exp.Select,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        where = node.find(exp.Where)
        if not where:
            return

        if node.find(exp.Join):
            return

        from_ = node.args.get("from")
        if not from_ or not list(from_.find_all(exp.Table)):
            return

        columns: set[str] = set()
        for col in where.find_all(exp.Column):
            if col.find_ancestor(exp.Subquery):
                continue
            columns.add(col.sql(dialect="snowflake"))

        if len(columns) < 2:
            return

        column_list = ", ".join(sorted(columns)[:3])
        suffix = "" if len(columns) <= 3 else f" (+{len(columns) - 3} more)"

        yield RuleMatch(
            node=node,
            context=context,
            message=f"WHERE filters on {len(columns)} columns - consider CLUSTER BY ({column_list}{suffix})",
            matched_text="SELECT with multi-column WHERE filters",
        )


class VariantExtractionWithoutCastRule(ASTRule):
    """SQL-SNOW-005: VARIANT extraction without explicit type cast.

    Extracting from VARIANT/OBJECT/ARRAY often benefits from casting to a concrete
    type for comparisons and joins:
        WHERE v:status::STRING = 'active'

    Detection:
    - JSON/VARIANT extraction (e.g., v:field, v[0]) inside WHERE/JOIN/HAVING
    - Not wrapped in CAST/TRY_CAST
    """

    rule_id = "SQL-SNOW-005"
    name = "VARIANT Extraction Without Cast"
    description = "Comparisons on VARIANT paths without casting can be inefficient/ambiguous"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Cast extracted values (e.g., v:field::STRING, TRY_TO_NUMBER(...))"
    dialects = ("snowflake",)

    target_node_types = (exp.JSONExtract, exp.Bracket)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not (context.in_where or context.in_join_condition or context.in_having):
            return

        if _has_cast_ancestor(node):
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="VARIANT extraction used in predicate without explicit cast",
            matched_text=node.sql()[:80],
        )


class FlattenWithoutLateralRule(ASTRule):
    """SQL-SNOW-006: FLATTEN used without LATERAL.

    Snowflake FLATTEN is a table function and is typically used with LATERAL:
        FROM t, LATERAL FLATTEN(input => t.v) f

    Detection:
    - Find FLATTEN (parsed as EXPLODE/UDTF) under FROM/JOIN
    - Flag when not wrapped in LATERAL
    """

    rule_id = "SQL-SNOW-006"
    name = "FLATTEN Without LATERAL"
    description = "Using FLATTEN without LATERAL can lead to inefficient/incorrect joins"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Use LATERAL FLATTEN(...) to correlate with the left table"
    dialects = ("snowflake",)

    target_node_types = (exp.Explode,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if node.find_ancestor(exp.Lateral):
            return

        if not (node.find_ancestor(exp.Join) or node.find_ancestor(exp.From)):
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="FLATTEN used without LATERAL - consider LATERAL FLATTEN for correlation",
            matched_text=node.sql()[:100],
        )


class TimeTravelWithoutRetentionCheckRule(ASTRule):
    """SQL-SNOW-007: Time travel query without retention awareness.

    Time travel (AT/BEFORE) only works within the object’s retention window
    (`DATA_RETENTION_TIME_IN_DAYS`) and account/edition limits.

    Detection:
    - Find table references using AT(...) or BEFORE(...).
    """

    rule_id = "SQL-SNOW-007"
    name = "Time Travel Retention Check"
    description = "Time travel queries can fail outside DATA_RETENTION_TIME retention window"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Verify DATA_RETENTION_TIME_IN_DAYS for the target object/account"
    dialects = ("snowflake",)

    target_node_types = (exp.Table,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if node.args.get("when") is None:
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="Time travel query (AT/BEFORE) - verify retention window (DATA_RETENTION_TIME_IN_DAYS)",
            matched_text=node.sql()[:120],
        )


class CrossDatabaseMetadataWithoutAccountUsageRule(ASTRule):
    """SQL-SNOW-008: Cross-database metadata queries without ACCOUNT_USAGE.

    Querying multiple databases’ INFORMATION_SCHEMA can be slow and expensive.
    For many governance/audit use cases, SNOWFLAKE.ACCOUNT_USAGE views can be a
    better centralized source (with proper time filtering).

    Detection (heuristic):
    - Query references INFORMATION_SCHEMA in 2+ databases
    - Query does not already reference SNOWFLAKE.ACCOUNT_USAGE
    """

    rule_id = "SQL-SNOW-008"
    name = "Cross-Database Metadata Query"
    description = "Cross-database INFORMATION_SCHEMA queries may be better served via SNOWFLAKE.ACCOUNT_USAGE"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Consider SNOWFLAKE.ACCOUNT_USAGE views (and filter by time range) for account-wide metadata"
    dialects = ("snowflake",)

    target_node_types = (exp.Select,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        tables = list(node.find_all(exp.Table))
        if not tables:
            return

        if any(_is_account_usage_table(t) for t in tables):
            return

        info_schema_catalogs: set[str] = set()
        for t in tables:
            if _upper_name(t.args.get("db")) == "INFORMATION_SCHEMA":
                info_schema_catalogs.add(_upper_name(t.args.get("catalog")) or "__CURRENT_DB__")

        if len(info_schema_catalogs) < 2:
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="Cross-database INFORMATION_SCHEMA query - consider SNOWFLAKE.ACCOUNT_USAGE optimization",
            matched_text="INFORMATION_SCHEMA across databases",
        )


class VariantPredicateWithoutSearchOptimizationRule(ASTRule):
    """SQL-SNOW-009: Filtering on VARIANT/OBJECT paths without search optimization.

    Filtering on VARIANT paths (e.g., v:field) can be slow at scale. Snowflake’s
    Search Optimization Service (SOS) or extracted/materialized columns can help
    for highly selective predicates.

    Detection (heuristic):
    - WHERE clause contains JSON/VARIANT extraction in a predicate
    """

    rule_id = "SQL-SNOW-009"
    name = "Consider Search Optimization for VARIANT"
    description = "Predicates on VARIANT/OBJECT paths may benefit from Search Optimization Service"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Consider Search Optimization Service or materializing extracted fields for frequent predicates"
    dialects = ("snowflake",)

    target_node_types = (exp.Where,)

    PREDICATE_TYPES = (exp.EQ, exp.NEQ, exp.GT, exp.GTE, exp.LT, exp.LTE, exp.Like, exp.ILike, exp.In, exp.Between)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        for predicate in node.find_all(*self.PREDICATE_TYPES):
            has_variant_extract = bool(predicate.find(exp.JSONExtract) or predicate.find(exp.Bracket))
            if not has_variant_extract:
                continue

            if predicate.find(exp.Literal) is None:
                continue

            yield RuleMatch(
                node=predicate,
                context=context,
                message="Predicate filters on VARIANT/OBJECT path - consider Search Optimization Service/materialization",
                matched_text=predicate.sql()[:120],
            )
            return  # Report once per WHERE for signal/noise


class GetDdlWithoutSchemaQualificationRule(ASTRule):
    """SQL-SNOW-010: GET_DDL without schema qualification.

    GET_DDL is clearer and safer when object names are qualified:
        SELECT GET_DDL('TABLE', 'DB.SCHEMA.MY_TABLE');

    Detection:
    - Find GET_DDL calls where the object name argument is an unqualified string.
    """

    rule_id = "SQL-SNOW-010"
    name = "GET_DDL Without Schema Qualification"
    description = "GET_DDL called with unqualified object name can resolve ambiguously"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Qualify the object name (e.g., DB.SCHEMA.OBJECT) in GET_DDL()"
    dialects = ("snowflake",)

    target_node_types = (exp.Anonymous,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        func_name = _upper_name(node.this)
        if func_name != "GET_DDL":
            return

        args = list(node.expressions or [])
        if len(args) < 2:
            return

        object_name = args[1]
        if not isinstance(object_name, exp.Literal) or not object_name.is_string:
            return

        raw = str(object_name.this or "")
        value = raw.strip().strip("'").strip('"')
        if "." in value:
            return

        yield RuleMatch(
            node=node,
            context=context,
            message="GET_DDL called with unqualified object name - add schema (and database) qualification",
            matched_text=node.sql()[:120],
        )


class InefficientMicroPartitionPruningRule(ASTRule):
    """SQL-SNOW-011: Inefficient micro-partition pruning patterns.

    Snowflake uses micro-partitions for data storage. Filters that prevent
    partition pruning (e.g., functions on filter columns, broad range scans)
    can cause full table scans.

    Detection:
    - WHERE clause with date/timestamp column wrapped in function (prevents pruning)
    - Very broad date ranges (e.g., > 1 year without additional filters)
    """

    rule_id = "SQL-SNOW-011"
    name = "Inefficient Micro-Partition Pruning"
    description = "Query pattern may prevent efficient micro-partition pruning"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Avoid functions on clustering/partition columns in WHERE; use direct comparisons"
    dialects = ("snowflake",)

    target_node_types = (exp.Where,)

    DATE_FUNC_NAMES = {"DATE_TRUNC", "TRUNC", "DATE_PART", "DATEADD", "DATEDIFF", "EXTRACT", "YEAR", "MONTH", "DAY"}

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Look for date functions wrapping columns in predicates
        for func in node.find_all(exp.Func):
            func_name = type(func).__name__.upper()
            if func_name in self.DATE_FUNC_NAMES or (
                isinstance(func, exp.Anonymous) and _upper_name(func.this) in self.DATE_FUNC_NAMES
            ):
                # Check if a column is an argument
                if func.find(exp.Column):
                    yield RuleMatch(
                        node=func,
                        context=context,
                        message="Date function on column may prevent micro-partition pruning",
                        matched_text=func.sql()[:80],
                    )
                    return  # One issue per WHERE for signal/noise


class MissingClusteringKeyMaintenanceRule(ASTRule):
    """SQL-SNOW-012: Tables without explicit clustering consideration.

    Large Snowflake tables with frequent filter patterns benefit from CLUSTER BY.
    This rule flags CREATE TABLE statements for tables that may benefit from
    clustering based on naming conventions (e.g., *_fact, *_events, *_logs).

    Detection:
    - CREATE TABLE without CLUSTER BY
    - Table name suggests high-volume data (fact, events, logs, transactions, history)
    """

    rule_id = "SQL-SNOW-012"
    name = "Consider Clustering Key"
    description = "Large data tables may benefit from CLUSTER BY for query performance"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Consider adding CLUSTER BY on frequently filtered columns (e.g., date, tenant_id)"
    dialects = ("snowflake",)

    target_node_types = (exp.Create,)

    HIGH_VOLUME_SUFFIXES = ("_FACT", "_FACTS", "_EVENTS", "_LOGS", "_TRANSACTIONS", "_HISTORY", "_AUDIT", "_METRICS")

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Only CREATE TABLE
        if not node.args.get("kind") or str(node.args.get("kind")).upper() != "TABLE":
            return

        # Check if already has CLUSTER BY
        if node.find(exp.Cluster):
            return

        # Get table name
        table = node.find(exp.Table)
        if not table:
            return

        table_name = _upper_name(table.args.get("this"))
        if not table_name:
            return

        # Check for high-volume naming patterns
        for suffix in self.HIGH_VOLUME_SUFFIXES:
            if table_name.endswith(suffix):
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"Table '{table_name}' may benefit from CLUSTER BY for query performance",
                    matched_text=f"CREATE TABLE {table_name}",
                )
                return


class OverClusteringRule(ASTRule):
    """SQL-SNOW-013: CLUSTER BY with too many columns.

    Snowflake recommends 3-4 clustering columns maximum. More columns increase
    maintenance overhead with diminishing returns for query performance.

    Detection:
    - CLUSTER BY with more than 4 columns
    """

    rule_id = "SQL-SNOW-013"
    name = "Over-Clustering"
    description = "CLUSTER BY with too many columns increases maintenance overhead"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Limit CLUSTER BY to 3-4 columns for optimal maintenance vs. query benefit"
    dialects = ("snowflake",)

    target_node_types = (exp.Cluster,)

    MAX_CLUSTERING_COLUMNS = 4

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count clustering expressions
        clustering_exprs = list(node.expressions) if node.expressions else []
        if len(clustering_exprs) > self.MAX_CLUSTERING_COLUMNS:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"CLUSTER BY has {len(clustering_exprs)} columns (recommended max: {self.MAX_CLUSTERING_COLUMNS})",
                matched_text=node.sql()[:100],
            )


class StaleClusteringCheckRule(ASTRule):
    """SQL-SNOW-014: Queries on clustered tables should monitor clustering health.

    Snowflake clustering can become stale after heavy DML. Use
    SYSTEM$CLUSTERING_INFORMATION() to monitor clustering depth/ratio.

    Detection:
    - Query references a table with CLUSTER BY (in same statement or DDL context)
    - Suggest monitoring clustering health
    """

    rule_id = "SQL-SNOW-014"
    name = "Monitor Clustering Health"
    description = "Clustered tables need periodic health checks via SYSTEM$CLUSTERING_INFORMATION"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Use SYSTEM$CLUSTERING_INFORMATION('table') to check clustering depth and ratio"
    dialects = ("snowflake",)

    target_node_types = (exp.Alter,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Check if ALTER TABLE adds/modifies clustering
        cluster = node.find(exp.Cluster)
        if cluster:
            table = node.find(exp.Table)
            table_name = table.sql() if table else "table"
            yield RuleMatch(
                node=node,
                context=context,
                message=f"After adding clustering, monitor health with SYSTEM$CLUSTERING_INFORMATION('{table_name}')",
                matched_text=node.sql()[:100],
            )


class SuboptimalClusteringColumnOrderRule(ASTRule):
    """SQL-SNOW-015: Suboptimal clustering column order.

    Clustering columns should be ordered by cardinality (low to high) for best
    pruning efficiency. Date/timestamp columns typically go first, followed by
    low-cardinality columns like status, type, region.

    Detection:
    - CLUSTER BY with identifiable column patterns in suboptimal order
    - E.g., high-cardinality UUID/ID before date column
    """

    rule_id = "SQL-SNOW-015"
    name = "Suboptimal Clustering Column Order"
    description = "Clustering columns should be ordered low-to-high cardinality for best pruning"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Order clustering columns: date/time first, then low-cardinality (status/type), then higher cardinality"
    dialects = ("snowflake",)

    target_node_types = (exp.Cluster,)

    # Patterns that suggest column cardinality
    HIGH_CARDINALITY_PATTERNS = ("ID", "UUID", "GUID", "KEY", "CODE", "NUMBER", "NUM")
    LOW_CARDINALITY_PATTERNS = ("DATE", "TIME", "TIMESTAMP", "DT", "STATUS", "TYPE", "REGION", "COUNTRY", "STATE")

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if not node.expressions:
            return

        columns = []
        for expr in node.expressions:
            if isinstance(expr, exp.Column):
                col_name = _upper_name(expr.args.get("this"))
                columns.append(col_name)

        if len(columns) < 2:
            return

        # Check if a high-cardinality column comes before a low-cardinality one
        high_card_idx = -1
        low_card_idx = -1

        for i, col in enumerate(columns):
            if any(p in col for p in self.HIGH_CARDINALITY_PATTERNS):
                if high_card_idx == -1:
                    high_card_idx = i
            if any(p in col for p in self.LOW_CARDINALITY_PATTERNS):
                low_card_idx = i

        if high_card_idx != -1 and low_card_idx != -1 and high_card_idx < low_card_idx:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"High-cardinality column before low-cardinality in CLUSTER BY - consider reordering",
                matched_text=node.sql()[:100],
            )


class InefficientDatePartitionPruningRule(ASTRule):
    """SQL-SNOW-016: Date filter patterns that prevent partition pruning.

    Direct date comparisons enable partition pruning. Functions on date columns,
    type conversions, or complex expressions can prevent pruning.

    Detection:
    - WHERE with TO_DATE, TO_TIMESTAMP, CAST on column instead of literal
    - Comparing dates as strings
    """

    rule_id = "SQL-SNOW-016"
    name = "Inefficient Date Partition Pruning"
    description = "Date conversion on column prevents partition pruning - convert the literal instead"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Apply date conversion to literals, not columns: WHERE date_col >= TO_DATE('2024-01-01')"
    dialects = ("snowflake",)

    target_node_types = (exp.Where,)

    DATE_CONVERT_FUNCS = {"TO_DATE", "TO_TIMESTAMP", "TO_TIMESTAMP_NTZ", "TO_TIMESTAMP_TZ", "TO_TIMESTAMP_LTZ"}

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        for func in node.find_all(exp.Anonymous):
            func_name = _upper_name(func.this)
            if func_name in self.DATE_CONVERT_FUNCS:
                # Check if first argument is a column (bad) vs literal (good)
                args = list(func.expressions or [])
                if args and isinstance(args[0], exp.Column):
                    yield RuleMatch(
                        node=func,
                        context=context,
                        message=f"{func_name}() on column prevents partition pruning - convert literal instead",
                        matched_text=func.sql()[:80],
                    )
                    return  # One issue per WHERE

        # Also check for Cast on columns in comparisons
        for cast in node.find_all(exp.Cast):
            if cast.find(exp.Column) and cast.args.get("to"):
                to_type = str(cast.args.get("to")).upper()
                if "DATE" in to_type or "TIMESTAMP" in to_type:
                    yield RuleMatch(
                        node=cast,
                        context=context,
                        message="CAST to date/timestamp on column may prevent partition pruning",
                        matched_text=cast.sql()[:80],
                    )
                    return


class MissingPartitionPruningHintRule(ASTRule):
    """SQL-SNOW-017: Large table scan without partition-friendly filter.

    Queries on known large tables without filters on likely partition columns
    (date, timestamp, tenant) may trigger expensive full scans.

    Detection:
    - SELECT from table with *_fact, *_history, *_events suffix
    - No WHERE clause or WHERE doesn't filter on date-like columns
    """

    rule_id = "SQL-SNOW-017"
    name = "Missing Partition Filter"
    description = "Query on large table without date/partition filter may cause full scan"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Add date range or partition key filter to enable pruning"
    dialects = ("snowflake",)

    target_node_types = (exp.Select,)

    LARGE_TABLE_SUFFIXES = ("_FACT", "_FACTS", "_HISTORY", "_EVENTS", "_LOGS", "_TRANSACTIONS", "_RAW")
    PARTITION_COLUMN_PATTERNS = ("DATE", "TIME", "TIMESTAMP", "DT", "CREATED", "UPDATED", "EVENT")

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Find tables
        tables = list(node.find_all(exp.Table))
        large_tables = []
        for t in tables:
            table_name = _upper_name(t.args.get("this"))
            if table_name and any(table_name.endswith(s) for s in self.LARGE_TABLE_SUFFIXES):
                large_tables.append(table_name)

        if not large_tables:
            return

        # Check WHERE clause for partition-friendly filters
        where = node.find(exp.Where)
        if not where:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Query on {large_tables[0]} without WHERE - consider adding date filter",
                matched_text=f"SELECT ... FROM {large_tables[0]}",
            )
            return

        # Check if WHERE has date-like column filters
        has_partition_filter = False
        for col in where.find_all(exp.Column):
            col_name = _upper_name(col.args.get("this"))
            if col_name and any(p in col_name for p in self.PARTITION_COLUMN_PATTERNS):
                has_partition_filter = True
                break

        if not has_partition_filter:
            yield RuleMatch(
                node=node,
                context=context,
                message=f"Query on {large_tables[0]} without date/partition filter - may cause full scan",
                matched_text=f"SELECT ... FROM {large_tables[0]} WHERE ...",
            )


class CrossPartitionScanRule(ASTRule):
    """SQL-SNOW-018: UNION queries that may scan across many partitions.

    UNION of queries with different date ranges forces scanning multiple
    partition sets. Consider restructuring as a single query with OR/IN.

    Detection:
    - UNION with multiple date-filtered queries on same table
    """

    rule_id = "SQL-SNOW-018"
    name = "Cross-Partition UNION Scan"
    description = "UNION across date ranges scans multiple partition sets - consider OR/IN instead"
    severity = "medium"
    category = "snowflake"
    penalty = 10
    suggestion = "Combine date ranges with OR or IN clause in single query for better optimization"
    dialects = ("snowflake",)

    target_node_types = (exp.Union,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        # Count how many UNION branches reference the same table with date filters
        branches = [node.left, node.right] if hasattr(node, "left") else []

        tables_with_date_filters: dict[str, int] = {}
        for branch in branches:
            if not isinstance(branch, exp.Select):
                continue
            for table in branch.find_all(exp.Table):
                table_name = _upper_name(table.args.get("this"))
                if not table_name:
                    continue

                where = branch.find(exp.Where)
                if where:
                    # Check for date-like predicates
                    for col in where.find_all(exp.Column):
                        col_name = _upper_name(col.args.get("this"))
                        if col_name and any(
                            p in col_name for p in ("DATE", "TIME", "CREATED", "UPDATED")
                        ):
                            tables_with_date_filters[table_name] = (
                                tables_with_date_filters.get(table_name, 0) + 1
                            )
                            break

        # If same table appears in multiple UNION branches with date filters
        for table, count in tables_with_date_filters.items():
            if count >= 2:
                yield RuleMatch(
                    node=node,
                    context=context,
                    message=f"UNION on {table} with date filters - consider OR/IN for better pruning",
                    matched_text=node.sql()[:100],
                )
                return


class MissingMaterializedViewCandidateRule(ASTRule):
    """SQL-SNOW-019: Query patterns that may benefit from materialized views.

    Repeated aggregate queries on large tables with consistent GROUP BY
    patterns are candidates for materialized views.

    Detection:
    - SELECT with aggregate functions
    - GROUP BY on date truncation (common reporting pattern)
    - No hints of ad-hoc/exploratory query (e.g., has specific columns, not SELECT *)
    """

    rule_id = "SQL-SNOW-019"
    name = "Materialized View Candidate"
    description = "Repeated aggregate query pattern may benefit from a materialized view"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Consider CREATE MATERIALIZED VIEW for frequently-used aggregate patterns"
    dialects = ("snowflake",)

    target_node_types = (exp.Select,)

    AGGREGATE_FUNCS = (exp.Sum, exp.Count, exp.Avg, exp.Min, exp.Max)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery or context.in_cte:
            return

        # Must have aggregates
        has_aggregates = any(node.find(agg) for agg in self.AGGREGATE_FUNCS)
        if not has_aggregates:
            return

        # Must have GROUP BY
        group_by = node.find(exp.Group)
        if not group_by:
            return

        # Check for date truncation in GROUP BY (common MV pattern)
        has_date_trunc = False
        for func in (group_by.expressions or []):
            if isinstance(func, exp.Anonymous) and _upper_name(func.this) == "DATE_TRUNC":
                has_date_trunc = True
                break

        if has_date_trunc:
            yield RuleMatch(
                node=node,
                context=context,
                message="Aggregate query with DATE_TRUNC grouping - consider materialized view",
                matched_text="SELECT ... GROUP BY DATE_TRUNC(...)",
            )


class StaleMaterializedViewCheckRule(ASTRule):
    """SQL-SNOW-020: Queries on materialized views should verify freshness.

    Materialized views in Snowflake can be set to auto-refresh or manual.
    Queries relying on MV data should be aware of potential staleness.

    Detection:
    - SELECT from object with _MV suffix (naming convention)
    - Suggest checking MV refresh status
    """

    rule_id = "SQL-SNOW-020"
    name = "Verify Materialized View Freshness"
    description = "Queries on materialized views should verify data freshness"
    severity = "low"
    category = "snowflake"
    penalty = 5
    suggestion = "Check MV refresh status with SHOW MATERIALIZED VIEWS or query MATERIALIZED_VIEW_REFRESH_HISTORY"
    dialects = ("snowflake",)

    target_node_types = (exp.Select,)

    def detect(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        yield from self.check(node, context)

    def check(self, node: exp.Expression, context: ASTContext) -> Iterator[RuleMatch]:
        if context.in_subquery:
            return

        # Look for tables with MV naming convention
        for table in node.find_all(exp.Table):
            table_name = _upper_name(table.args.get("this"))
            if table_name and (table_name.endswith("_MV") or table_name.startswith("MV_")):
                yield RuleMatch(
                    node=table,
                    context=context,
                    message=f"Query on materialized view '{table_name}' - verify freshness for time-sensitive reports",
                    matched_text=table.sql(),
                )
                return  # One per query
