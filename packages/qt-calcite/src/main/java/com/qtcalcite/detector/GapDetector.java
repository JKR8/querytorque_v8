package com.qtcalcite.detector;

import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.duckdb.DuckDBAdapter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;

import java.sql.SQLException;
import java.util.*;

/**
 * Detects DuckDB optimizer gaps where Calcite can provide improvements.
 * Uses Calcite's SQL parser for accurate AST analysis.
 *
 * Documented gaps from DUCKDB_OPTIMIZER_GAPS.md:
 * 1. Join Order - TPC-DS Q37 pattern
 * 2. Semi-Join Inequality - EXISTS with <> predicate
 * 3. Grouped TopN - ROW_NUMBER() OVER PARTITION BY + filter
 * 4. CTE + LIMIT - ORDER BY in CTE, LIMIT outside
 * 5. INTERSECT/EXCEPT - empty input not short-circuited
 * 6. Multiple LEFT OUTER JOINs - exponential degradation
 */
public class GapDetector {

    private final DuckDBAdapter adapter;
    private final DuckDBStatistics statistics;
    private final SqlParser.Config parserConfig;

    public GapDetector(DuckDBAdapter adapter, DuckDBStatistics statistics) {
        this.adapter = adapter;
        this.statistics = statistics;
        // Use Babel parser for better compatibility with DuckDB SQL
        // Babel is more permissive with reserved words like "year", "value", etc.
        this.parserConfig = SqlParser.config()
            .withParserFactory(SqlBabelParserImpl.FACTORY)
            .withConformance(SqlConformanceEnum.BABEL)
            .withCaseSensitive(false);
    }

    /**
     * Analyze a SQL query for known DuckDB optimizer gaps.
     */
    public GapAnalysis analyze(String sql) {
        GapAnalysis analysis = new GapAnalysis(sql);

        try {
            // Parse SQL into AST
            SqlParser parser = SqlParser.create(sql, parserConfig);
            SqlNode parsed = parser.parseQuery();

            // Visit AST to detect patterns
            GapVisitor visitor = new GapVisitor(analysis, statistics);
            parsed.accept(visitor);

            // Post-processing checks
            visitor.finalizeAnalysis();

        } catch (SqlParseException e) {
            analysis.addWarning("SQL parse error: " + e.getMessage());
            // Fall back to basic text detection for unparseable queries
            detectBasicPatterns(sql, analysis);
        }

        // Phase 2: EXPLAIN-based detection (requires DB call)
        try {
            String explainPlan = adapter.getExplainPlan(sql);
            analysis.setExplainPlan(explainPlan);
            detectPlanIssues(explainPlan, analysis);
        } catch (SQLException e) {
            analysis.addWarning("Could not get EXPLAIN plan: " + e.getMessage());
        }

        return analysis;
    }

    /**
     * Basic pattern detection for queries that can't be parsed.
     */
    private void detectBasicPatterns(String sql, GapAnalysis analysis) {
        String lower = sql.toLowerCase();

        if (lower.contains("intersect")) {
            analysis.addGap(createSetOpGap("INTERSECT"));
        }
        if (lower.contains("except")) {
            analysis.addGap(createSetOpGap("EXCEPT"));
        }
    }

    /**
     * AST Visitor that detects gap patterns.
     */
    private static class GapVisitor extends SqlBasicVisitor<Void> {
        private final GapAnalysis analysis;
        private final DuckDBStatistics statistics;

        // Tracking state
        private int explicitJoinCount = 0;
        private int implicitJoinCount = 0;  // FROM a, b, c
        private int leftJoinCount = 0;
        private final Set<String> tableNames = new LinkedHashSet<>();

        // CTE tracking
        private boolean inCTE = false;
        private boolean cteHasOrderBy = false;
        private boolean outerQueryHasLimit = false;

        // Window function tracking
        private boolean hasWindowWithPartition = false;
        private boolean inSubquery = false;
        private int subqueryDepth = 0;

        // Track if we're inside a subquery that has window with partition
        private boolean subqueryHasWindowPartition = false;

        GapVisitor(GapAnalysis analysis, DuckDBStatistics statistics) {
            this.analysis = analysis;
            this.statistics = statistics;
        }

        @Override
        public Void visit(SqlCall call) {
            SqlKind kind = call.getKind();

            switch (kind) {
                case SELECT:
                    visitSelect((SqlSelect) call);
                    break;

                case JOIN:
                    visitJoin((SqlJoin) call);
                    break;

                case EXISTS:
                    visitExists(call);
                    break;

                case INTERSECT:
                    analysis.addGap(createSetOpGap("INTERSECT"));
                    break;

                case EXCEPT:
                    analysis.addGap(createSetOpGap("EXCEPT"));
                    break;

                case WITH:
                    visitWith(call);
                    return null; // Don't visit children again, handled in visitWith

                case ORDER_BY:
                    visitOrderBy(call);
                    // SqlOrderBy can also contain LIMIT (fetch)
                    if (!inCTE && call instanceof SqlOrderBy) {
                        SqlOrderBy orderBy = (SqlOrderBy) call;
                        if (orderBy.fetch != null) {
                            outerQueryHasLimit = true;
                        }
                    }
                    break;

                case ROW_NUMBER:
                case RANK:
                case DENSE_RANK:
                    // These require OVER clause to be window functions
                    break;

                default:
                    break;
            }

            // Continue visiting children
            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }

            return null;
        }

        @Override
        public Void visit(SqlIdentifier id) {
            // Collect potential table names
            if (id.names.size() == 1) {
                String name = id.names.get(0).toLowerCase();
                if (!isKeyword(name)) {
                    tableNames.add(name);
                }
            }
            return null;
        }

        private void visitSelect(SqlSelect select) {
            // Track subquery depth
            subqueryDepth++;
            boolean wasInSubquery = inSubquery;
            boolean prevWindowPartition = hasWindowWithPartition;
            if (subqueryDepth > 1) {
                inSubquery = true;
            }

            // Check FROM clause for implicit joins (comma-separated tables)
            SqlNode from = select.getFrom();
            if (from != null) {
                int tableCount = countTablesInFrom(from);
                if (tableCount > 1 && !(from instanceof SqlJoin)) {
                    // Implicit join: FROM a, b, c
                    implicitJoinCount = tableCount - 1;
                }

                // Check if FROM is a subquery - need to analyze it first for window functions
                if (from instanceof SqlSelect || (from instanceof SqlBasicCall &&
                    ((SqlBasicCall) from).getKind() == SqlKind.AS)) {
                    // Visit FROM subquery first to detect window functions
                    from.accept(this);
                }
            }

            // Check for LIMIT (FETCH)
            if (select.getFetch() != null) {
                if (!inCTE) {
                    outerQueryHasLimit = true;
                }
            }

            // Check for window functions with PARTITION BY in select list
            SqlNodeList selectList = select.getSelectList();
            if (selectList != null) {
                for (SqlNode node : selectList) {
                    checkForWindowFunction(node);
                }
            }

            // Check WHERE for rank filter pattern (grouped TopN)
            // This detects: SELECT * FROM (SELECT ..., ROW_NUMBER() OVER (PARTITION BY ...) as rn ...) WHERE rn <= 3
            if (select.getWhere() != null) {
                // Check if this outer query filters on a rank column
                // and the FROM is a subquery with window function
                if (subqueryHasWindowPartition || hasWindowWithPartition) {
                    checkRankFilter(select.getWhere());
                }
            }

            // Remember if this subquery had a window partition for outer query check
            if (inSubquery && hasWindowWithPartition) {
                subqueryHasWindowPartition = true;
            }

            // Visit children (but not FROM again if already visited)
            if (select.getSelectList() != null) {
                select.getSelectList().accept(this);
            }
            if (select.getWhere() != null) {
                select.getWhere().accept(this);
            }
            if (select.getGroup() != null) {
                select.getGroup().accept(this);
            }
            if (select.getHaving() != null) {
                select.getHaving().accept(this);
            }
            // Only visit FROM if not already a subquery (already visited above)
            if (from != null && !(from instanceof SqlSelect) &&
                !(from instanceof SqlBasicCall && ((SqlBasicCall) from).getKind() == SqlKind.AS)) {
                from.accept(this);
            }

            subqueryDepth--;
            inSubquery = wasInSubquery;
            // Keep window partition flag if we found one
            if (!prevWindowPartition && hasWindowWithPartition) {
                subqueryHasWindowPartition = true;
            }
        }

        private void checkForWindowFunction(SqlNode node) {
            if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;

                // Check if this is a window function call (has OVER)
                for (SqlNode operand : call.getOperandList()) {
                    if (operand instanceof SqlCall) {
                        SqlCall innerCall = (SqlCall) operand;
                        if (innerCall.getKind() == SqlKind.OVER) {
                            // Found OVER clause - check for PARTITION BY
                            for (SqlNode overOperand : innerCall.getOperandList()) {
                                if (overOperand instanceof SqlWindow) {
                                    SqlWindow window = (SqlWindow) overOperand;
                                    if (window.getPartitionList() != null &&
                                        window.getPartitionList().size() > 0) {
                                        hasWindowWithPartition = true;
                                    }
                                }
                            }
                        }
                    }
                    checkForWindowFunction(operand);
                }

                // Direct OVER check
                if (call.getKind() == SqlKind.OVER) {
                    for (SqlNode operand : call.getOperandList()) {
                        if (operand instanceof SqlWindow) {
                            SqlWindow window = (SqlWindow) operand;
                            if (window.getPartitionList() != null &&
                                window.getPartitionList().size() > 0) {
                                hasWindowWithPartition = true;
                            }
                        }
                    }
                }
            }
        }

        private void checkRankFilter(SqlNode where) {
            // Look for patterns like: rn <= 3, rank < 5, rownum = 1
            RankFilterChecker checker = new RankFilterChecker();
            where.accept(checker);

            if (checker.hasRankFilter) {
                analysis.addGap(new DetectedGap(
                    Gap.GROUPED_TOPN,
                    "Window function with PARTITION BY and row filter detected",
                    "DuckDB performs full sort per group instead of heap-based TopN",
                    List.of("PROJECT_WINDOW_TRANSPOSE", "FILTER_PROJECT_TRANSPOSE", "SORT_PROJECT_TRANSPOSE"),
                    OptimizerType.HEP,
                    "Consider rewriting to QUALIFY clause or LATERAL join if NDV(partition_col) is small"
                ));
            }
        }

        private int countTablesInFrom(SqlNode from) {
            if (from == null) return 0;

            if (from instanceof SqlIdentifier) {
                return 1;
            } else if (from instanceof SqlJoin) {
                SqlJoin join = (SqlJoin) from;
                return countTablesInFrom(join.getLeft()) + countTablesInFrom(join.getRight());
            } else if (from instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) from;
                // AS clause: table AS alias
                if (call.getKind() == SqlKind.AS) {
                    return 1;
                }
                // Comma-join represented as operator
                int count = 0;
                for (SqlNode operand : call.getOperandList()) {
                    count += countTablesInFrom(operand);
                }
                return count > 0 ? count : 1;
            } else if (from instanceof SqlSelect) {
                return 1; // Subquery counts as one "table"
            }
            return 1;
        }

        private void visitJoin(SqlJoin join) {
            explicitJoinCount++;

            JoinType joinType = join.getJoinType();
            if (joinType == JoinType.LEFT || joinType == JoinType.FULL) {
                leftJoinCount++;
            }

            // Check for multiple LEFT JOINs
            if (leftJoinCount >= 3 && !analysis.hasGap(Gap.MULTIPLE_LEFT_JOINS)) {
                analysis.addGap(new DetectedGap(
                    Gap.MULTIPLE_LEFT_JOINS,
                    leftJoinCount + " LEFT/FULL OUTER JOINs detected",
                    "DuckDB shows exponential performance degradation with multiple outer joins (Issue #14354)",
                    List.of("FILTER_INTO_JOIN", "JOIN_CONDITION_PUSH", "JOIN_PUSH_TRANSITIVE_PREDICATES"),
                    OptimizerType.VOLCANO,
                    "Check for null-rejecting predicates that allow conversion to INNER JOIN"
                ));
            }
        }

        private void visitExists(SqlCall existsCall) {
            if (existsCall.getOperandList().isEmpty()) return;

            SqlNode subquery = existsCall.getOperandList().get(0);

            // Check for inequality predicates in subquery
            InequalityFinder finder = new InequalityFinder();
            subquery.accept(finder);

            if (finder.hasInequality) {
                analysis.addGap(new DetectedGap(
                    Gap.SEMI_JOIN_INEQUALITY,
                    "EXISTS subquery with inequality predicate (" + finder.inequalityOp + ")",
                    "DuckDB may not push inequality into semi-join efficiently (Issue #4950, #19213)",
                    List.of("PROJECT_TO_SEMI_JOIN", "JOIN_TO_SEMI_JOIN",
                           "SEMI_JOIN_FILTER_TRANSPOSE", "JOIN_CONDITION_PUSH"),
                    OptimizerType.HEP
                ));
            }
        }

        private void visitWith(SqlCall withCall) {
            // WITH clause structure:
            // operand 0: SqlNodeList of WITH items
            // operand 1: The main query

            List<SqlNode> operands = withCall.getOperandList();

            // Visit CTE definitions first
            inCTE = true;
            if (operands.size() > 0 && operands.get(0) != null) {
                // Check each WITH item for ORDER BY
                SqlNode withItems = operands.get(0);
                checkCTEForOrderBy(withItems);
                withItems.accept(this);
            }
            inCTE = false;

            // Visit main query
            if (operands.size() > 1 && operands.get(1) != null) {
                operands.get(1).accept(this);
            }

            // Check for CTE + LIMIT gap
            if (cteHasOrderBy && outerQueryHasLimit && !analysis.hasGap(Gap.CTE_LIMIT)) {
                analysis.addGap(new DetectedGap(
                    Gap.CTE_LIMIT,
                    "CTE with ORDER BY inside, LIMIT outside",
                    "DuckDB materializes CTE with full sort, doesn't push LIMIT for TopN (Issue #11260)",
                    List.of("SORT_PROJECT_TRANSPOSE", "LIMIT_MERGE", "SORT_REMOVE_REDUNDANT"),
                    OptimizerType.HEP,
                    "Consider inlining CTE or moving LIMIT inside"
                ));
            }
        }

        private void checkCTEForOrderBy(SqlNode node) {
            if (node == null) return;

            if (node instanceof SqlNodeList) {
                for (SqlNode item : (SqlNodeList) node) {
                    checkCTEForOrderBy(item);
                }
            } else if (node instanceof SqlOrderBy) {
                // SqlOrderBy wraps a SELECT with ORDER BY - this is what we're looking for
                cteHasOrderBy = true;
            } else if (node instanceof SqlCall) {
                SqlCall call = (SqlCall) node;
                if (call.getKind() == SqlKind.ORDER_BY) {
                    cteHasOrderBy = true;
                } else if (call.getKind() == SqlKind.WITH_ITEM) {
                    // WITH item: operand 0 = name, operand 1 = column list, operand 2 = query
                    List<SqlNode> ops = call.getOperandList();
                    if (ops.size() > 2 && ops.get(2) != null) {
                        checkCTEForOrderBy(ops.get(2));
                    }
                } else {
                    // Recurse into children
                    for (SqlNode op : call.getOperandList()) {
                        checkCTEForOrderBy(op);
                    }
                }
            }
        }

        private void visitOrderBy(SqlCall orderBy) {
            if (inCTE) {
                cteHasOrderBy = true;
            }
        }

        /**
         * Called after all visiting is done to check multi-table joins.
         */
        void finalizeAnalysis() {
            int totalJoins = explicitJoinCount + implicitJoinCount;

            // Check for join order gap with large tables
            if (totalJoins >= 2 && !analysis.hasGap(Gap.JOIN_ORDER)) {
                String largestTable = null;
                long maxRows = 0;

                for (String table : tableNames) {
                    long rows = statistics.getRowCount(table);
                    if (rows > 1_000_000) {
                        if (rows > maxRows) {
                            maxRows = rows;
                            largestTable = table;
                        }
                    }
                }

                if (largestTable != null) {
                    analysis.addGap(new DetectedGap(
                        Gap.JOIN_ORDER,
                        (totalJoins + 1) + "-way join with large table (" + largestTable + ": " +
                            String.format("%,d", maxRows) + " rows)",
                        "DuckDB's cardinality estimator may choose suboptimal join order (Issue #3525)",
                        List.of("JOIN_COMMUTE", "JOIN_ASSOCIATE", "JOIN_TO_MULTI_JOIN",
                               "MULTI_JOIN_OPTIMIZE", "FILTER_INTO_JOIN"),
                        OptimizerType.VOLCANO,
                        "Large tables should be filtered first, joined last"
                    ));
                }
            }
        }

        private boolean isKeyword(String word) {
            Set<String> keywords = Set.of(
                "select", "from", "where", "and", "or", "on", "as", "by", "order",
                "group", "having", "limit", "offset", "union", "except", "join",
                "intersect", "inner", "outer", "left", "right", "full", "cross",
                "case", "when", "then", "else", "end", "null", "true", "false",
                "not", "in", "is", "like", "between", "exists", "all", "any",
                "sum", "count", "avg", "min", "max", "distinct"
            );
            return keywords.contains(word.toLowerCase());
        }
    }

    /**
     * Visitor to find inequality operators in predicates.
     */
    private static class InequalityFinder extends SqlBasicVisitor<Void> {
        boolean hasInequality = false;
        String inequalityOp = null;

        @Override
        public Void visit(SqlCall call) {
            SqlKind kind = call.getKind();

            if (kind == SqlKind.NOT_EQUALS) {
                hasInequality = true;
                inequalityOp = "<>";
            } else if (kind == SqlKind.LESS_THAN || kind == SqlKind.GREATER_THAN ||
                       kind == SqlKind.LESS_THAN_OR_EQUAL || kind == SqlKind.GREATER_THAN_OR_EQUAL) {
                if (!hasInequality) {
                    hasInequality = true;
                    inequalityOp = call.getOperator().getName();
                }
            }

            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }
    }

    /**
     * Visitor to check for rank/rownum filter patterns.
     */
    private static class RankFilterChecker extends SqlBasicVisitor<Void> {
        boolean hasRankFilter = false;

        @Override
        public Void visit(SqlCall call) {
            SqlKind kind = call.getKind();

            if (kind == SqlKind.LESS_THAN_OR_EQUAL || kind == SqlKind.LESS_THAN ||
                kind == SqlKind.EQUALS || kind == SqlKind.GREATER_THAN_OR_EQUAL) {
                for (SqlNode operand : call.getOperandList()) {
                    if (operand instanceof SqlIdentifier) {
                        String name = ((SqlIdentifier) operand).getSimple().toLowerCase();
                        if (name.equals("rn") || name.equals("rank") || name.equals("rnk") ||
                            name.equals("rownum") || name.equals("row_num") || name.equals("row_number")) {
                            hasRankFilter = true;
                        }
                    }
                }
            }

            for (SqlNode operand : call.getOperandList()) {
                if (operand != null) {
                    operand.accept(this);
                }
            }
            return null;
        }
    }

    /**
     * Detect issues from EXPLAIN plan output.
     */
    private void detectPlanIssues(String explain, GapAnalysis analysis) {
        String lower = explain.toLowerCase();

        // Nested loop with large rows
        if (lower.contains("nested_loop") || lower.contains("nested loop")) {
            long estimatedRows = extractMaxRowEstimate(explain);
            if (estimatedRows > 10000) {
                analysis.addPlanIssue(
                    "NESTED_LOOP with large input (~" + String.format("%,d", estimatedRows) + " rows)",
                    "Nested loop is O(n*m) - consider join reordering"
                );

                if (!analysis.hasGap(Gap.JOIN_ORDER)) {
                    analysis.addGap(new DetectedGap(
                        Gap.JOIN_ORDER,
                        "NESTED_LOOP join with large input detected in EXPLAIN",
                        "Plan shows inefficient nested loop - join reordering may help",
                        List.of("JOIN_COMMUTE", "JOIN_ASSOCIATE", "MULTI_JOIN_OPTIMIZE"),
                        OptimizerType.VOLCANO
                    ));
                }
            }
        }

        // Window with large input
        if (lower.contains("window")) {
            long windowInput = extractRowEstimateNear(explain, "window");
            if (windowInput > 100000) {
                analysis.addPlanIssue(
                    "WINDOW operator processing ~" + String.format("%,d", windowInput) + " rows",
                    "Large window operation - may benefit from QUALIFY or LATERAL rewrite"
                );

                // If we see a large window and haven't detected grouped TopN from AST,
                // it might still be a grouped TopN pattern
                if (!analysis.hasGap(Gap.GROUPED_TOPN)) {
                    // Check if there's also a limit in the query
                    if (lower.contains("limit") || lower.contains("top")) {
                        analysis.addGap(new DetectedGap(
                            Gap.GROUPED_TOPN,
                            "Large WINDOW operation with LIMIT detected in EXPLAIN",
                            "Potential grouped TopN pattern - DuckDB may not optimize",
                            List.of("PROJECT_WINDOW_TRANSPOSE", "FILTER_PROJECT_TRANSPOSE"),
                            OptimizerType.HEP,
                            "Consider QUALIFY clause or LATERAL join"
                        ));
                    }
                }
            }
        }

        // Full sort before limit (should be TopN)
        if ((lower.contains("order_by") || lower.contains("sort"))
            && lower.contains("limit")
            && !lower.contains("top_n") && !lower.contains("topn")) {
            analysis.addPlanIssue(
                "ORDER BY + LIMIT without TopN optimization",
                "DuckDB should use TopN operator but is doing full sort"
            );
        }
    }

    private long extractMaxRowEstimate(String explain) {
        long max = 0;
        String[] parts = explain.split("\\s+");
        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i].replaceAll("[~,]", "");
            if (parts[i + 1].toLowerCase().startsWith("row")) {
                try {
                    long val = Long.parseLong(part);
                    if (val > max) max = val;
                } catch (NumberFormatException ignored) {}
            }
        }
        return max;
    }

    private long extractRowEstimateNear(String explain, String keyword) {
        int idx = explain.toLowerCase().indexOf(keyword);
        if (idx < 0) return 0;
        String context = explain.substring(Math.max(0, idx - 100),
            Math.min(explain.length(), idx + 100));
        return extractMaxRowEstimate(context);
    }

    private static DetectedGap createSetOpGap(String op) {
        return new DetectedGap(
            Gap.SET_OP_NO_SHORTCIRCUIT,
            op + " operation detected",
            "DuckDB doesn't short-circuit when one input is empty (Issue #18121)",
            List.of(op.equals("INTERSECT") ? "INTERSECT_MERGE" : "MINUS_MERGE",
                   "INTERSECT_TO_DISTINCT"),
            OptimizerType.HEP,
            "Only short-circuit if input is PROVABLY empty (not estimated)"
        );
    }

    // ==================== Data Classes ====================

    public enum Gap {
        JOIN_ORDER("Join Order", "Phase 1"),
        SEMI_JOIN_INEQUALITY("Semi-Join Inequality", "Phase 2"),
        GROUPED_TOPN("Grouped TopN", "Phase 3"),
        CTE_LIMIT("CTE + LIMIT", "Phase 4"),
        SET_OP_NO_SHORTCIRCUIT("Set Op Short-Circuit", "Phase 5"),
        MULTIPLE_LEFT_JOINS("Multiple LEFT JOINs", "Phase 6");

        private final String displayName;
        private final String phase;

        Gap(String displayName, String phase) {
            this.displayName = displayName;
            this.phase = phase;
        }

        public String getDisplayName() { return displayName; }
        public String getPhase() { return phase; }
    }

    public enum OptimizerType {
        HEP,      // Pattern-based, deterministic
        VOLCANO   // Cost-based, uses statistics
    }

    public static class DetectedGap {
        private final Gap gap;
        private final String description;
        private final String reason;
        private final List<String> recommendedRules;
        private final OptimizerType optimizerType;
        private final String note;

        public DetectedGap(Gap gap, String description, String reason,
                          List<String> recommendedRules, OptimizerType optimizerType) {
            this(gap, description, reason, recommendedRules, optimizerType, null);
        }

        public DetectedGap(Gap gap, String description, String reason,
                          List<String> recommendedRules, OptimizerType optimizerType, String note) {
            this.gap = gap;
            this.description = description;
            this.reason = reason;
            this.recommendedRules = recommendedRules;
            this.optimizerType = optimizerType;
            this.note = note;
        }

        public Gap getGap() { return gap; }
        public String getDescription() { return description; }
        public String getReason() { return reason; }
        public List<String> getRecommendedRules() { return recommendedRules; }
        public OptimizerType getOptimizerType() { return optimizerType; }
        public String getNote() { return note; }
    }

    public static class GapAnalysis {
        private final String sql;
        private final List<DetectedGap> gaps = new ArrayList<>();
        private final List<String> planIssues = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();
        private String explainPlan;

        public GapAnalysis(String sql) {
            this.sql = sql;
        }

        public void addGap(DetectedGap gap) {
            if (gaps.stream().noneMatch(g -> g.getGap() == gap.getGap())) {
                gaps.add(gap);
            }
        }

        public void addPlanIssue(String issue, String suggestion) {
            planIssues.add(issue + " → " + suggestion);
        }

        public void addWarning(String warning) {
            warnings.add(warning);
        }

        public void setExplainPlan(String plan) {
            this.explainPlan = plan;
        }

        public boolean hasGap(Gap gap) {
            return gaps.stream().anyMatch(g -> g.getGap() == gap);
        }

        public String getSql() { return sql; }
        public List<DetectedGap> getGaps() { return gaps; }
        public List<String> getPlanIssues() { return planIssues; }
        public List<String> getWarnings() { return warnings; }
        public String getExplainPlan() { return explainPlan; }

        public boolean hasGaps() { return !gaps.isEmpty(); }

        public List<String> getAllRecommendedRules() {
            Set<String> rules = new LinkedHashSet<>();
            for (DetectedGap gap : gaps) {
                rules.addAll(gap.getRecommendedRules());
            }
            return new ArrayList<>(rules);
        }

        public OptimizerType getRecommendedOptimizer() {
            for (DetectedGap gap : gaps) {
                if (gap.getOptimizerType() == OptimizerType.VOLCANO) {
                    return OptimizerType.VOLCANO;
                }
            }
            return gaps.isEmpty() ? OptimizerType.HEP : gaps.get(0).getOptimizerType();
        }

        public String format() {
            StringBuilder sb = new StringBuilder();
            sb.append("=".repeat(60)).append("\n");
            sb.append("GAP ANALYSIS REPORT\n");
            sb.append("=".repeat(60)).append("\n\n");

            if (gaps.isEmpty()) {
                sb.append("No known DuckDB optimizer gaps detected.\n");
            } else {
                sb.append("DETECTED GAPS (").append(gaps.size()).append("):\n\n");

                for (int i = 0; i < gaps.size(); i++) {
                    DetectedGap g = gaps.get(i);
                    sb.append(String.format("%d. [%s] %s\n", i + 1, g.getGap().getPhase(),
                        g.getGap().getDisplayName()));
                    sb.append("   Description: ").append(g.getDescription()).append("\n");
                    sb.append("   Reason: ").append(g.getReason()).append("\n");
                    sb.append("   Optimizer: ").append(g.getOptimizerType()).append("\n");
                    sb.append("   Rules: ").append(String.join(", ", g.getRecommendedRules())).append("\n");
                    if (g.getNote() != null) {
                        sb.append("   Note: ").append(g.getNote()).append("\n");
                    }
                    sb.append("\n");
                }
            }

            if (!planIssues.isEmpty()) {
                sb.append("PLAN ISSUES:\n");
                for (String issue : planIssues) {
                    sb.append("  - ").append(issue).append("\n");
                }
                sb.append("\n");
            }

            if (!warnings.isEmpty()) {
                sb.append("WARNINGS:\n");
                for (String warning : warnings) {
                    sb.append("  ! ").append(warning).append("\n");
                }
                sb.append("\n");
            }

            sb.append("RECOMMENDATION:\n");
            sb.append("  Optimizer: ").append(getRecommendedOptimizer()).append("\n");
            sb.append("  Rules: ").append(String.join(", ", getAllRecommendedRules())).append("\n");

            return sb.toString();
        }
    }
}
