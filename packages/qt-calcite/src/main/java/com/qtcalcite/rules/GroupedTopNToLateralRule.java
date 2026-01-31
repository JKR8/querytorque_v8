package com.qtcalcite.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import org.apache.calcite.plan.hep.HepRelVertex;

import java.util.*;
import java.util.function.Function;

/**
 * HEP rule that transforms grouped TopN with window functions to LATERAL joins.
 *
 * Pattern matched:
 *   Filter(rn <= N)
 *     Project(..., ROW_NUMBER() OVER (PARTITION BY x ORDER BY y) AS rn)
 *       [Join/TableScan]
 *
 * Transformed to:
 *   Project (output columns)
 *     Correlate (LATERAL)
 *       Aggregate(DISTINCT partition_col)  -- get distinct groups
 *       Sort(LIMIT N)
 *         Filter(partition_col = $cor.partition_col)
 *           [original input]
 *
 * This enables early termination via LIMIT for each group.
 */
public class GroupedTopNToLateralRule extends RelOptRule {

    public static final GroupedTopNToLateralRule INSTANCE =
        new GroupedTopNToLateralRule(null);

    private static final long DEFAULT_NDV_THRESHOLD = 10_000;

    private final Function<String, Long> ndvLookup;

    public GroupedTopNToLateralRule(Function<String, Long> ndvLookup) {
        super(operand(LogicalFilter.class,
                operand(LogicalProject.class, any())),
            "GroupedTopNToLateralRule");
        this.ndvLookup = ndvLookup != null ? ndvLookup : (col) -> 1000L;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalFilter filter = call.rel(0);
        final LogicalProject project = call.rel(1);

        // 1. Extract rank filter info (rn <= N)
        RankFilterInfo rankFilter = extractRankFilter(filter.getCondition(), project);
        if (rankFilter == null) {
            return;
        }

        // 2. Extract window function info
        WindowInfo windowInfo = extractWindowInfo(project, rankFilter.rankIndex);
        if (windowInfo == null) {
            return;
        }

        // 3. Check NDV guardrail
        String partitionCol = windowInfo.partitionColumn;
        long ndv = ndvLookup.apply(partitionCol);
        if (ndv > DEFAULT_NDV_THRESHOLD) {
            return;
        }

        // 4. Build the LATERAL (Correlate) transformation
        RelNode transformed = buildLateralRewrite(call, filter, project, windowInfo, rankFilter.limit);
        if (transformed != null) {
            call.transformTo(transformed);
        }
    }

    private RankFilterInfo extractRankFilter(RexNode condition, Project project) {
        if (!(condition instanceof RexCall)) {
            return null;
        }

        RexCall call = (RexCall) condition;
        SqlKind kind = call.getKind();

        if (kind != SqlKind.LESS_THAN_OR_EQUAL && kind != SqlKind.LESS_THAN) {
            return null;
        }

        List<RexNode> operands = call.getOperands();
        if (operands.size() != 2) {
            return null;
        }

        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        if (!(left instanceof RexInputRef) || !(right instanceof RexLiteral)) {
            return null;
        }

        int rankIndex = ((RexInputRef) left).getIndex();
        RexLiteral literal = (RexLiteral) right;

        // Check if numeric
        Object value = literal.getValue();
        if (value == null) {
            return null;
        }

        int limit;
        if (value instanceof Number) {
            limit = ((Number) value).intValue();
        } else {
            return null;
        }

        if (kind == SqlKind.LESS_THAN) {
            limit--;
        }

        if (limit <= 0) {
            return null;
        }

        return new RankFilterInfo(rankIndex, limit);
    }

    private WindowInfo extractWindowInfo(Project project, int rankIndex) {
        List<RexNode> projects = project.getProjects();
        if (rankIndex >= projects.size()) {
            return null;
        }

        RexNode rankExpr = projects.get(rankIndex);
        if (!(rankExpr instanceof RexOver)) {
            return null;
        }

        RexOver over = (RexOver) rankExpr;

        // Check it's ROW_NUMBER, RANK, or DENSE_RANK
        String funcName = over.getAggOperator().getName();
        if (!funcName.equals("ROW_NUMBER") && !funcName.equals("RANK") && !funcName.equals("DENSE_RANK")) {
            return null;
        }

        RexWindow window = over.getWindow();

        // Get partition column
        List<RexNode> partitionKeys = window.partitionKeys;
        if (partitionKeys.isEmpty()) {
            return null;
        }

        String partitionColumn = null;
        int partitionIndex = -1;
        RexNode partitionKey = partitionKeys.get(0);
        if (partitionKey instanceof RexInputRef) {
            partitionIndex = ((RexInputRef) partitionKey).getIndex();
            RelDataTypeField field = project.getInput().getRowType().getFieldList().get(partitionIndex);
            partitionColumn = field.getName();
        }

        if (partitionColumn == null) {
            return null;
        }

        // Get order column
        List<RexFieldCollation> orderKeys = window.orderKeys;
        if (orderKeys.isEmpty()) {
            return null;
        }

        String orderColumn = null;
        int orderIndex = -1;
        boolean orderDesc = false;
        RexFieldCollation orderKey = orderKeys.get(0);
        RexNode orderExpr = orderKey.left;
        if (orderExpr instanceof RexInputRef) {
            orderIndex = ((RexInputRef) orderExpr).getIndex();
            RelDataTypeField field = project.getInput().getRowType().getFieldList().get(orderIndex);
            orderColumn = field.getName();
            orderDesc = orderKey.getDirection().isDescending();
        }

        if (orderColumn == null) {
            return null;
        }

        // Collect non-rank columns for output
        List<Integer> outputColumns = new ArrayList<>();
        for (int i = 0; i < projects.size(); i++) {
            if (i != rankIndex) {
                outputColumns.add(i);
            }
        }

        return new WindowInfo(partitionColumn, partitionIndex, orderColumn, orderIndex, orderDesc, outputColumns);
    }

    /**
     * Build the LATERAL rewrite.
     *
     * Strategy: Create a Correlate where:
     * - Left side: Aggregate(DISTINCT partition_col) from original input
     * - Right side: Sort(LIMIT N) with correlation filter
     */
    private RelNode buildLateralRewrite(RelOptRuleCall call, LogicalFilter filter,
                                        LogicalProject project, WindowInfo windowInfo, int limit) {
        RelOptCluster cluster = filter.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelNode input = project.getInput();

        try {
            // Create correlation ID
            CorrelationId correlationId = cluster.createCorrel();

            // Build left side: get distinct partition values
            RelNode leftSide = buildDistinctPartition(cluster, rexBuilder, input, windowInfo.partitionIndex);

            // Build right side: correlated subquery with filter and LIMIT
            RelNode rightSide = buildCorrelatedSubquery(cluster, rexBuilder, input,
                windowInfo, limit, correlationId);

            // Create the Correlate (LATERAL join)
            ImmutableBitSet requiredColumns = ImmutableBitSet.of(0); // partition column
            LogicalCorrelate correlate = LogicalCorrelate.create(
                leftSide,
                rightSide,
                correlationId,
                requiredColumns,
                JoinRelType.INNER
            );

            // Build output projection to match ORIGINAL output type (including rank column placeholder)
            return buildOutputProjectMatchingOriginal(cluster, rexBuilder, correlate, filter, windowInfo, limit);

        } catch (Exception e) {
            // If anything goes wrong, don't transform
            return null;
        }
    }

    /**
     * Build: SELECT DISTINCT partition_col FROM input
     */
    private RelNode buildDistinctPartition(RelOptCluster cluster, RexBuilder rexBuilder,
                                           RelNode input, int partitionIndex) {
        // Project just the partition column
        RelDataType inputType = input.getRowType();
        RelDataTypeField partitionField = inputType.getFieldList().get(partitionIndex);

        List<RexNode> projects = Collections.singletonList(
            rexBuilder.makeInputRef(partitionField.getType(), partitionIndex)
        );
        List<String> names = Collections.singletonList(partitionField.getName());

        RelNode projected = LogicalProject.create(input, Collections.emptyList(), projects, names);

        // Make it distinct via Aggregate
        return org.apache.calcite.rel.logical.LogicalAggregate.create(
            projected,
            Collections.emptyList(),
            ImmutableBitSet.of(0),
            Collections.emptyList(),
            Collections.emptyList()
        );
    }

    /**
     * Build correlated subquery: SELECT ... WHERE partition = $cor.partition ORDER BY ... LIMIT N
     */
    private RelNode buildCorrelatedSubquery(RelOptCluster cluster, RexBuilder rexBuilder,
                                            RelNode input, WindowInfo windowInfo, int limit,
                                            CorrelationId correlationId) {
        RelDataType inputType = input.getRowType();

        // Build correlation variable reference
        RelDataTypeField partitionField = inputType.getFieldList().get(windowInfo.partitionIndex);
        RexNode correlationRef = rexBuilder.makeCorrel(
            cluster.getTypeFactory().createStructType(
                Collections.singletonList(partitionField.getType()),
                Collections.singletonList(partitionField.getName())
            ),
            correlationId
        );

        // Build filter condition: partition_col = $cor.partition_col
        RexNode partitionRef = rexBuilder.makeInputRef(input, windowInfo.partitionIndex);
        RexNode correlatedField = rexBuilder.makeFieldAccess(correlationRef, 0);
        RexNode filterCondition = rexBuilder.makeCall(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS,
            partitionRef,
            correlatedField
        );

        // Add correlation filter
        RelNode filtered = LogicalFilter.create(input, filterCondition);

        // Add Sort with LIMIT
        RelFieldCollation fieldCollation = new RelFieldCollation(
            windowInfo.orderIndex,
            windowInfo.orderDesc ? RelFieldCollation.Direction.DESCENDING : RelFieldCollation.Direction.ASCENDING
        );
        RelCollation collation = RelCollations.of(fieldCollation);

        return LogicalSort.create(
            filtered,
            collation,
            null,  // offset
            rexBuilder.makeLiteral(limit, cluster.getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER), false)
        );
    }

    /**
     * Build output projection matching the ORIGINAL Filter's output type exactly.
     * This is required by HEP planner - transformed node must have same type.
     *
     * @param originalProject The original project with window function
     * @param rankIndex The index of the rank column in the project output
     */
    private RelNode buildOutputProjectMatchingOriginal(RelOptCluster cluster, RexBuilder rexBuilder,
                                                       LogicalCorrelate correlate, LogicalFilter originalFilter,
                                                       WindowInfo windowInfo, int limit) {
        // Get the original project (child of filter) - unwrap HepRelVertex if needed
        RelNode filterInput = originalFilter.getInput();
        if (filterInput instanceof HepRelVertex) {
            filterInput = ((HepRelVertex) filterInput).getCurrentRel();
        }
        LogicalProject originalProject = (LogicalProject) filterInput;
        List<RexNode> originalExprs = originalProject.getProjects();

        RelDataType originalType = originalFilter.getRowType();
        int leftFieldCount = 1; // Left side has just the partition column

        List<RexNode> projects = new ArrayList<>();
        List<String> names = new ArrayList<>();

        // For each column in the original output, rebuild the expression
        for (int i = 0; i < originalType.getFieldCount(); i++) {
            RelDataTypeField origField = originalType.getFieldList().get(i);
            names.add(origField.getName());

            RexNode origExpr = originalExprs.get(i);

            if (origExpr instanceof RexOver) {
                // This is the rank/row_number column - replace with literal
                // Since we're using LIMIT, all returned rows satisfy the condition
                projects.add(rexBuilder.makeLiteral(1,
                    cluster.getTypeFactory().createSqlType(org.apache.calcite.sql.type.SqlTypeName.BIGINT), false));
            } else if (origExpr instanceof RexInputRef) {
                // This is a simple column reference - map to correlate's right side
                int inputIdx = ((RexInputRef) origExpr).getIndex();
                // Correlate schema: [left columns (1)] [right columns (original input)]
                int correlateIdx = leftFieldCount + inputIdx;
                projects.add(rexBuilder.makeInputRef(correlate, correlateIdx));
            } else {
                // More complex expression - try to shift input refs
                RexNode shifted = shiftInputRefs(rexBuilder, origExpr, leftFieldCount, correlate.getRowType());
                projects.add(shifted);
            }
        }

        return LogicalProject.create(correlate, Collections.emptyList(), projects, names);
    }

    /**
     * Shift all InputRef indices in an expression by offset.
     */
    private RexNode shiftInputRefs(RexBuilder rexBuilder, RexNode expr, int offset, RelDataType targetType) {
        if (expr instanceof RexInputRef) {
            int newIdx = ((RexInputRef) expr).getIndex() + offset;
            return rexBuilder.makeInputRef(targetType.getFieldList().get(newIdx).getType(), newIdx);
        } else if (expr instanceof RexCall) {
            RexCall call = (RexCall) expr;
            List<RexNode> newOperands = new ArrayList<>();
            for (RexNode operand : call.getOperands()) {
                newOperands.add(shiftInputRefs(rexBuilder, operand, offset, targetType));
            }
            return rexBuilder.makeCall(call.getType(), call.getOperator(), newOperands);
        }
        return expr;
    }

    private static class RankFilterInfo {
        final int rankIndex;
        final int limit;

        RankFilterInfo(int rankIndex, int limit) {
            this.rankIndex = rankIndex;
            this.limit = limit;
        }
    }

    private static class WindowInfo {
        final String partitionColumn;
        final int partitionIndex;
        final String orderColumn;
        final int orderIndex;
        final boolean orderDesc;
        final List<Integer> outputColumns;

        WindowInfo(String partitionColumn, int partitionIndex, String orderColumn,
                   int orderIndex, boolean orderDesc, List<Integer> outputColumns) {
            this.partitionColumn = partitionColumn;
            this.partitionIndex = partitionIndex;
            this.orderColumn = orderColumn;
            this.orderIndex = orderIndex;
            this.orderDesc = orderDesc;
            this.outputColumns = outputColumns;
        }
    }
}
