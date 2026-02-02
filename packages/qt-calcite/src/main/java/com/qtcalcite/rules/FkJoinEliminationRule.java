package com.qtcalcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HEP rule that removes redundant dimension joins used only for FK validation.
 *
 * Pattern matched (within a subtree):
 *   Join(LEFT, RIGHT)  -- INNER equi-join on LEFT.fk = RIGHT.pk
 *
 * Conditions:
 * - Join is INNER and equi-join.
 * - Required columns from RIGHT are only join keys.
 *
 * Transformation:
 *   Project(LEFT.*, RIGHT.keys <- LEFT.keys, RIGHT.other <- NULL)
 *     Filter(LEFT.fk IS NOT NULL)
 *       LEFT
 */
public class FkJoinEliminationRule extends RelOptRule {

    public static final FkJoinEliminationRule INSTANCE = new FkJoinEliminationRule();
    private static final boolean DEBUG = true;

    public FkJoinEliminationRule() {
        super(operand(RelNode.class, any()), "FkJoinEliminationRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode top = call.rel(0);
        if (DEBUG) {
            System.out.println("[FK_JOIN_ELIMINATION] onMatch top=" + top.getClass().getSimpleName());
        }
        if (call.getPlanner() instanceof HepPlanner) {
            RelNode root = ((HepPlanner) call.getPlanner()).getRoot();
            if (root instanceof HepRelVertex) {
                root = ((HepRelVertex) root).getCurrentRel();
            }
            if (top != root) {
                return;
            }
        }
        if (top instanceof LogicalJoin) {
            // Avoid starting from joins; we want required columns from a parent context.
            return;
        }
        RequiredResult rewritten = rewrite(top, allColumns(top));
        if (rewritten.changed) {
            call.transformTo(rewritten.node);
        }
    }

    private RequiredResult rewrite(RelNode node, ImmutableBitSet requiredOutputs) {
        if (node instanceof HepRelVertex) {
            RelNode current = ((HepRelVertex) node).getCurrentRel();
            return rewrite(current, requiredOutputs);
        }
        if (node instanceof LogicalJoin) {
            return rewriteJoin((LogicalJoin) node, requiredOutputs);
        }
        if (node instanceof LogicalProject) {
            return rewriteProject((LogicalProject) node, requiredOutputs);
        }
        if (node instanceof LogicalAggregate) {
            return rewriteAggregate((LogicalAggregate) node, requiredOutputs);
        }
        if (node instanceof LogicalFilter) {
            return rewriteFilter((LogicalFilter) node, requiredOutputs);
        }
        if (node instanceof LogicalSort) {
            return rewriteSort((LogicalSort) node, requiredOutputs);
        }
        if (node instanceof LogicalUnion) {
            return rewriteUnion((LogicalUnion) node, requiredOutputs);
        }
        return new RequiredResult(node, false);
    }

    private RequiredResult rewriteProject(LogicalProject project, ImmutableBitSet requiredOutputs) {
        List<RexNode> newProjects = new ArrayList<>();
        boolean subQueryChanged = false;
        for (RexNode node : project.getProjects()) {
            SubQueryRewriteResult sub = rewriteSubQueries(node);
            newProjects.add(sub.node);
            subQueryChanged |= sub.changed;
        }

        ImmutableBitSet requiredInputs = collectInputRefs(newProjects, requiredOutputs);
        RequiredResult child = rewrite(project.getInput(), requiredInputs);
        if (!child.changed && !subQueryChanged) {
            return new RequiredResult(project, false);
        }
        LogicalProject rewritten = LogicalProject.create(
            child.node,
            Collections.emptyList(),
            newProjects,
            project.getRowType().getFieldNames()
        );
        return new RequiredResult(rewritten, true);
    }

    private RequiredResult rewriteAggregate(LogicalAggregate aggregate, ImmutableBitSet requiredOutputs) {
        int groupCount = aggregate.getGroupSet().cardinality();
        ImmutableBitSet.Builder requiredInputs = ImmutableBitSet.builder();

        for (int i = 0; i < groupCount; i++) {
            if (requiredOutputs.get(i)) {
                requiredInputs.set(aggregate.getGroupSet().nth(i));
            }
        }

        List<AggregateCall> calls = aggregate.getAggCallList();
        for (int i = 0; i < calls.size(); i++) {
            int outputIndex = groupCount + i;
            if (!requiredOutputs.get(outputIndex)) {
                continue;
            }
            AggregateCall call = calls.get(i);
            for (int arg : call.getArgList()) {
                requiredInputs.set(arg);
            }
            if (call.filterArg >= 0) {
                requiredInputs.set(call.filterArg);
            }
            if (call.distinctKeys != null) {
                requiredInputs.addAll(call.distinctKeys);
            }
        }

        RequiredResult child = rewrite(aggregate.getInput(), requiredInputs.build());
        if (!child.changed) {
            return new RequiredResult(aggregate, false);
        }
        LogicalAggregate rewritten = aggregate.copy(
            aggregate.getTraitSet(),
            child.node,
            aggregate.getGroupSet(),
            aggregate.getGroupSets(),
            aggregate.getAggCallList()
        );
        return new RequiredResult(rewritten, true);
    }

    private RequiredResult rewriteFilter(LogicalFilter filter, ImmutableBitSet requiredOutputs) {
        SubQueryRewriteResult sub = rewriteSubQueries(filter.getCondition());
        RexNode condition = sub.node;
        ImmutableBitSet conditionRefs = collectInputRefs(condition);
        ImmutableBitSet requiredInputs = requiredOutputs.union(conditionRefs);
        RequiredResult child = rewrite(filter.getInput(), requiredInputs);
        if (!child.changed && !sub.changed) {
            return new RequiredResult(filter, false);
        }
        LogicalFilter rewritten = LogicalFilter.create(child.node, condition);
        return new RequiredResult(rewritten, true);
    }

    private RequiredResult rewriteSort(LogicalSort sort, ImmutableBitSet requiredOutputs) {
        ImmutableBitSet.Builder requiredInputs = ImmutableBitSet.builder();
        requiredInputs.addAll(requiredOutputs);
        sort.getCollation().getFieldCollations().forEach(fc -> requiredInputs.set(fc.getFieldIndex()));
        RequiredResult child = rewrite(sort.getInput(), requiredInputs.build());
        if (!child.changed) {
            return new RequiredResult(sort, false);
        }
        RelNode rewritten = sort.copy(sort.getTraitSet(), child.node, sort.getCollation(), sort.offset, sort.fetch);
        return new RequiredResult(rewritten, true);
    }

    private RequiredResult rewriteUnion(LogicalUnion union, ImmutableBitSet requiredOutputs) {
        boolean changed = false;
        List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : union.getInputs()) {
            RequiredResult child = rewrite(input, requiredOutputs);
            newInputs.add(child.node);
            changed |= child.changed;
        }
        if (!changed) {
            return new RequiredResult(union, false);
        }
        LogicalUnion rewritten = LogicalUnion.create(newInputs, union.all);
        return new RequiredResult(rewritten, true);
    }

    private RequiredResult rewriteJoin(LogicalJoin join, ImmutableBitSet requiredOutputs) {
        JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());
        if (DEBUG) {
            String rightName = getTableName(join.getRight());
            System.out.println("[FK_JOIN_ELIMINATION] Seen join right=" + rightName
                + " joinType=" + join.getJoinType());
        }
        if (join.getJoinType() == JoinRelType.INNER && joinInfo.isEqui()) {
            int leftCount = join.getLeft().getRowType().getFieldCount();
            ImmutableBitSet requiredLeft = requiredOutputs.intersect(ImmutableBitSet.range(0, leftCount));
            ImmutableBitSet requiredRight = shift(requiredOutputs, -leftCount)
                .intersect(ImmutableBitSet.range(0, join.getRight().getRowType().getFieldCount()));

            ImmutableBitSet conditionRefs = collectInputRefs(join.getCondition());
            ImmutableBitSet conditionLeft = conditionRefs.intersect(ImmutableBitSet.range(0, leftCount));
            ImmutableBitSet conditionRight = shift(conditionRefs, -leftCount)
                .intersect(ImmutableBitSet.range(0, join.getRight().getRowType().getFieldCount()));

            requiredLeft = requiredLeft.union(conditionLeft);
            requiredRight = requiredRight.union(conditionRight);

            Set<Integer> rightKeys = new HashSet<>(joinInfo.rightKeys);
            if (DEBUG) {
                System.out.println("[FK_JOIN_ELIMINATION] requiredRight=" + requiredRight
                    + " rightKeys=" + rightKeys + " conditionRight=" + conditionRight);
            }
            boolean rightOnlyKeys = true;
            for (int idx : requiredRight) {
                if (!rightKeys.contains(idx)) {
                    rightOnlyKeys = false;
                    break;
                }
            }

            if (rightOnlyKeys) {
                RelNode replacement = replaceJoinWithLeft(join, joinInfo);
                return new RequiredResult(replacement, true);
            }
        }

        ImmutableBitSet requiredLeft = requiredOutputs.intersect(
            ImmutableBitSet.range(0, join.getLeft().getRowType().getFieldCount()));
        ImmutableBitSet requiredRight = shift(requiredOutputs, -join.getLeft().getRowType().getFieldCount())
            .intersect(ImmutableBitSet.range(0, join.getRight().getRowType().getFieldCount()));
        ImmutableBitSet conditionRefs = collectInputRefs(join.getCondition());
        ImmutableBitSet conditionLeft = conditionRefs.intersect(
            ImmutableBitSet.range(0, join.getLeft().getRowType().getFieldCount()));
        ImmutableBitSet conditionRight = shift(conditionRefs, -join.getLeft().getRowType().getFieldCount())
            .intersect(ImmutableBitSet.range(0, join.getRight().getRowType().getFieldCount()));

        requiredLeft = requiredLeft.union(conditionLeft);
        requiredRight = requiredRight.union(conditionRight);

        RequiredResult newLeft = rewrite(join.getLeft(), requiredLeft);
        RequiredResult newRight = rewrite(join.getRight(), requiredRight);
        if (!newLeft.changed && !newRight.changed) {
            return new RequiredResult(join, false);
        }
        LogicalJoin rewritten = LogicalJoin.create(
            newLeft.node,
            newRight.node,
            Collections.emptyList(),
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType()
        );
        return new RequiredResult(rewritten, true);
    }

    private RelNode replaceJoinWithLeft(LogicalJoin join, JoinInfo joinInfo) {
        RelNode left = join.getLeft();
        RelDataType leftRowType = left.getRowType();
        RelDataType rightRowType = join.getRight().getRowType();
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        RexNode notNullFilter = null;
        for (int leftKey : joinInfo.leftKeys) {
            if (!leftRowType.getFieldList().get(leftKey).getType().isNullable()) {
                continue;
            }
            RexNode isNotNull = rexBuilder.makeCall(
                SqlStdOperatorTable.IS_NOT_NULL,
                RexInputRef.of(leftKey, leftRowType)
            );
            notNullFilter = notNullFilter == null
                ? isNotNull
                : rexBuilder.makeCall(SqlStdOperatorTable.AND, notNullFilter, isNotNull);
        }

        RelNode filteredLeft = notNullFilter == null
            ? left
            : LogicalFilter.create(left, notNullFilter);

        Map<Integer, Integer> rightToLeftKey = new HashMap<>();
        for (int i = 0; i < joinInfo.rightKeys.size(); i++) {
            rightToLeftKey.put(joinInfo.rightKeys.get(i), joinInfo.leftKeys.get(i));
        }

        List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < leftRowType.getFieldCount(); i++) {
            projects.add(RexInputRef.of(i, filteredLeft.getRowType()));
        }
        for (int i = 0; i < rightRowType.getFieldCount(); i++) {
            Integer leftIndex = rightToLeftKey.get(i);
            if (leftIndex != null) {
                projects.add(RexInputRef.of(leftIndex, filteredLeft.getRowType()));
            } else {
                projects.add(rexBuilder.makeNullLiteral(rightRowType.getFieldList().get(i).getType()));
            }
        }

        List<String> names = join.getRowType().getFieldNames();
        return LogicalProject.create(filteredLeft, Collections.emptyList(), projects, names);
    }

    private ImmutableBitSet collectInputRefs(List<RexNode> nodes, ImmutableBitSet outputMask) {
        Set<Integer> refs = new HashSet<>();
        for (int i = 0; i < nodes.size(); i++) {
            if (!outputMask.get(i)) {
                continue;
            }
            RexNode node = nodes.get(i);
            node.accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    refs.add(ref.getIndex());
                    return ref;
                }
            });
        }
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        refs.forEach(builder::set);
        return builder.build();
    }

    private ImmutableBitSet collectInputRefs(RexNode node) {
        Set<Integer> refs = new HashSet<>();
        node.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                refs.add(ref.getIndex());
                return ref;
            }
        });
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        refs.forEach(builder::set);
        return builder.build();
    }

    private SubQueryRewriteResult rewriteSubQueries(RexNode node) {
        boolean[] changed = new boolean[]{false};
        RexNode rewritten = node.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery sub) {
                if (DEBUG) {
                    System.out.println("[FK_JOIN_ELIMINATION] Visiting subquery rel=" + sub.rel.getClass().getSimpleName());
                }
                RequiredResult subRel = rewrite(sub.rel, requiredOutputsForSubQuery(sub));
                if (subRel.changed) {
                    changed[0] = true;
                    return sub.clone(subRel.node);
                }
                return sub;
            }

            @Override
            public RexNode visitCall(org.apache.calcite.rex.RexCall call) {
                return super.visitCall(call);
            }
        });
        return new SubQueryRewriteResult(rewritten, changed[0]);
    }

    private ImmutableBitSet allColumns(RelNode node) {
        return ImmutableBitSet.range(0, node.getRowType().getFieldCount());
    }

    private ImmutableBitSet requiredOutputsForSubQuery(RexSubQuery sub) {
        int fieldCount = sub.rel.getRowType().getFieldCount();
        SqlKind kind = sub.getKind();
        if (kind == SqlKind.EXISTS) {
            return ImmutableBitSet.of();
        }
        if (kind == SqlKind.IN || kind == SqlKind.SCALAR_QUERY
            || kind == SqlKind.SOME || kind == SqlKind.ALL) {
            return fieldCount == 0 ? ImmutableBitSet.of() : ImmutableBitSet.of(0);
        }
        return allColumns(sub.rel);
    }

    private ImmutableBitSet shift(ImmutableBitSet bitSet, int delta) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int index : bitSet) {
            int shifted = index + delta;
            if (shifted >= 0) {
                builder.set(shifted);
            }
        }
        return builder.build();
    }

    private static class RequiredResult {
        final RelNode node;
        final boolean changed;

        RequiredResult(RelNode node, boolean changed) {
            this.node = node;
            this.changed = changed;
        }
    }

    private static class SubQueryRewriteResult {
        final RexNode node;
        final boolean changed;

        SubQueryRewriteResult(RexNode node, boolean changed) {
            this.node = node;
            this.changed = changed;
        }
    }

    private String getTableName(RelNode node) {
        if (node instanceof HepRelVertex) {
            node = ((HepRelVertex) node).getCurrentRel();
        }
        if (node instanceof LogicalTableScan) {
            LogicalTableScan scan = (LogicalTableScan) node;
            List<String> names = scan.getTable().getQualifiedName();
            if (!names.isEmpty()) {
                return names.get(names.size() - 1);
            }
        }
        return null;
    }
}
