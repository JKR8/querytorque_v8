package com.qtcalcite.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

/**
 * Custom RelMetadataProvider that injects DuckDB statistics into Calcite's cost model.
 * Provides accurate row counts and selectivity estimates based on real table statistics.
 */
public class DuckDBRelMetadataProvider {

    private final DuckDBStatistics statistics;

    public DuckDBRelMetadataProvider(DuckDBStatistics statistics) {
        this.statistics = statistics;
    }

    /**
     * Creates a chained metadata provider that includes DuckDB statistics.
     */
    public RelMetadataProvider createProvider() {
        // Create handlers for row count and selectivity
        DuckDBRelMdRowCount rowCountHandler = new DuckDBRelMdRowCount(statistics);
        DuckDBRelMdSelectivity selectivityHandler = new DuckDBRelMdSelectivity(statistics);

        // Build custom provider from handlers
        RelMetadataProvider customProvider = ChainedRelMetadataProvider.of(
                ImmutableList.of(
                        ReflectiveRelMetadataProvider.reflectiveSource(
                                rowCountHandler, BuiltInMethod.ROW_COUNT.method),
                        ReflectiveRelMetadataProvider.reflectiveSource(
                                selectivityHandler, BuiltInMethod.SELECTIVITY.method)
                )
        );

        // Chain with default provider as fallback
        return ChainedRelMetadataProvider.of(
                ImmutableList.of(customProvider, DefaultRelMetadataProvider.INSTANCE)
        );
    }

    /**
     * Handler for BuiltInMetadata.RowCount - returns accurate row counts from DuckDB.
     */
    public static class DuckDBRelMdRowCount implements MetadataHandler<BuiltInMetadata.RowCount> {
        private final DuckDBStatistics statistics;

        public DuckDBRelMdRowCount(DuckDBStatistics statistics) {
            this.statistics = statistics;
        }

        @Override
        public MetadataDef<BuiltInMetadata.RowCount> getDef() {
            return BuiltInMetadata.RowCount.DEF;
        }

        /**
         * Get row count for a TableScan using DuckDB statistics.
         */
        public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
            List<String> names = rel.getTable().getQualifiedName();
            // Table name is usually the last part of the qualified name
            String tableName = names.get(names.size() - 1);
            long rowCount = statistics.getRowCount(tableName);
            return (double) rowCount;
        }

        /**
         * Fallback for other RelNode types - return null to use default.
         */
        public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
            // Return null to delegate to default handler
            return null;
        }
    }

    /**
     * Handler for BuiltInMetadata.Selectivity - estimates filter selectivity using column statistics.
     */
    public static class DuckDBRelMdSelectivity implements MetadataHandler<BuiltInMetadata.Selectivity> {
        private final DuckDBStatistics statistics;

        public DuckDBRelMdSelectivity(DuckDBStatistics statistics) {
            this.statistics = statistics;
        }

        @Override
        public MetadataDef<BuiltInMetadata.Selectivity> getDef() {
            return BuiltInMetadata.Selectivity.DEF;
        }

        /**
         * Estimate selectivity of a predicate.
         */
        public Double getSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
            if (predicate == null) {
                return 1.0;
            }
            return estimateSelectivity(rel, predicate);
        }

        private Double estimateSelectivity(RelNode rel, RexNode predicate) {
            if (predicate == null) {
                return 1.0;
            }

            // Handle AND - multiply selectivities
            if (predicate.isA(SqlKind.AND)) {
                RexCall andCall = (RexCall) predicate;
                double selectivity = 1.0;
                for (RexNode operand : andCall.getOperands()) {
                    selectivity *= estimateSelectivity(rel, operand);
                }
                return selectivity;
            }

            // Handle OR - use formula: P(A or B) = P(A) + P(B) - P(A)*P(B)
            if (predicate.isA(SqlKind.OR)) {
                RexCall orCall = (RexCall) predicate;
                double combined = 0.0;
                for (RexNode operand : orCall.getOperands()) {
                    double opSel = estimateSelectivity(rel, operand);
                    combined = combined + opSel - combined * opSel;
                }
                return combined;
            }

            // Handle NOT
            if (predicate.isA(SqlKind.NOT)) {
                RexCall notCall = (RexCall) predicate;
                return 1.0 - estimateSelectivity(rel, notCall.getOperands().get(0));
            }

            // Handle equality predicate: col = value
            if (predicate.isA(SqlKind.EQUALS)) {
                return estimateEqualitySelectivity(rel, (RexCall) predicate);
            }

            // Handle range predicates: col < value, col > value, etc.
            if (predicate.isA(SqlKind.LESS_THAN) || predicate.isA(SqlKind.LESS_THAN_OR_EQUAL) ||
                predicate.isA(SqlKind.GREATER_THAN) || predicate.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
                return estimateRangeSelectivity(rel, (RexCall) predicate);
            }

            // Handle BETWEEN
            if (predicate.isA(SqlKind.BETWEEN)) {
                // BETWEEN is typically x >= low AND x <= high
                // Use conservative estimate
                return 0.25;
            }

            // Handle IN
            if (predicate.isA(SqlKind.IN)) {
                RexCall inCall = (RexCall) predicate;
                // Each value in IN list contributes 1/distinctCount
                int numValues = inCall.getOperands().size() - 1; // First operand is the column
                double perValueSel = estimateEqualitySelectivity(rel, null);
                return Math.min(1.0, numValues * perValueSel);
            }

            // Handle LIKE
            if (predicate.isA(SqlKind.LIKE)) {
                return 0.25; // Conservative estimate for LIKE
            }

            // Handle IS NULL / IS NOT NULL
            if (predicate.isA(SqlKind.IS_NULL)) {
                return 0.01; // Assume 1% nulls by default
            }
            if (predicate.isA(SqlKind.IS_NOT_NULL)) {
                return 0.99;
            }

            // Default selectivity for unknown predicates
            return 0.25;
        }

        private Double estimateEqualitySelectivity(RelNode rel, RexCall eqCall) {
            // For equality, selectivity = 1 / distinctCount
            if (eqCall == null) {
                return 0.1; // Default 10%
            }

            // Try to get column info
            RexNode leftOp = eqCall.getOperands().get(0);
            if (leftOp instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef) leftOp;
                String columnName = rel.getRowType().getFieldNames().get(ref.getIndex());

                // Find the table this column belongs to
                if (rel instanceof TableScan) {
                    TableScan scan = (TableScan) rel;
                    List<String> names = scan.getTable().getQualifiedName();
                    String tableName = names.get(names.size() - 1);
                    return statistics.getSelectivity(tableName, columnName);
                }
            }

            return 0.1; // Default 10% selectivity for equality
        }

        private Double estimateRangeSelectivity(RelNode rel, RexCall rangeCall) {
            // Try to estimate based on min/max statistics
            RexNode leftOp = rangeCall.getOperands().get(0);
            RexNode rightOp = rangeCall.getOperands().get(1);

            if (leftOp instanceof RexInputRef && rightOp instanceof RexLiteral) {
                RexInputRef ref = (RexInputRef) leftOp;
                RexLiteral literal = (RexLiteral) rightOp;

                String columnName = rel.getRowType().getFieldNames().get(ref.getIndex());

                if (rel instanceof TableScan) {
                    TableScan scan = (TableScan) rel;
                    List<String> names = scan.getTable().getQualifiedName();
                    String tableName = names.get(names.size() - 1);

                    DuckDBStatistics.ColumnStats colStats = statistics.getColumnStats(tableName, columnName);
                    if (colStats != null && colStats.minValue != null && colStats.maxValue != null) {
                        try {
                            double min = Double.parseDouble(colStats.minValue);
                            double max = Double.parseDouble(colStats.maxValue);
                            double range = max - min;

                            if (range > 0) {
                                Comparable<?> value = literal.getValueAs(Comparable.class);
                                if (value instanceof Number) {
                                    double numVal = ((Number) value).doubleValue();
                                    // Estimate fraction of range selected
                                    if (rangeCall.isA(SqlKind.LESS_THAN) || rangeCall.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
                                        return Math.max(0.0, Math.min(1.0, (numVal - min) / range));
                                    } else {
                                        return Math.max(0.0, Math.min(1.0, (max - numVal) / range));
                                    }
                                }
                            }
                        } catch (NumberFormatException e) {
                            // Non-numeric, use default
                        }
                    }
                }
            }

            // Default range selectivity
            return 0.33;
        }
    }
}
