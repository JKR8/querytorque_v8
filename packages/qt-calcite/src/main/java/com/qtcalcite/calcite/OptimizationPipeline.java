package com.qtcalcite.calcite;

import com.qtcalcite.detector.GapDetector;
import com.qtcalcite.detector.GapDetector.*;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.rules.GroupedTopNToLateralRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * End-to-end optimization pipeline that:
 * 1. Detects DuckDB optimizer gaps via GapDetector
 * 2. Applies appropriate Calcite rules (HEP or Volcano)
 * 3. Returns optimized SQL
 */
public class OptimizationPipeline {

    private final DuckDBAdapter adapter;
    private final DuckDBStatistics statistics;
    private final GapDetector gapDetector;
    private final CalciteOptimizer calciteOptimizer;

    public OptimizationPipeline(DuckDBAdapter adapter) throws SQLException {
        this.adapter = adapter;
        this.statistics = new DuckDBStatistics(adapter);
        this.gapDetector = new GapDetector(adapter, statistics);
        this.calciteOptimizer = new CalciteOptimizer(adapter);
    }

    /**
     * Analyze and optimize a SQL query.
     */
    public PipelineResult optimize(String sql) {
        PipelineResult result = new PipelineResult(sql);

        // Step 1: Detect gaps
        GapAnalysis analysis = gapDetector.analyze(sql);
        result.setGapAnalysis(analysis);

        if (!analysis.hasGaps()) {
            result.setMessage("No optimization gaps detected");
            return result;
        }

        // Step 2: Apply optimizations based on detected gaps
        try {
            String optimizedSql = sql;
            List<String> appliedRules = new ArrayList<>();

            for (DetectedGap gap : analysis.getGaps()) {
                OptimizationAttempt attempt = applyGapOptimization(optimizedSql, gap);
                if (attempt.success) {
                    optimizedSql = attempt.resultSql;
                    appliedRules.addAll(attempt.rulesApplied);
                    result.addAppliedOptimization(gap.getGap().getDisplayName(), attempt.rulesApplied);
                }
            }

            result.setOptimizedSql(optimizedSql);
            result.setAppliedRules(appliedRules);
            result.setSuccess(!optimizedSql.equals(sql));

            if (result.isSuccess()) {
                result.setMessage("Applied " + appliedRules.size() + " optimization rules");
            } else {
                result.setMessage("Gaps detected but no transformations applied");
            }

        } catch (Exception e) {
            result.setError("Optimization failed: " + e.getMessage());
        }

        return result;
    }

    /**
     * Apply optimization for a specific gap.
     */
    private OptimizationAttempt applyGapOptimization(String sql, DetectedGap gap) {
        OptimizationAttempt attempt = new OptimizationAttempt();

        try {
            switch (gap.getGap()) {
                case GROUPED_TOPN:
                    return applyGroupedTopNOptimization(sql);

                case JOIN_ORDER:
                case MULTIPLE_LEFT_JOINS:
                    // Use Volcano optimizer for cost-based join optimization
                    return applyVolcanoOptimization(sql, gap.getRecommendedRules());

                case SEMI_JOIN_INEQUALITY:
                case CTE_LIMIT:
                case SET_OP_NO_SHORTCIRCUIT:
                    // Use HEP optimizer for pattern-based rules
                    return applyHepOptimization(sql, gap.getRecommendedRules());

                default:
                    attempt.success = false;
                    return attempt;
            }
        } catch (Exception e) {
            attempt.success = false;
            attempt.error = e.getMessage();
            return attempt;
        }
    }

    /**
     * Apply GROUPED_TOPN → LATERAL transformation using HEP rule.
     */
    private OptimizationAttempt applyGroupedTopNOptimization(String sql) throws SqlParseException {
        OptimizationAttempt attempt = new OptimizationAttempt();

        // Create NDV lookup function using statistics
        Function<String, Long> ndvLookup = columnName -> {
            // Try to find the table containing this column and get its NDV
            for (String tableName : statistics.getTableNames()) {
                DuckDBStatistics.ColumnStats cs = statistics.getColumnStats(tableName, columnName);
                if (cs != null && cs.distinctCount > 0) {
                    return cs.distinctCount;
                }
            }
            // Default estimate if not found
            return 1000L;
        };

        // Create rule with NDV lookup
        GroupedTopNToLateralRule rule = new GroupedTopNToLateralRule(ndvLookup);

        // Parse SQL to RelNode
        RelNode relNode = calciteOptimizer.sqlToRelNode(sql);
        String originalPlan = relNode.explain();

        // Apply rule via HEP
        HepProgram program = new HepProgramBuilder()
            .addRuleInstance(rule)
            .build();

        HepPlanner planner = new HepPlanner(program);
        planner.setRoot(relNode);
        RelNode optimized = planner.findBestExp();

        String optimizedPlan = optimized.explain();

        // Check if transformation occurred
        if (!originalPlan.equals(optimizedPlan)) {
            attempt.success = true;
            attempt.resultSql = calciteOptimizer.relNodeToSql(optimized);
            attempt.rulesApplied.add("GROUPED_TOPN_TO_LATERAL");
        } else {
            attempt.success = false;
        }

        return attempt;
    }

    /**
     * Apply HEP (pattern-based) optimization.
     */
    private OptimizationAttempt applyHepOptimization(String sql, List<String> ruleNames) throws SqlParseException {
        OptimizationAttempt attempt = new OptimizationAttempt();

        CalciteOptimizer.OptimizationResult result = calciteOptimizer.optimize(sql, ruleNames);

        if (result.isChanged()) {
            attempt.success = true;
            attempt.resultSql = result.getOptimizedSql();
            attempt.rulesApplied.addAll(ruleNames);
        } else {
            attempt.success = false;
        }

        return attempt;
    }

    /**
     * Apply Volcano (cost-based) optimization.
     */
    private OptimizationAttempt applyVolcanoOptimization(String sql, List<String> ruleNames) {
        OptimizationAttempt attempt = new OptimizationAttempt();

        // For now, use HEP as fallback - Volcano integration is more complex
        try {
            return applyHepOptimization(sql, ruleNames);
        } catch (Exception e) {
            attempt.success = false;
            attempt.error = e.getMessage();
            return attempt;
        }
    }

    private static class OptimizationAttempt {
        boolean success = false;
        String resultSql;
        List<String> rulesApplied = new ArrayList<>();
        String error;
    }

    /**
     * Result of the optimization pipeline.
     */
    public static class PipelineResult {
        private final String originalSql;
        private String optimizedSql;
        private GapAnalysis gapAnalysis;
        private List<String> appliedRules = new ArrayList<>();
        private List<AppliedOptimization> appliedOptimizations = new ArrayList<>();
        private boolean success = false;
        private String message;
        private String error;

        public PipelineResult(String originalSql) {
            this.originalSql = originalSql;
            this.optimizedSql = originalSql;
        }

        public void setOptimizedSql(String sql) { this.optimizedSql = sql; }
        public void setGapAnalysis(GapAnalysis analysis) { this.gapAnalysis = analysis; }
        public void setAppliedRules(List<String> rules) { this.appliedRules = rules; }
        public void setSuccess(boolean success) { this.success = success; }
        public void setMessage(String message) { this.message = message; }
        public void setError(String error) { this.error = error; this.success = false; }

        public void addAppliedOptimization(String gapName, List<String> rules) {
            appliedOptimizations.add(new AppliedOptimization(gapName, rules));
        }

        public String getOriginalSql() { return originalSql; }
        public String getOptimizedSql() { return optimizedSql; }
        public GapAnalysis getGapAnalysis() { return gapAnalysis; }
        public List<String> getAppliedRules() { return appliedRules; }
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public String getError() { return error; }
        public boolean hasError() { return error != null; }

        public String format() {
            StringBuilder sb = new StringBuilder();
            sb.append("=".repeat(60)).append("\n");
            sb.append("OPTIMIZATION PIPELINE RESULT\n");
            sb.append("=".repeat(60)).append("\n\n");

            if (gapAnalysis != null && gapAnalysis.hasGaps()) {
                sb.append("DETECTED GAPS:\n");
                for (DetectedGap gap : gapAnalysis.getGaps()) {
                    sb.append("  - ").append(gap.getGap().getDisplayName())
                      .append(": ").append(gap.getDescription()).append("\n");
                }
                sb.append("\n");
            }

            if (!appliedOptimizations.isEmpty()) {
                sb.append("APPLIED OPTIMIZATIONS:\n");
                for (AppliedOptimization opt : appliedOptimizations) {
                    sb.append("  - ").append(opt.gapName)
                      .append(": ").append(String.join(", ", opt.rules)).append("\n");
                }
                sb.append("\n");
            }

            sb.append("STATUS: ").append(success ? "SUCCESS" : "NO CHANGE").append("\n");
            if (message != null) {
                sb.append("MESSAGE: ").append(message).append("\n");
            }
            if (error != null) {
                sb.append("ERROR: ").append(error).append("\n");
            }

            if (success) {
                sb.append("\nORIGINAL SQL:\n").append(originalSql).append("\n");
                sb.append("\nOPTIMIZED SQL:\n").append(optimizedSql).append("\n");
            }

            return sb.toString();
        }
    }

    private static class AppliedOptimization {
        final String gapName;
        final List<String> rules;

        AppliedOptimization(String gapName, List<String> rules) {
            this.gapName = gapName;
            this.rules = rules;
        }
    }
}
