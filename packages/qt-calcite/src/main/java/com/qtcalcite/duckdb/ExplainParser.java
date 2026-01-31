package com.qtcalcite.duckdb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses DuckDB EXPLAIN output into a structured list of optimization issues.
 */
public class ExplainParser {

    public enum Severity {
        HIGH(1, "HIGH"),
        MEDIUM(2, "MEDIUM"),
        LOW(3, "LOW");

        private final int order;
        private final String label;

        Severity(int order, String label) {
            this.order = order;
            this.label = label;
        }

        public int getOrder() { return order; }
        public String getLabel() { return label; }
    }

    public static class PlanIssue {
        private final Severity severity;
        private final String category;
        private final String description;
        private final String location;
        private final String suggestedRules;

        public PlanIssue(Severity severity, String category, String description,
                         String location, String suggestedRules) {
            this.severity = severity;
            this.category = category;
            this.description = description;
            this.location = location;
            this.suggestedRules = suggestedRules;
        }

        public Severity getSeverity() { return severity; }
        public String getCategory() { return category; }
        public String getDescription() { return description; }
        public String getLocation() { return location; }
        public String getSuggestedRules() { return suggestedRules; }
    }

    /**
     * Parse DuckDB EXPLAIN output into a structured list of issues for LLM consumption.
     * Returns issues ordered by severity (HIGH first).
     */
    public static String formatForLLM(String explainOutput) {
        if (explainOutput == null || explainOutput.isEmpty()) {
            return "No execution plan available";
        }

        List<PlanIssue> issues = analyzeExplainPlan(explainOutput);

        if (issues.isEmpty()) {
            return "No optimization issues detected in the execution plan.";
        }

        // Sort by severity (HIGH first)
        issues.sort(Comparator.comparingInt(i -> i.getSeverity().getOrder()));

        StringBuilder sb = new StringBuilder();
        sb.append("## Detected Optimization Issues\n\n");

        for (int i = 0; i < issues.size(); i++) {
            PlanIssue issue = issues.get(i);
            sb.append(String.format("%d. [%s] %s\n", i + 1, issue.getSeverity().getLabel(), issue.getCategory()));
            sb.append(String.format("   - Issue: %s\n", issue.getDescription()));
            if (issue.getLocation() != null && !issue.getLocation().isEmpty()) {
                sb.append(String.format("   - Location: %s\n", issue.getLocation()));
            }
            sb.append(String.format("   - Suggested rules: %s\n", issue.getSuggestedRules()));
            sb.append("\n");
        }

        return sb.toString();
    }

    /**
     * Analyze EXPLAIN output and extract optimization issues.
     */
    private static List<PlanIssue> analyzeExplainPlan(String explainOutput) {
        List<PlanIssue> issues = new ArrayList<>();
        String lower = explainOutput.toLowerCase();

        // Issue 1: Sequential/Table Scans (HIGH if filter exists after scan, MEDIUM otherwise)
        if (lower.contains("seq_scan") || lower.contains("table_scan")) {
            String tableName = extractTableName(explainOutput, "SEQ_SCAN", "TABLE_SCAN");
            boolean hasFilterAfterScan = hasFilterAfterOperator(explainOutput, "SEQ_SCAN", "TABLE_SCAN");

            if (hasFilterAfterScan) {
                issues.add(new PlanIssue(
                    Severity.HIGH,
                    "Filter After Table Scan",
                    "Filter is applied after full table scan instead of during scan",
                    tableName,
                    "FILTER_INTO_JOIN, FILTER_PROJECT_TRANSPOSE, FILTER_SCAN"
                ));
            } else {
                issues.add(new PlanIssue(
                    Severity.MEDIUM,
                    "Full Table Scan",
                    "Table is scanned without apparent predicate pushdown",
                    tableName,
                    "FILTER_SCAN, FILTER_PROJECT_TRANSPOSE"
                ));
            }
        }

        // Issue 2: Nested Loop Joins (HIGH - usually inefficient for large tables)
        if (lower.contains("nested_loop") || lower.contains("nested loop")) {
            String joinLocation = extractJoinLocation(explainOutput, "NESTED");
            issues.add(new PlanIssue(
                Severity.HIGH,
                "Nested Loop Join",
                "Nested loop join detected - O(n*m) complexity, inefficient for large tables",
                joinLocation,
                "JOIN_COMMUTE, JOIN_ASSOCIATE, MULTI_JOIN_OPTIMIZE"
            ));
        }

        // Issue 3: Filter above Join (HIGH - filter should be pushed into join)
        if (hasFilterAboveJoin(explainOutput)) {
            issues.add(new PlanIssue(
                Severity.HIGH,
                "Filter Above Join",
                "Filter is applied after join operation instead of being pushed into join inputs",
                "Filter → Join",
                "FILTER_INTO_JOIN, JOIN_CONDITION_PUSH, JOIN_PUSH_TRANSITIVE_PREDICATES"
            ));
        }

        // Issue 4: Multiple Projections (MEDIUM - can be merged)
        int projCount = countOccurrences(explainOutput, "PROJECTION");
        if (projCount > 1) {
            issues.add(new PlanIssue(
                Severity.MEDIUM,
                "Multiple Projections",
                String.format("%d consecutive projections detected that could be merged", projCount),
                "Multiple PROJECTION operators",
                "PROJECT_MERGE, PROJECT_REMOVE"
            ));
        }

        // Issue 5: Hash Join with large build side (MEDIUM)
        if (lower.contains("hash_join") || lower.contains("hash join")) {
            long estimatedRows = extractMaxRowEstimate(explainOutput);
            if (estimatedRows > 10000) {
                issues.add(new PlanIssue(
                    Severity.MEDIUM,
                    "Hash Join with Large Input",
                    "Hash join may benefit from filter pushdown to reduce hash table size",
                    extractJoinLocation(explainOutput, "HASH"),
                    "FILTER_INTO_JOIN, JOIN_CONDITION_PUSH, JOIN_PROJECT_BOTH_TRANSPOSE"
                ));
            }
        }

        // Issue 6: Aggregation without filter pushdown (MEDIUM)
        if ((lower.contains("aggregate") || lower.contains("hash_group")) &&
            hasFilterAboveAggregate(explainOutput)) {
            issues.add(new PlanIssue(
                Severity.MEDIUM,
                "Filter After Aggregation",
                "Filter could potentially be pushed before aggregation if it uses grouping columns",
                "Filter → Aggregate",
                "FILTER_AGGREGATE_TRANSPOSE, AGGREGATE_PROJECT_MERGE"
            ));
        }

        // Issue 7: Sort with projection (LOW - minor optimization)
        if ((lower.contains("order") || lower.contains("sort")) && projCount > 0) {
            issues.add(new PlanIssue(
                Severity.LOW,
                "Sort with Projection",
                "Sort and projection may be transposed for optimization",
                "Sort + Project",
                "SORT_PROJECT_TRANSPOSE, SORT_REMOVE_CONSTANT_KEYS"
            ));
        }

        // Issue 8: Cross product / Cartesian join (HIGH)
        if (lower.contains("cross") || lower.contains("cartesian") ||
            (lower.contains("join") && !lower.contains("="))) {
            issues.add(new PlanIssue(
                Severity.HIGH,
                "Possible Cartesian Product",
                "Join without equality condition may produce cartesian product",
                "Join operator",
                "JOIN_EXTRACT_FILTER, JOIN_CONDITION_PUSH"
            ));
        }

        // Issue 9: Subquery that could be decorrelated (MEDIUM)
        if (lower.contains("subquery") || lower.contains("correlate")) {
            issues.add(new PlanIssue(
                Severity.MEDIUM,
                "Correlated Subquery",
                "Subquery may be decorrelated and converted to join",
                "Subquery/Correlate",
                "FILTER_SUB_QUERY_TO_CORRELATE, PROJECT_SUB_QUERY_TO_CORRELATE, SUBQUERY_REMOVE"
            ));
        }

        // Issue 10: Redundant sort (LOW)
        int sortCount = countOccurrences(lower, "sort") + countOccurrences(lower, "order");
        if (sortCount > 1) {
            issues.add(new PlanIssue(
                Severity.LOW,
                "Multiple Sort Operations",
                String.format("%d sort operations detected - some may be redundant", sortCount),
                "Multiple Sort operators",
                "SORT_REMOVE, SORT_REMOVE_REDUNDANT, LIMIT_MERGE"
            ));
        }

        return issues;
    }

    private static String extractTableName(String text, String... keywords) {
        for (String keyword : keywords) {
            Pattern p = Pattern.compile(keyword + "\\s*\\(?([a-zA-Z_][a-zA-Z0-9_]*)", Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(text);
            if (m.find()) {
                return keyword + " on " + m.group(1);
            }
        }
        return "Table scan";
    }

    private static String extractJoinLocation(String text, String joinType) {
        Pattern p = Pattern.compile(joinType + "[^\\n]*", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(text);
        if (m.find()) {
            String match = m.group().trim();
            return match.length() > 60 ? match.substring(0, 57) + "..." : match;
        }
        return joinType + " join";
    }

    private static boolean hasFilterAfterOperator(String text, String... operators) {
        String lower = text.toLowerCase();
        for (String op : operators) {
            int opIdx = lower.indexOf(op.toLowerCase());
            if (opIdx >= 0) {
                // Check if FILTER appears before the operator in the plan (meaning it's above in tree)
                int filterIdx = lower.indexOf("filter");
                if (filterIdx >= 0 && filterIdx < opIdx) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean hasFilterAboveJoin(String text) {
        String lower = text.toLowerCase();
        int filterIdx = lower.indexOf("filter");
        int joinIdx = Math.min(
            lower.contains("join") ? lower.indexOf("join") : Integer.MAX_VALUE,
            lower.contains("hash_join") ? lower.indexOf("hash_join") : Integer.MAX_VALUE
        );
        // In EXPLAIN output, operators listed first are higher in the tree
        return filterIdx >= 0 && joinIdx >= 0 && filterIdx < joinIdx;
    }

    private static boolean hasFilterAboveAggregate(String text) {
        String lower = text.toLowerCase();
        int filterIdx = lower.indexOf("filter");
        int aggIdx = Math.min(
            lower.contains("aggregate") ? lower.indexOf("aggregate") : Integer.MAX_VALUE,
            lower.contains("hash_group") ? lower.indexOf("hash_group") : Integer.MAX_VALUE
        );
        return filterIdx >= 0 && aggIdx >= 0 && filterIdx < aggIdx;
    }

    private static int countOccurrences(String text, String pattern) {
        Pattern p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(text);
        int count = 0;
        while (m.find()) count++;
        return count;
    }

    private static long extractMaxRowEstimate(String text) {
        Pattern p = Pattern.compile("~?(\\d+)\\s*[Rr]ows?");
        Matcher m = p.matcher(text);
        long max = 0;
        while (m.find()) {
            try {
                long val = Long.parseLong(m.group(1));
                if (val > max) max = val;
            } catch (NumberFormatException ignored) {}
        }
        return max;
    }

    /**
     * Get just the list of issues without formatting (for programmatic use).
     */
    public static List<PlanIssue> getIssues(String explainOutput) {
        if (explainOutput == null || explainOutput.isEmpty()) {
            return new ArrayList<>();
        }
        List<PlanIssue> issues = analyzeExplainPlan(explainOutput);
        issues.sort(Comparator.comparingInt(i -> i.getSeverity().getOrder()));
        return issues;
    }
}
