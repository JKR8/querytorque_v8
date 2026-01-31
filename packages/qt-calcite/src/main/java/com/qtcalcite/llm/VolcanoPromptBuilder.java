package com.qtcalcite.llm;

import com.qtcalcite.calcite.DuckDBStatistics;
import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.duckdb.ExplainParser;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds LLM prompts specifically for Volcano (cost-based) optimization.
 * Includes detailed statistics and cost information to help LLM make informed decisions.
 */
public class VolcanoPromptBuilder {

    private final RuleRegistry ruleRegistry;
    private final DuckDBAdapter duckDBAdapter;
    private final DuckDBStatistics statistics;

    public VolcanoPromptBuilder(RuleRegistry ruleRegistry, DuckDBAdapter duckDBAdapter,
                                 DuckDBStatistics statistics) {
        this.ruleRegistry = ruleRegistry;
        this.duckDBAdapter = duckDBAdapter;
        this.statistics = statistics;
    }

    /**
     * Build a cost-aware system prompt for Volcano optimization.
     */
    public String buildSystemPrompt() {
        return """
            You are an expert SQL query optimizer specializing in cost-based optimization.
            You use Apache Calcite's Volcano optimizer with real database statistics.

            ## Your Role
            Given a SQL query, table statistics, and execution plan, select optimal Calcite rewrite rules
            to minimize query cost. Focus on:

            1. **Join Ordering**: Large tables should be filtered first. Use statistics to determine
               which table has fewer rows after filtering.

            2. **Filter Pushdown**: Push filters as close to table scans as possible to reduce
               intermediate result sizes.

            3. **Projection Pruning**: Remove unused columns early to reduce I/O and memory.

            4. **Aggregate Optimization**: Push aggregates through joins when possible.

            ## Cost Model
            The optimizer uses these cost factors:
            - **Rows**: Number of rows processed (most important)
            - **CPU**: Processing operations
            - **I/O**: Disk reads

            Lower cost is better. Use row count statistics to estimate which transformations
            will reduce the number of rows processed earliest in the plan.

            ## Response Format
            Respond with ONLY a comma-separated list of rule names to apply.
            Order rules by priority (most impactful first).

            ## Available Rules
            """ + ruleRegistry.formatRulesForPrompt();
    }

    /**
     * Build user prompt with statistics for a query.
     */
    public String buildUserPrompt(String sql) throws SQLException {
        StringBuilder prompt = new StringBuilder();

        // Add the query
        prompt.append("## Input Query\n```sql\n");
        prompt.append(sql.trim());
        prompt.append("\n```\n\n");

        // Extract tables from query and show statistics
        Set<String> tables = extractTableNames(sql);
        if (!tables.isEmpty()) {
            prompt.append("## Table Statistics\n");
            prompt.append("Real statistics from the database:\n\n");

            for (String tableName : tables) {
                DuckDBStatistics.TableStats tableStats = statistics.getTableStats(tableName);
                if (tableStats != null) {
                    prompt.append("### ").append(tableName.toUpperCase()).append("\n");
                    prompt.append(String.format("- **Row Count**: %,d%n", tableStats.rowCount));
                    prompt.append(String.format("- **Column Count**: %d%n", tableStats.columnCount));

                    // Add column-level statistics for key columns
                    Map<String, DuckDBStatistics.ColumnStats> colStats = getColumnStats(tableName);
                    if (colStats != null && !colStats.isEmpty()) {
                        prompt.append("- **Column Statistics**:\n");
                        int colCount = 0;
                        for (Map.Entry<String, DuckDBStatistics.ColumnStats> entry : colStats.entrySet()) {
                            if (colCount++ >= 5) { // Limit to 5 columns to avoid huge prompts
                                prompt.append("  - _(and more columns...)_\n");
                                break;
                            }
                            DuckDBStatistics.ColumnStats cs = entry.getValue();
                            prompt.append(String.format("  - `%s` (%s)", cs.columnName, cs.dataType));
                            if (cs.minValue != null && cs.maxValue != null) {
                                prompt.append(String.format(": min=%s, max=%s", cs.minValue, cs.maxValue));
                            }
                            if (cs.distinctCount > 0) {
                                prompt.append(String.format(", ~%,d distinct", cs.distinctCount));
                            }
                            prompt.append("\n");
                        }
                    }
                    prompt.append("\n");
                } else {
                    prompt.append("### ").append(tableName.toUpperCase()).append("\n");
                    prompt.append("- Statistics not available\n\n");
                }
            }
        }

        // Add explain plan
        try {
            String explainPlan = duckDBAdapter.getExplainPlan(sql);
            prompt.append("## Execution Plan\n");
            prompt.append(ExplainParser.formatForLLM(explainPlan));
        } catch (SQLException e) {
            prompt.append("## Execution Plan\n");
            prompt.append("_(Unable to generate: ").append(e.getMessage()).append(")_\n");
        }

        // Add optimization hints based on query structure
        prompt.append("\n## Optimization Hints\n");
        prompt.append(generateOptimizationHints(sql, tables));

        prompt.append("\n## Task\n");
        prompt.append("Based on the query structure, table sizes, and statistics above, ");
        prompt.append("select the optimal Calcite rewrite rules to minimize cost.\n");
        prompt.append("Consider:\n");
        prompt.append("- Which tables are largest and should be filtered first?\n");
        prompt.append("- Where can filters be pushed down to reduce intermediate results?\n");
        prompt.append("- Can join order be improved based on table sizes?\n\n");
        prompt.append("Respond with ONLY a comma-separated list of rule names.\n");

        return prompt.toString();
    }

    /**
     * Build complete prompt for display.
     */
    public String buildCompletePrompt(String sql) throws SQLException {
        StringBuilder prompt = new StringBuilder();

        prompt.append("=".repeat(70)).append("\n");
        prompt.append("VOLCANO OPTIMIZER PROMPT (Cost-Based with Statistics)\n");
        prompt.append("=".repeat(70)).append("\n\n");

        prompt.append("--- SYSTEM PROMPT ---\n");
        prompt.append(buildSystemPrompt());
        prompt.append("\n\n");

        prompt.append("--- USER PROMPT ---\n");
        prompt.append(buildUserPrompt(sql));
        prompt.append("\n");

        prompt.append("=".repeat(70)).append("\n");

        return prompt.toString();
    }

    /**
     * Extract table names from SQL query.
     */
    private Set<String> extractTableNames(String sql) {
        Set<String> tables = new LinkedHashSet<>();
        String normalized = sql.toLowerCase();

        // Pattern for FROM and JOIN clauses
        Pattern pattern = Pattern.compile(
            "(?:from|join)\\s+([a-zA-Z_][a-zA-Z0-9_]*)",
            Pattern.CASE_INSENSITIVE
        );

        Matcher matcher = pattern.matcher(normalized);
        while (matcher.find()) {
            String tableName = matcher.group(1).toLowerCase();
            // Filter out SQL keywords that might be matched
            if (!isKeyword(tableName)) {
                tables.add(tableName);
            }
        }

        return tables;
    }

    private boolean isKeyword(String word) {
        Set<String> keywords = Set.of(
            "select", "where", "and", "or", "on", "as", "by", "order",
            "group", "having", "limit", "offset", "union", "except", "intersect"
        );
        return keywords.contains(word.toLowerCase());
    }

    private Map<String, DuckDBStatistics.ColumnStats> getColumnStats(String tableName) {
        // Access column stats through the statistics object
        // This is a bit of a workaround since DuckDBStatistics doesn't expose all column stats directly
        Map<String, DuckDBStatistics.ColumnStats> result = new LinkedHashMap<>();

        // Try to get stats for common column patterns
        String[] commonColumns = {"_sk", "_id", "_name", "_date", "_year"};
        for (String suffix : commonColumns) {
            // We can only get stats for specific columns we know about
            // In practice, we'd iterate through known columns
        }

        return result;
    }

    /**
     * Generate optimization hints based on query structure.
     */
    private String generateOptimizationHints(String sql, Set<String> tables) {
        StringBuilder hints = new StringBuilder();
        String normalized = sql.toLowerCase();

        // Count joins
        int joinCount = countOccurrences(normalized, " join ");
        if (joinCount >= 3) {
            hints.append("- **Multi-way join detected** (").append(joinCount).append(" joins): ");
            hints.append("Consider JOIN_COMMUTE and JOIN_ASSOCIATE for optimal join ordering\n");
        }

        // Check for filters on large tables
        for (String table : tables) {
            long rowCount = statistics.getRowCount(table);
            if (rowCount > 1_000_000) {
                hints.append("- **Large table**: `").append(table).append("` has ");
                hints.append(String.format("%,d", rowCount)).append(" rows. ");
                hints.append("Prioritize filtering this table early.\n");
            }
        }

        // Check for WHERE clause
        if (normalized.contains(" where ")) {
            hints.append("- **Filters present**: Consider FILTER_INTO_JOIN and FILTER_PROJECT_TRANSPOSE\n");
        }

        // Check for GROUP BY
        if (normalized.contains(" group by ")) {
            hints.append("- **Aggregation present**: Consider AGGREGATE_PROJECT_MERGE\n");
        }

        // Check for subqueries
        if (countOccurrences(normalized, "select") > 1) {
            hints.append("- **Subqueries detected**: May benefit from decorrelation rules\n");
        }

        // Check for self-joins (same table appears multiple times)
        for (String table : tables) {
            int occurrences = countOccurrences(normalized, table);
            if (occurrences > 1) {
                hints.append("- **Self-join on** `").append(table).append("`: ");
                hints.append("Join ordering is critical for performance\n");
            }
        }

        if (hints.length() == 0) {
            hints.append("- No specific optimization patterns detected\n");
        }

        return hints.toString();
    }

    private int countOccurrences(String text, String pattern) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(pattern, index)) != -1) {
            count++;
            index += pattern.length();
        }
        return count;
    }

    /**
     * Container for system and user prompts.
     */
    public static class LLMPrompt {
        private final String systemPrompt;
        private final String userPrompt;

        public LLMPrompt(String systemPrompt, String userPrompt) {
            this.systemPrompt = systemPrompt;
            this.userPrompt = userPrompt;
        }

        public String getSystemPrompt() { return systemPrompt; }
        public String getUserPrompt() { return userPrompt; }
    }

    /**
     * Build prompt pair for API calls.
     */
    public LLMPrompt buildLLMPrompt(String sql) throws SQLException {
        return new LLMPrompt(buildSystemPrompt(), buildUserPrompt(sql));
    }
}
