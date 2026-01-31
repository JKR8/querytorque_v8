package com.qtcalcite.llm;

import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.duckdb.DuckDBAdapter;
import com.qtcalcite.duckdb.ExplainParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.stream.Collectors;

/**
 * Builds LLM-R2 formatted prompts for query optimization.
 */
public class PromptBuilder {

    private final RuleRegistry ruleRegistry;
    private final DuckDBAdapter duckDBAdapter;
    private String systemPromptTemplate;

    public PromptBuilder(RuleRegistry ruleRegistry, DuckDBAdapter duckDBAdapter) {
        this.ruleRegistry = ruleRegistry;
        this.duckDBAdapter = duckDBAdapter;
        loadSystemPrompt();
    }

    private void loadSystemPrompt() {
        try (InputStream is = getClass().getResourceAsStream("/prompts/system-prompt.txt")) {
            if (is != null) {
                this.systemPromptTemplate = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                        .lines()
                        .collect(Collectors.joining("\n"));
            } else {
                this.systemPromptTemplate = getDefaultSystemPrompt();
            }
        } catch (IOException e) {
            this.systemPromptTemplate = getDefaultSystemPrompt();
        }
    }

    private String getDefaultSystemPrompt() {
        return """
            You are an expert SQL query optimizer. Your task is to analyze SQL queries and their execution plans, then select the optimal Apache Calcite rewrite rules to improve query performance.

            ## Your Role
            Given a SQL query and its execution plan from DuckDB, you must:
            1. Analyze the query structure and identify optimization opportunities
            2. Review the execution plan to understand current performance characteristics
            3. Select the most appropriate Calcite rewrite rules to optimize the query

            ## Response Format
            Respond with ONLY a comma-separated list of rule names to apply. Do not include explanations.

            ## Available Rules
            """;
    }

    /**
     * Build the complete system prompt including rule descriptions.
     */
    public String buildSystemPrompt() {
        StringBuilder prompt = new StringBuilder();
        prompt.append(systemPromptTemplate);
        prompt.append(ruleRegistry.formatRulesForPrompt());
        return prompt.toString();
    }

    /**
     * Build the user prompt for a specific query.
     */
    public String buildUserPrompt(String sql) throws SQLException {
        StringBuilder prompt = new StringBuilder();

        // Add the query
        prompt.append("## Input Query\n```sql\n");
        prompt.append(sql.trim());
        prompt.append("\n```\n\n");

        // Add explain plan analysis (structured issues list)
        try {
            String explainPlan = duckDBAdapter.getExplainPlan(sql);
            prompt.append(ExplainParser.formatForLLM(explainPlan));
        } catch (SQLException e) {
            prompt.append("(Unable to generate execution plan: ").append(e.getMessage()).append(")\n");
        }

        prompt.append("\n## Task\n");
        prompt.append("Based on the query and its execution plan above, select the optimal Calcite rewrite rules to apply.\n");
        prompt.append("Respond with ONLY a comma-separated list of rule names.\n");

        return prompt.toString();
    }

    /**
     * Build the complete prompt for display in manual mode.
     */
    public String buildCompletePrompt(String sql) throws SQLException {
        StringBuilder prompt = new StringBuilder();

        prompt.append("=".repeat(60)).append("\n");
        prompt.append("LLM PROMPT FOR QUERY OPTIMIZATION\n");
        prompt.append("=".repeat(60)).append("\n\n");

        prompt.append("--- SYSTEM PROMPT ---\n");
        prompt.append(buildSystemPrompt());
        prompt.append("\n\n");

        prompt.append("--- USER PROMPT ---\n");
        prompt.append(buildUserPrompt(sql));
        prompt.append("\n");

        prompt.append("=".repeat(60)).append("\n");

        return prompt.toString();
    }

    /**
     * Build a compact prompt for API calls.
     */
    public LLMPrompt buildLLMPrompt(String sql) throws SQLException {
        return new LLMPrompt(buildSystemPrompt(), buildUserPrompt(sql));
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
}
