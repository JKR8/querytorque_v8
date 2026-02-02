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
            You are an expert SQL query optimizer using Apache Calcite's HEP (heuristic) planner.
            Your task is to choose a small, ordered set of rewrite rules that will most likely improve performance.

            ## HEP-Specific Guidance
            - HEP applies rules in the order you provide. Order matters.
            - There is no cost model here; prioritize rules that reduce rows early and simplify the plan.
            - Prefer a short list (3-8 rules). Too many rules can over-rewrite or add noise.
            - Only choose from the available rule list below. Do not invent new rules.

            ## What to Optimize
            Use the Detected Optimization Issues from the user prompt as your primary signal.
            Match issues to the suggested rules there, then add small cleanup rules if helpful.

            Priority order (most impactful first):
            1) Filter pushdown (FILTER_* , JOIN_* pushdown)
            2) Join order / join simplification (JOIN_* , MULTI_JOIN_*)
            3) Aggregate pushdown / reduction (AGGREGATE_*)
            4) Projection pruning / merge (PROJECT_*)
            5) Sort / limit cleanup (SORT_* , LIMIT_*)

            Special cases (use only when clearly applicable):
            - GROUPED_TOPN_TO_LATERAL: windowed Top-N per group patterns.
            - FK_JOIN_ELIMINATION: joins used only for FK validation (columns not referenced).

            ## Response Format
            Respond with ONLY a comma-separated list of rule names to apply. No extra text.
            If nothing obvious stands out, return a conservative short list:
            FILTER_INTO_JOIN, FILTER_PROJECT_TRANSPOSE, PROJECT_MERGE

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
