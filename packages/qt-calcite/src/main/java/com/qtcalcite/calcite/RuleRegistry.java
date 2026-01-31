package com.qtcalcite.calcite;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.qtcalcite.rules.GroupedTopNToLateralRule;
import org.apache.calcite.rel.rules.CoreRules;

import org.apache.calcite.plan.RelOptRule;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Registry of available Calcite rewrite rules.
 * Maps rule names to actual RelOptRule instances.
 * Contains 140+ rules from Apache Calcite CoreRules.
 */
public class RuleRegistry {

    private final Map<String, RelOptRule> rules = new LinkedHashMap<>();
    private final Map<String, RuleInfo> ruleInfoMap = new LinkedHashMap<>();

    public RuleRegistry() {
        registerAllRules();
        loadRuleDescriptions();
    }

    private void registerAllRules() {
        // ==================== AGGREGATE RULES ====================
        rules.put("AGGREGATE_PROJECT_MERGE", CoreRules.AGGREGATE_PROJECT_MERGE);
        rules.put("AGGREGATE_PROJECT_PULL_UP_CONSTANTS", CoreRules.AGGREGATE_PROJECT_PULL_UP_CONSTANTS);
        rules.put("AGGREGATE_ANY_PULL_UP_CONSTANTS", CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS);
        rules.put("AGGREGATE_STAR_TABLE", CoreRules.AGGREGATE_STAR_TABLE);
        rules.put("AGGREGATE_PROJECT_STAR_TABLE", CoreRules.AGGREGATE_PROJECT_STAR_TABLE);
        rules.put("AGGREGATE_REDUCE_FUNCTIONS", CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
        rules.put("AGGREGATE_MERGE", CoreRules.AGGREGATE_MERGE);
        rules.put("AGGREGATE_REMOVE", CoreRules.AGGREGATE_REMOVE);
        rules.put("AGGREGATE_EXPAND_DISTINCT_AGGREGATES", CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES);
        rules.put("AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN", CoreRules.AGGREGATE_EXPAND_DISTINCT_AGGREGATES_TO_JOIN);
        rules.put("AGGREGATE_EXPAND_WITHIN_DISTINCT", CoreRules.AGGREGATE_EXPAND_WITHIN_DISTINCT);
        rules.put("AGGREGATE_FILTER_TRANSPOSE", CoreRules.AGGREGATE_FILTER_TRANSPOSE);
        rules.put("AGGREGATE_JOIN_JOIN_REMOVE", CoreRules.AGGREGATE_JOIN_JOIN_REMOVE);
        rules.put("AGGREGATE_JOIN_REMOVE", CoreRules.AGGREGATE_JOIN_REMOVE);
        rules.put("AGGREGATE_JOIN_TRANSPOSE", CoreRules.AGGREGATE_JOIN_TRANSPOSE);
        rules.put("AGGREGATE_JOIN_TRANSPOSE_EXTENDED", CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED);
        rules.put("AGGREGATE_UNION_TRANSPOSE", CoreRules.AGGREGATE_UNION_TRANSPOSE);
        rules.put("AGGREGATE_UNION_AGGREGATE", CoreRules.AGGREGATE_UNION_AGGREGATE);
        rules.put("AGGREGATE_UNION_AGGREGATE_FIRST", CoreRules.AGGREGATE_UNION_AGGREGATE_FIRST);
        rules.put("AGGREGATE_UNION_AGGREGATE_SECOND", CoreRules.AGGREGATE_UNION_AGGREGATE_SECOND);
        rules.put("AGGREGATE_CASE_TO_FILTER", CoreRules.AGGREGATE_CASE_TO_FILTER);
        rules.put("AGGREGATE_VALUES", CoreRules.AGGREGATE_VALUES);

        // ==================== CALC RULES ====================
        rules.put("CALC_MERGE", CoreRules.CALC_MERGE);
        rules.put("CALC_REMOVE", CoreRules.CALC_REMOVE);
        rules.put("CALC_REDUCE_EXPRESSIONS", CoreRules.CALC_REDUCE_EXPRESSIONS);
        rules.put("CALC_SPLIT", CoreRules.CALC_SPLIT);

        // ==================== EXCHANGE RULES ====================
        rules.put("EXCHANGE_REMOVE_CONSTANT_KEYS", CoreRules.EXCHANGE_REMOVE_CONSTANT_KEYS);
        rules.put("SORT_EXCHANGE_REMOVE_CONSTANT_KEYS", CoreRules.SORT_EXCHANGE_REMOVE_CONSTANT_KEYS);

        // ==================== FILTER RULES ====================
        rules.put("FILTER_INTO_JOIN", CoreRules.FILTER_INTO_JOIN);
        rules.put("FILTER_INTO_JOIN_DUMB", CoreRules.FILTER_INTO_JOIN_DUMB);
        rules.put("FILTER_MERGE", CoreRules.FILTER_MERGE);
        rules.put("FILTER_CALC_MERGE", CoreRules.FILTER_CALC_MERGE);
        rules.put("FILTER_TO_CALC", CoreRules.FILTER_TO_CALC);
        rules.put("FILTER_AGGREGATE_TRANSPOSE", CoreRules.FILTER_AGGREGATE_TRANSPOSE);
        rules.put("FILTER_PROJECT_TRANSPOSE", CoreRules.FILTER_PROJECT_TRANSPOSE);
        rules.put("FILTER_SAMPLE_TRANSPOSE", CoreRules.FILTER_SAMPLE_TRANSPOSE);
        rules.put("FILTER_TABLE_FUNCTION_TRANSPOSE", CoreRules.FILTER_TABLE_FUNCTION_TRANSPOSE);
        rules.put("FILTER_SCAN", CoreRules.FILTER_SCAN);
        rules.put("FILTER_CORRELATE", CoreRules.FILTER_CORRELATE);
        rules.put("FILTER_MULTI_JOIN_MERGE", CoreRules.FILTER_MULTI_JOIN_MERGE);
        rules.put("FILTER_SET_OP_TRANSPOSE", CoreRules.FILTER_SET_OP_TRANSPOSE);
        rules.put("FILTER_REDUCE_EXPRESSIONS", CoreRules.FILTER_REDUCE_EXPRESSIONS);
        rules.put("FILTER_EXPAND_IS_NOT_DISTINCT_FROM", CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM);

        // ==================== INTERSECT RULES ====================
        rules.put("INTERSECT_MERGE", CoreRules.INTERSECT_MERGE);
        rules.put("INTERSECT_TO_DISTINCT", CoreRules.INTERSECT_TO_DISTINCT);

        // ==================== MINUS RULES ====================
        rules.put("MINUS_MERGE", CoreRules.MINUS_MERGE);

        // ==================== PROJECT RULES ====================
        rules.put("PROJECT_AGGREGATE_MERGE", CoreRules.PROJECT_AGGREGATE_MERGE);
        rules.put("PROJECT_CALC_MERGE", CoreRules.PROJECT_CALC_MERGE);
        rules.put("PROJECT_CORRELATE_TRANSPOSE", CoreRules.PROJECT_CORRELATE_TRANSPOSE);
        rules.put("PROJECT_FILTER_TRANSPOSE", CoreRules.PROJECT_FILTER_TRANSPOSE);
        rules.put("PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS", CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_EXPRESSIONS);
        rules.put("PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS", CoreRules.PROJECT_FILTER_TRANSPOSE_WHOLE_PROJECT_EXPRESSIONS);
        rules.put("PROJECT_REDUCE_EXPRESSIONS", CoreRules.PROJECT_REDUCE_EXPRESSIONS);
        rules.put("PROJECT_SUB_QUERY_TO_CORRELATE", CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
        rules.put("FILTER_SUB_QUERY_TO_CORRELATE", CoreRules.FILTER_SUB_QUERY_TO_CORRELATE);
        rules.put("JOIN_SUB_QUERY_TO_CORRELATE", CoreRules.JOIN_SUB_QUERY_TO_CORRELATE);
        rules.put("PROJECT_TO_SEMI_JOIN", CoreRules.PROJECT_TO_SEMI_JOIN);
        rules.put("PROJECT_JOIN_JOIN_REMOVE", CoreRules.PROJECT_JOIN_JOIN_REMOVE);
        rules.put("PROJECT_JOIN_REMOVE", CoreRules.PROJECT_JOIN_REMOVE);
        rules.put("PROJECT_JOIN_TRANSPOSE", CoreRules.PROJECT_JOIN_TRANSPOSE);
        rules.put("PROJECT_MERGE", CoreRules.PROJECT_MERGE);
        rules.put("PROJECT_SET_OP_TRANSPOSE", CoreRules.PROJECT_SET_OP_TRANSPOSE);
        rules.put("PROJECT_MULTI_JOIN_MERGE", CoreRules.PROJECT_MULTI_JOIN_MERGE);
        rules.put("PROJECT_REMOVE", CoreRules.PROJECT_REMOVE);
        rules.put("PROJECT_TABLE_SCAN", CoreRules.PROJECT_TABLE_SCAN);
        rules.put("PROJECT_TO_CALC", CoreRules.PROJECT_TO_CALC);
        rules.put("PROJECT_WINDOW_TRANSPOSE", CoreRules.PROJECT_WINDOW_TRANSPOSE);
        rules.put("PROJECT_FILTER_VALUES_MERGE", CoreRules.PROJECT_FILTER_VALUES_MERGE);
        rules.put("PROJECT_VALUES_MERGE", CoreRules.PROJECT_VALUES_MERGE);

        // ==================== SEMI-JOIN RULES ====================
        rules.put("SEMI_JOIN_FILTER_TRANSPOSE", CoreRules.SEMI_JOIN_FILTER_TRANSPOSE);
        rules.put("SEMI_JOIN_JOIN_TRANSPOSE", CoreRules.SEMI_JOIN_JOIN_TRANSPOSE);
        rules.put("SEMI_JOIN_PROJECT_TRANSPOSE", CoreRules.SEMI_JOIN_PROJECT_TRANSPOSE);
        rules.put("SEMI_JOIN_REMOVE", CoreRules.SEMI_JOIN_REMOVE);

        // ==================== SORT RULES ====================
        rules.put("SORT_JOIN_COPY", CoreRules.SORT_JOIN_COPY);
        rules.put("SORT_JOIN_TRANSPOSE", CoreRules.SORT_JOIN_TRANSPOSE);
        rules.put("SORT_PROJECT_TRANSPOSE", CoreRules.SORT_PROJECT_TRANSPOSE);
        rules.put("SORT_REMOVE", CoreRules.SORT_REMOVE);
        rules.put("SORT_REMOVE_CONSTANT_KEYS", CoreRules.SORT_REMOVE_CONSTANT_KEYS);
        rules.put("SORT_REMOVE_REDUNDANT", CoreRules.SORT_REMOVE_REDUNDANT);
        rules.put("SORT_UNION_TRANSPOSE", CoreRules.SORT_UNION_TRANSPOSE);
        rules.put("SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH", CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH);

        // ==================== UNION RULES ====================
        rules.put("UNION_MERGE", CoreRules.UNION_MERGE);
        rules.put("UNION_PULL_UP_CONSTANTS", CoreRules.UNION_PULL_UP_CONSTANTS);
        rules.put("UNION_REMOVE", CoreRules.UNION_REMOVE);
        rules.put("UNION_TO_DISTINCT", CoreRules.UNION_TO_DISTINCT);

        // ==================== WINDOW RULES ====================
        rules.put("WINDOW_REDUCE_EXPRESSIONS", CoreRules.WINDOW_REDUCE_EXPRESSIONS);

        // ==================== CUSTOM RULES (QTCalcite) ====================
        rules.put("GROUPED_TOPN_TO_LATERAL", GroupedTopNToLateralRule.INSTANCE);

        // ==================== JOIN RULES ====================
        rules.put("JOIN_ASSOCIATE", CoreRules.JOIN_ASSOCIATE);
        rules.put("JOIN_COMMUTE", CoreRules.JOIN_COMMUTE);
        rules.put("JOIN_COMMUTE_OUTER", CoreRules.JOIN_COMMUTE_OUTER);
        rules.put("JOIN_CONDITION_PUSH", CoreRules.JOIN_CONDITION_PUSH);
        rules.put("JOIN_EXTRACT_FILTER", CoreRules.JOIN_EXTRACT_FILTER);
        rules.put("JOIN_LEFT_UNION_TRANSPOSE", CoreRules.JOIN_LEFT_UNION_TRANSPOSE);
        rules.put("JOIN_PROJECT_BOTH_TRANSPOSE", CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE);
        rules.put("JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER", CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE_INCLUDE_OUTER);
        rules.put("JOIN_PROJECT_LEFT_TRANSPOSE", CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE);
        rules.put("JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER", CoreRules.JOIN_PROJECT_LEFT_TRANSPOSE_INCLUDE_OUTER);
        rules.put("JOIN_PROJECT_RIGHT_TRANSPOSE", CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE);
        rules.put("JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER", CoreRules.JOIN_PROJECT_RIGHT_TRANSPOSE_INCLUDE_OUTER);
        rules.put("JOIN_PUSH_EXPRESSIONS", CoreRules.JOIN_PUSH_EXPRESSIONS);
        rules.put("JOIN_PUSH_TRANSITIVE_PREDICATES", CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);
        rules.put("JOIN_REDUCE_EXPRESSIONS", CoreRules.JOIN_REDUCE_EXPRESSIONS);
        rules.put("JOIN_RIGHT_UNION_TRANSPOSE", CoreRules.JOIN_RIGHT_UNION_TRANSPOSE);
        rules.put("JOIN_TO_CORRELATE", CoreRules.JOIN_TO_CORRELATE);
        rules.put("JOIN_TO_MULTI_JOIN", CoreRules.JOIN_TO_MULTI_JOIN);
        rules.put("JOIN_TO_SEMI_JOIN", CoreRules.JOIN_TO_SEMI_JOIN);
        rules.put("JOIN_ADD_REDUNDANT_SEMI_JOIN", CoreRules.JOIN_ADD_REDUNDANT_SEMI_JOIN);

        // ==================== MULTI-JOIN RULES ====================
        rules.put("MULTI_JOIN_BOTH_PROJECT", CoreRules.MULTI_JOIN_BOTH_PROJECT);
        rules.put("MULTI_JOIN_LEFT_PROJECT", CoreRules.MULTI_JOIN_LEFT_PROJECT);
        rules.put("MULTI_JOIN_RIGHT_PROJECT", CoreRules.MULTI_JOIN_RIGHT_PROJECT);
        rules.put("MULTI_JOIN_OPTIMIZE", CoreRules.MULTI_JOIN_OPTIMIZE);
        rules.put("MULTI_JOIN_OPTIMIZE_BUSHY", CoreRules.MULTI_JOIN_OPTIMIZE_BUSHY);

        // ==================== COERCE RULES ====================
        rules.put("COERCE_INPUTS", CoreRules.COERCE_INPUTS);

        // ==================== SAMPLE RULES ====================
        rules.put("SAMPLE_TO_FILTER", CoreRules.SAMPLE_TO_FILTER);

        // ==================== LIMIT RULES ====================
        rules.put("LIMIT_MERGE", CoreRules.LIMIT_MERGE);

        // Create aliases for common short names
        rules.put("SUBQUERY_REMOVE", CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE);
    }

    private void loadRuleDescriptions() {
        try (InputStream is = getClass().getResourceAsStream("/rules/calcite-rules.json")) {
            if (is == null) {
                createDefaultDescriptions();
                return;
            }

            Gson gson = new Gson();
            JsonObject root = gson.fromJson(new InputStreamReader(is, StandardCharsets.UTF_8), JsonObject.class);
            JsonArray rulesArray = root.getAsJsonArray("rules");

            for (JsonElement elem : rulesArray) {
                JsonObject ruleObj = elem.getAsJsonObject();
                String name = ruleObj.get("name").getAsString();
                String description = ruleObj.get("description").getAsString();
                String category = ruleObj.get("category").getAsString();
                ruleInfoMap.put(name, new RuleInfo(name, description, category));
            }

            // Add descriptions for any rules not in JSON
            for (String ruleName : rules.keySet()) {
                if (!ruleInfoMap.containsKey(ruleName)) {
                    ruleInfoMap.put(ruleName, new RuleInfo(ruleName, getAutoDescription(ruleName), guessCategory(ruleName)));
                }
            }
        } catch (IOException e) {
            createDefaultDescriptions();
        }
    }

    private void createDefaultDescriptions() {
        for (String ruleName : rules.keySet()) {
            ruleInfoMap.put(ruleName, new RuleInfo(ruleName, getAutoDescription(ruleName), guessCategory(ruleName)));
        }
    }

    private String getAutoDescription(String ruleName) {
        // Generate description from rule name
        String desc = ruleName.replace("_", " ").toLowerCase();
        return desc.substring(0, 1).toUpperCase() + desc.substring(1) + " optimization rule";
    }

    private String guessCategory(String ruleName) {
        if (ruleName.startsWith("FILTER")) return "filter_pushdown";
        if (ruleName.startsWith("PROJECT")) return "projection";
        if (ruleName.startsWith("JOIN") || ruleName.startsWith("MULTI_JOIN")) return "join_optimization";
        if (ruleName.startsWith("AGGREGATE")) return "aggregate";
        if (ruleName.startsWith("SORT") || ruleName.startsWith("LIMIT")) return "sort";
        if (ruleName.startsWith("UNION") || ruleName.startsWith("INTERSECT") || ruleName.startsWith("MINUS")) return "set_operations";
        if (ruleName.startsWith("CALC")) return "calc";
        if (ruleName.startsWith("SEMI_JOIN")) return "semi_join";
        if (ruleName.startsWith("WINDOW")) return "window";
        return "general";
    }

    /**
     * Get a rule by name.
     */
    public RelOptRule getRule(String name) {
        return rules.get(name.toUpperCase().trim());
    }

    /**
     * Get rules by names.
     */
    public List<RelOptRule> getRules(List<String> names) {
        List<RelOptRule> result = new ArrayList<>();
        for (String name : names) {
            RelOptRule rule = getRule(name);
            if (rule != null) {
                result.add(rule);
            }
        }
        return result;
    }

    /**
     * Get all available rule names.
     */
    public Set<String> getAvailableRuleNames() {
        return Collections.unmodifiableSet(rules.keySet());
    }

    /**
     * Get rule info by name.
     */
    public RuleInfo getRuleInfo(String name) {
        return ruleInfoMap.get(name.toUpperCase().trim());
    }

    /**
     * Get all rule info.
     */
    public Collection<RuleInfo> getAllRuleInfo() {
        return Collections.unmodifiableCollection(ruleInfoMap.values());
    }

    /**
     * Get rule count.
     */
    public int getRuleCount() {
        return rules.size();
    }

    /**
     * Format rules for LLM prompt.
     */
    public String formatRulesForPrompt() {
        StringBuilder sb = new StringBuilder();

        // Group by category
        Map<String, List<RuleInfo>> byCategory = new LinkedHashMap<>();
        for (RuleInfo info : ruleInfoMap.values()) {
            byCategory.computeIfAbsent(info.category, k -> new ArrayList<>()).add(info);
        }

        for (Map.Entry<String, List<RuleInfo>> entry : byCategory.entrySet()) {
            String category = entry.getKey().replace("_", " ");
            category = category.substring(0, 1).toUpperCase() + category.substring(1);
            sb.append("\n### ").append(category).append("\n");

            for (RuleInfo info : entry.getValue()) {
                sb.append("- **").append(info.name).append("**: ").append(info.description).append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * Parse rule names from LLM response.
     */
    public List<String> parseRulesFromResponse(String response) {
        List<String> foundRules = new ArrayList<>();

        if (response == null || response.isEmpty()) {
            return foundRules;
        }

        // Clean up the response
        String cleaned = response.trim();

        // Remove common prefixes
        cleaned = cleaned.replaceAll("(?i)^(rules|selected rules|optimal rules|apply|selected)?\\s*:?\\s*", "");

        // Split by common delimiters
        String[] parts = cleaned.split("[,;\\n]+");

        for (String part : parts) {
            String candidate = part.trim()
                    .replaceAll("^[\\-\\*\\d\\.\\)]+\\s*", "")
                    .replaceAll("[\"'`]", "")
                    .toUpperCase()
                    .trim();

            if (rules.containsKey(candidate)) {
                if (!foundRules.contains(candidate)) {
                    foundRules.add(candidate);
                }
            }
        }

        return foundRules;
    }

    /**
     * Rule information holder.
     */
    public static class RuleInfo {
        public final String name;
        public final String description;
        public final String category;

        public RuleInfo(String name, String description, String category) {
            this.name = name;
            this.description = description;
            this.category = category;
        }
    }
}
