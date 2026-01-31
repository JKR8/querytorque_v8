package com.qtcalcite.llm;

import com.qtcalcite.calcite.RuleRegistry;

import java.util.List;

/**
 * Parses LLM responses to extract rule selections.
 */
public class ResponseParser {

    private final RuleRegistry ruleRegistry;

    public ResponseParser(RuleRegistry ruleRegistry) {
        this.ruleRegistry = ruleRegistry;
    }

    /**
     * Parse rules from LLM response text.
     */
    public ParseResult parse(String response) {
        if (response == null || response.trim().isEmpty()) {
            return new ParseResult(List.of(), "Empty response");
        }

        List<String> rules = ruleRegistry.parseRulesFromResponse(response);

        if (rules.isEmpty()) {
            return new ParseResult(rules, "No valid rules found in response: " + response.trim());
        }

        return new ParseResult(rules, null);
    }

    /**
     * Validate that all rule names are valid.
     */
    public ValidationResult validateRules(List<String> ruleNames) {
        StringBuilder errors = new StringBuilder();
        int validCount = 0;

        for (String name : ruleNames) {
            if (ruleRegistry.getRule(name) != null) {
                validCount++;
            } else {
                errors.append("Unknown rule: ").append(name).append("\n");
            }
        }

        boolean allValid = validCount == ruleNames.size();
        return new ValidationResult(allValid, validCount, errors.toString().trim());
    }

    /**
     * Result of parsing LLM response.
     */
    public static class ParseResult {
        private final List<String> rules;
        private final String error;

        public ParseResult(List<String> rules, String error) {
            this.rules = rules;
            this.error = error;
        }

        public List<String> getRules() { return rules; }
        public String getError() { return error; }
        public boolean hasError() { return error != null; }
        public boolean hasRules() { return !rules.isEmpty(); }
    }

    /**
     * Result of rule validation.
     */
    public static class ValidationResult {
        private final boolean allValid;
        private final int validCount;
        private final String errors;

        public ValidationResult(boolean allValid, int validCount, String errors) {
            this.allValid = allValid;
            this.validCount = validCount;
            this.errors = errors;
        }

        public boolean isAllValid() { return allValid; }
        public int getValidCount() { return validCount; }
        public String getErrors() { return errors; }
    }
}
