package com.qtcalcite;

import com.qtcalcite.calcite.RuleRegistry;
import org.apache.calcite.plan.RelOptRule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class RuleRegistryTest {

    private RuleRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new RuleRegistry();
    }

    @Test
    void testGetAvailableRules() {
        Set<String> rules = registry.getAvailableRuleNames();
        assertFalse(rules.isEmpty());
        assertTrue(rules.contains("FILTER_INTO_JOIN"));
        assertTrue(rules.contains("PROJECT_MERGE"));
        assertTrue(rules.contains("JOIN_COMMUTE"));
    }

    @Test
    void testGetRule() {
        RelOptRule rule = registry.getRule("FILTER_INTO_JOIN");
        assertNotNull(rule);

        // Case insensitive
        RelOptRule ruleLower = registry.getRule("filter_into_join");
        assertNotNull(ruleLower);
    }

    @Test
    void testGetRuleInfo() {
        RuleRegistry.RuleInfo info = registry.getRuleInfo("FILTER_INTO_JOIN");
        assertNotNull(info);
        assertEquals("FILTER_INTO_JOIN", info.name);
        assertNotNull(info.description);
        assertNotNull(info.category);
    }

    @Test
    void testParseRulesFromResponse() {
        // Simple comma-separated
        List<String> rules1 = registry.parseRulesFromResponse("FILTER_INTO_JOIN, PROJECT_MERGE");
        assertEquals(2, rules1.size());
        assertTrue(rules1.contains("FILTER_INTO_JOIN"));
        assertTrue(rules1.contains("PROJECT_MERGE"));

        // With prefix text
        List<String> rules2 = registry.parseRulesFromResponse("Rules: FILTER_INTO_JOIN, JOIN_COMMUTE");
        assertEquals(2, rules2.size());

        // With list markers
        List<String> rules3 = registry.parseRulesFromResponse("1. FILTER_INTO_JOIN\n2. PROJECT_MERGE");
        assertEquals(2, rules3.size());

        // Mixed valid and invalid
        List<String> rules4 = registry.parseRulesFromResponse("FILTER_INTO_JOIN, INVALID_RULE, PROJECT_MERGE");
        assertEquals(2, rules4.size());
    }

    @Test
    void testFormatRulesForPrompt() {
        String formatted = registry.formatRulesForPrompt();
        assertNotNull(formatted);
        assertFalse(formatted.isEmpty());
        assertTrue(formatted.contains("FILTER_INTO_JOIN"));
        assertTrue(formatted.contains("PROJECT_MERGE"));
    }
}
