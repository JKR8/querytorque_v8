package com.qtcalcite;

import com.qtcalcite.calcite.RuleRegistry;
import com.qtcalcite.llm.ResponseParser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResponseParserTest {

    private ResponseParser parser;

    @BeforeEach
    void setUp() {
        RuleRegistry registry = new RuleRegistry();
        parser = new ResponseParser(registry);
    }

    @Test
    void testParseSimpleResponse() {
        ResponseParser.ParseResult result = parser.parse("FILTER_INTO_JOIN, PROJECT_MERGE");
        assertFalse(result.hasError());
        assertTrue(result.hasRules());
        assertEquals(2, result.getRules().size());
    }

    @Test
    void testParseEmptyResponse() {
        ResponseParser.ParseResult result = parser.parse("");
        assertTrue(result.hasError());
        assertFalse(result.hasRules());
    }

    @Test
    void testParseNullResponse() {
        ResponseParser.ParseResult result = parser.parse(null);
        assertTrue(result.hasError());
        assertFalse(result.hasRules());
    }

    @Test
    void testParseInvalidRules() {
        ResponseParser.ParseResult result = parser.parse("INVALID_RULE, ANOTHER_INVALID");
        assertTrue(result.hasError());
        assertFalse(result.hasRules());
    }

    @Test
    void testValidateRules() {
        List<String> validRules = List.of("FILTER_INTO_JOIN", "PROJECT_MERGE");
        ResponseParser.ValidationResult result = parser.validateRules(validRules);
        assertTrue(result.isAllValid());
        assertEquals(2, result.getValidCount());
    }

    @Test
    void testValidateRulesWithInvalid() {
        List<String> mixedRules = List.of("FILTER_INTO_JOIN", "INVALID_RULE");
        ResponseParser.ValidationResult result = parser.validateRules(mixedRules);
        assertFalse(result.isAllValid());
        assertEquals(1, result.getValidCount());
    }
}
