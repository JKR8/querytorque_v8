"""Phase 3: DAX Analyzer Tests - DAX Parser.

Tests for DAX tokenization, parsing, and metadata extraction.
"""

import pytest
from qt_dax.parsers.dax_parser import (
    Token,
    FunctionCall,
    DAXMetadata,
    DAXLexer,
    DAXParser,
    analyze_dax,
)


class TestDAXLexer:
    """Tests for DAX tokenization."""

    def test_tokenize_simple_function(self):
        """Test tokenizing a simple function call."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("SUM('Sales'[Amount])")

        token_types = [t.type for t in tokens]
        assert "FUNC" in token_types
        assert "PAREN_OPEN" in token_types
        assert "TABLE_COL" in token_types
        assert "PAREN_CLOSE" in token_types

    def test_tokenize_string_literal(self):
        """Test tokenizing string literals."""
        lexer = DAXLexer()
        tokens = lexer.tokenize('"Hello World"')

        string_tokens = [t for t in tokens if t.type == "STRING"]
        assert len(string_tokens) == 1
        assert string_tokens[0].value == '"Hello World"'

    def test_tokenize_escaped_quotes_in_string(self):
        """Test tokenizing strings with escaped quotes."""
        lexer = DAXLexer()
        tokens = lexer.tokenize('"Say ""Hello"""')

        string_tokens = [t for t in tokens if t.type == "STRING"]
        assert len(string_tokens) == 1
        assert '""' in string_tokens[0].value

    def test_tokenize_table_column_reference(self):
        """Test tokenizing 'Table'[Column] references."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("'Sales Table'[Amount]")

        table_col_tokens = [t for t in tokens if t.type == "TABLE_COL"]
        assert len(table_col_tokens) == 1

    def test_tokenize_column_only_reference(self):
        """Test tokenizing [Column] references."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("[Total Sales]")

        col_tokens = [t for t in tokens if t.type == "COLUMN_REF"]
        assert len(col_tokens) == 1
        assert col_tokens[0].value == "[Total Sales]"

    def test_tokenize_number(self):
        """Test tokenizing numbers."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("123.45")

        num_tokens = [t for t in tokens if t.type == "NUMBER"]
        assert len(num_tokens) == 1
        assert num_tokens[0].value == "123.45"

    def test_tokenize_operators(self):
        """Test tokenizing operators."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("1 + 2 - 3 * 4 / 5")

        op_tokens = [t for t in tokens if t.type == "OPERATOR"]
        assert len(op_tokens) == 4

    def test_tokenize_single_line_comment(self):
        """Test that single-line comments are skipped."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("SUM(x) // comment\nAVERAGE(y)")

        # Comments should not appear as tokens
        token_values = [t.value for t in tokens]
        assert "// comment" not in token_values

        # But both functions should be tokenized
        func_tokens = [t for t in tokens if t.type == "FUNC"]
        assert len(func_tokens) == 2

    def test_tokenize_block_comment(self):
        """Test that block comments are skipped."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("/* comment */ SUM(x)")

        token_values = [t.value for t in tokens]
        assert "/* comment */" not in token_values

    def test_tokenize_sql_style_comment(self):
        """Test that -- comments are skipped."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("SUM(x) -- comment\nAVERAGE(y)")

        func_tokens = [t for t in tokens if t.type == "FUNC"]
        assert len(func_tokens) == 2

    def test_line_number_tracking(self):
        """Test that line numbers are tracked correctly."""
        lexer = DAXLexer()
        tokens = lexer.tokenize("SUM(\n  'Sales'[Amount]\n)")

        # Find the TABLE_COL token - should be on line 2
        table_col = next(t for t in tokens if t.type == "TABLE_COL")
        assert table_col.line == 2


class TestDAXParser:
    """Tests for DAX parsing and analysis."""

    def test_extract_function_calls(self, sample_simple_dax):
        """Test extracting function calls."""
        parser = DAXParser(sample_simple_dax)
        metadata = parser.analyze()

        func_names = [f.name for f in metadata.function_calls]
        assert "SUM" in func_names

    def test_function_call_nesting_depth(self):
        """Test nesting depth tracking."""
        dax = "CALCULATE(SUM('Sales'[Amount]), ALL('Date'))"
        parser = DAXParser(dax)
        metadata = parser.analyze()

        # CALCULATE is depth 0, SUM and ALL are depth 1
        calculate_call = next(f for f in metadata.function_calls if f.name == "CALCULATE")
        sum_call = next(f for f in metadata.function_calls if f.name == "SUM")

        assert calculate_call.nesting_depth == 0
        assert sum_call.nesting_depth == 1

    def test_function_argument_extraction(self):
        """Test extracting function arguments."""
        dax = "DIVIDE(100, 5, 0)"
        parser = DAXParser(dax)
        metadata = parser.analyze()

        divide_call = next(f for f in metadata.function_calls if f.name == "DIVIDE")
        assert divide_call.arg_count == 3

    def test_table_reference_extraction(self):
        """Test extracting table references."""
        dax = "SUMX('Sales', 'Sales'[Amount] * 'Products'[Price])"
        parser = DAXParser(dax)
        metadata = parser.analyze()

        assert "Sales" in metadata.tables_referenced
        assert "Products" in metadata.tables_referenced

    def test_column_reference_extraction(self):
        """Test extracting column references."""
        dax = "SUM('Sales'[Amount])"
        parser = DAXParser(dax)
        metadata = parser.analyze()

        # Column should be extracted (with or without table)
        assert any("[Amount]" in c for c in metadata.columns_referenced)

    def test_measure_reference_extraction(self):
        """Test extracting measure references."""
        dax = "[Total Sales] + [Total Cost]"
        parser = DAXParser(dax)
        metadata = parser.analyze()

        assert "[Total Sales]" in metadata.measures_referenced
        assert "[Total Cost]" in metadata.measures_referenced

    def test_variable_extraction(self, sample_complex_dax):
        """Test extracting VAR declarations."""
        parser = DAXParser(sample_complex_dax)
        metadata = parser.analyze()

        assert metadata.has_variables
        assert "TotalSales" in metadata.variable_names
        assert "TotalCost" in metadata.variable_names
        assert "Profit" in metadata.variable_names

    def test_iterator_function_identification(self, sample_iterator_dax):
        """Test identifying iterator functions."""
        parser = DAXParser(sample_iterator_dax)
        metadata = parser.analyze()

        assert len(metadata.iterator_functions) >= 1
        iterator_names = [f.name for f in metadata.iterator_functions]
        assert "SUMX" in iterator_names

    def test_filter_function_identification(self):
        """Test identifying filter functions."""
        dax = "CALCULATE(SUM('Sales'[Amount]), ALL('Date'))"
        parser = DAXParser(dax)
        metadata = parser.analyze()

        filter_names = [f.name for f in metadata.filter_functions]
        assert "ALL" in filter_names

    def test_calculate_function_identification(self, sample_calculate_dax):
        """Test identifying CALCULATE functions."""
        parser = DAXParser(sample_calculate_dax)
        metadata = parser.analyze()

        calc_names = [f.name for f in metadata.calculate_functions]
        assert "CALCULATE" in calc_names

    def test_max_nesting_depth(self, sample_nested_calculate_dax):
        """Test max nesting depth calculation."""
        parser = DAXParser(sample_nested_calculate_dax)
        metadata = parser.analyze()

        # Should detect deep nesting
        assert metadata.max_nesting_depth >= 3


class TestAnalyzeDaxFunction:
    """Tests for the analyze_dax convenience function."""

    def test_analyze_dax_returns_metadata(self):
        """Test that analyze_dax returns DAXMetadata."""
        metadata = analyze_dax("SUM('Sales'[Amount])")
        assert isinstance(metadata, DAXMetadata)

    def test_analyze_dax_simple(self, sample_simple_dax):
        """Test analyzing simple DAX."""
        metadata = analyze_dax(sample_simple_dax)
        assert metadata.raw_code == sample_simple_dax
        assert len(metadata.function_calls) >= 1

    def test_analyze_dax_with_variables(self, sample_complex_dax):
        """Test analyzing DAX with variables."""
        metadata = analyze_dax(sample_complex_dax)
        assert metadata.has_variables
        assert len(metadata.variable_names) >= 3


class TestDAXMetadata:
    """Tests for DAXMetadata dataclass."""

    def test_to_dict_serializable(self, sample_complex_dax):
        """Test that to_dict produces serializable output."""
        metadata = analyze_dax(sample_complex_dax)
        result = metadata.to_dict()

        assert isinstance(result, dict)
        assert "raw_code" in result
        assert "function_calls" in result
        assert "max_nesting_depth" in result

        # Should be JSON serializable
        import json
        json_str = json.dumps(result)
        assert len(json_str) > 0

    def test_metadata_line_count(self):
        """Test line count calculation."""
        dax = "VAR x = 1\nVAR y = 2\nRETURN x + y"
        metadata = analyze_dax(dax)
        assert metadata.line_count == 3


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_expression(self):
        """Test handling of empty expression."""
        metadata = analyze_dax("")
        assert metadata.raw_code == ""
        assert len(metadata.function_calls) == 0

    def test_whitespace_only(self):
        """Test handling of whitespace-only expression."""
        metadata = analyze_dax("   \n\t  ")
        assert len(metadata.function_calls) == 0

    def test_comment_only_expression(self, sample_with_comments_dax):
        """Test handling of comments."""
        metadata = analyze_dax(sample_with_comments_dax)
        # Should still extract the actual DAX
        assert metadata.has_variables

    def test_nested_parentheses(self):
        """Test handling of deeply nested parentheses."""
        dax = "IF(IF(IF(1=1, 2, 3) > 1, 4, 5) = 4, 6, 7)"
        metadata = analyze_dax(dax)
        assert len(metadata.function_calls) >= 3

    def test_unicode_in_names(self):
        """Test handling of Unicode in names."""
        dax = "SUM('売上'[金額])"
        metadata = analyze_dax(dax)
        assert len(metadata.function_calls) >= 1

    def test_very_long_expression(self):
        """Test handling of very long expression."""
        # Generate a long expression
        parts = [f"SUM('T{i}'[C{i}])" for i in range(50)]
        dax = " + ".join(parts)
        metadata = analyze_dax(dax)
        assert len(metadata.function_calls) == 50
