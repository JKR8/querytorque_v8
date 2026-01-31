"""DAX Tokenizer and Parser for QueryTorque.

Provides structured analysis of DAX expressions to assist LLM-based pattern detection.
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class Token:
    """A single token from DAX code."""
    type: str  # FUNC, STRING, NUMBER, OPERATOR, PAREN_OPEN, PAREN_CLOSE, TABLE_COL, COMMA
    value: str
    line: int
    column: int


@dataclass
class FunctionCall:
    """Metadata about a function call in DAX."""
    name: str
    line: int
    column: int
    arg_count: int
    args: List[List[Token]]
    nesting_depth: int  # How deep in nested function calls
    parent_func: Optional[str] = None  # Name of containing function if nested


@dataclass
class DAXMetadata:
    """Extracted metadata from a DAX expression."""
    raw_code: str
    tokens: List[Token]
    function_calls: List[FunctionCall]
    max_nesting_depth: int
    tables_referenced: List[str]
    columns_referenced: List[str]
    measures_referenced: List[str]  # [MeasureName] pattern
    has_variables: bool
    variable_names: List[str]
    line_count: int

    # Quick lookups
    iterator_functions: List[FunctionCall] = field(default_factory=list)
    filter_functions: List[FunctionCall] = field(default_factory=list)
    calculate_functions: List[FunctionCall] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "raw_code": self.raw_code,
            "line_count": self.line_count,
            "max_nesting_depth": self.max_nesting_depth,
            "has_variables": self.has_variables,
            "variable_names": self.variable_names,
            "tables_referenced": self.tables_referenced,
            "columns_referenced": self.columns_referenced,
            "measures_referenced": self.measures_referenced,
            "function_calls": [
                {
                    "name": fc.name,
                    "line": fc.line,
                    "arg_count": fc.arg_count,
                    "nesting_depth": fc.nesting_depth,
                    "parent_func": fc.parent_func,
                }
                for fc in self.function_calls
            ],
            "iterator_functions": [
                {"name": fc.name, "line": fc.line, "nesting_depth": fc.nesting_depth}
                for fc in self.iterator_functions
            ],
            "filter_functions": [
                {"name": fc.name, "line": fc.line, "nesting_depth": fc.nesting_depth}
                for fc in self.filter_functions
            ],
            "calculate_functions": [
                {"name": fc.name, "line": fc.line, "nesting_depth": fc.nesting_depth}
                for fc in self.calculate_functions
            ],
        }


class DAXLexer:
    """Tokenizer for DAX expressions.

    Handles comments, strings, table/column references, and nested parentheses.
    """

    # Token specifications
    token_spec = [
        ('COMMENT_BLOCK', r'/\*[\s\S]*?\*/'),  # /* ... */
        ('COMMENT_LINE',  r'//.*'),            # // ...
        ('COMMENT_DASH',  r'--.*'),            # -- ...
        ('STRING',        r'"(?:""|[^"])*"'),  # "string" with escaped quotes
        ('TABLE_COL',     r"'[^']*'\[[^\]]+\]"),  # 'Table'[Column]
        ('TABLE_REF',     r"'[^']*'"),         # 'Table Name'
        ('COLUMN_REF',    r'\[[^\]]+\]'),      # [Column] or [Measure]
        ('NUMBER',        r'\d+(\.\d*)?'),
        ('FUNC',          r'[a-zA-Z_][a-zA-Z0-9_]*'),  # Function names or keywords
        ('PAREN_OPEN',    r'\('),
        ('PAREN_CLOSE',   r'\)'),
        ('COMMA',         r','),
        ('OPERATOR',      r'[+\-*/&|=<>]+'),
        ('SKIP',          r'[ \t\r\n]+'),
        ('MISMATCH',      r'.'),
    ]

    # Compile regex once
    master_re = re.compile('|'.join(f'(?P<{pair[0]}>{pair[1]})' for pair in token_spec))

    def tokenize(self, code: str) -> List[Token]:
        """Tokenize DAX code into a list of tokens."""
        tokens = []
        line_num = 1
        line_start = 0

        for mo in self.master_re.finditer(code):
            kind = mo.lastgroup
            value = mo.group()

            if kind == 'SKIP':
                if '\n' in value:
                    line_num += value.count('\n')
                    line_start = mo.end() - len(value.split('\n')[-1])
                continue
            elif kind in ('COMMENT_BLOCK', 'COMMENT_LINE', 'COMMENT_DASH'):
                # Count newlines in block comments
                if '\n' in value:
                    line_num += value.count('\n')
                continue
            elif kind == 'MISMATCH':
                continue

            column = mo.start() - line_start
            tokens.append(Token(kind, value, line_num, column))

        return tokens


class DAXParser:
    """Analyzes DAX token streams to extract metadata for LLM analysis."""

    # Known DAX iterator functions
    ITERATOR_FUNCTIONS = {
        'SUMX', 'AVERAGEX', 'MINX', 'MAXX', 'COUNTX', 'RANKX',
        'PRODUCTX', 'CONCATENATEX', 'ADDCOLUMNS', 'SELECTCOLUMNS',
        'GENERATE', 'GENERATEALL', 'FILTER', 'TOPN'
    }

    # Functions that modify filter context
    FILTER_FUNCTIONS = {
        'FILTER', 'ALL', 'ALLEXCEPT', 'ALLSELECTED', 'ALLNOBLANKROW',
        'REMOVEFILTERS', 'KEEPFILTERS', 'VALUES', 'DISTINCT'
    }

    # Calculate variants
    CALCULATE_FUNCTIONS = {'CALCULATE', 'CALCULATETABLE'}

    # DAX keywords (not functions)
    KEYWORDS = {'VAR', 'RETURN', 'TRUE', 'FALSE', 'BLANK', 'IN', 'NOT', 'AND', 'OR'}

    def __init__(self, code: str):
        self.code = code
        self.lexer = DAXLexer()
        self.tokens = self.lexer.tokenize(code)

    def analyze(self) -> DAXMetadata:
        """Perform full analysis and return metadata."""
        function_calls = self._extract_function_calls()
        tables, columns, measures = self._extract_references()
        has_vars, var_names = self._extract_variables()

        # Categorize function calls
        iterators = [fc for fc in function_calls if fc.name.upper() in self.ITERATOR_FUNCTIONS]
        filters = [fc for fc in function_calls if fc.name.upper() in self.FILTER_FUNCTIONS]
        calculates = [fc for fc in function_calls if fc.name.upper() in self.CALCULATE_FUNCTIONS]

        max_depth = max((fc.nesting_depth for fc in function_calls), default=0)

        return DAXMetadata(
            raw_code=self.code,
            tokens=self.tokens,
            function_calls=function_calls,
            max_nesting_depth=max_depth,
            tables_referenced=tables,
            columns_referenced=columns,
            measures_referenced=measures,
            has_variables=has_vars,
            variable_names=var_names,
            line_count=self.code.count('\n') + 1,
            iterator_functions=iterators,
            filter_functions=filters,
            calculate_functions=calculates,
        )

    def _extract_function_calls(self) -> List[FunctionCall]:
        """Extract all function calls with their arguments and nesting depth."""
        calls = []
        depth = 0
        func_stack: List[str] = []  # Track nested function names

        i = 0
        while i < len(self.tokens):
            token = self.tokens[i]

            if token.type == 'FUNC' and token.value.upper() not in self.KEYWORDS:
                # Check if next token is (
                if i + 1 < len(self.tokens) and self.tokens[i + 1].type == 'PAREN_OPEN':
                    args = self._get_function_args(i)
                    parent = func_stack[-1] if func_stack else None

                    calls.append(FunctionCall(
                        name=token.value.upper(),
                        line=token.line,
                        column=token.column,
                        arg_count=len(args),
                        args=args,
                        nesting_depth=depth,
                        parent_func=parent,
                    ))

                    func_stack.append(token.value.upper())
                    depth += 1
                    i += 2  # Skip FUNC and (
                    continue

            elif token.type == 'PAREN_OPEN':
                depth += 1
            elif token.type == 'PAREN_CLOSE':
                depth = max(0, depth - 1)
                if func_stack:
                    func_stack.pop()

            i += 1

        return calls

    def _get_function_args(self, start_idx: int) -> List[List[Token]]:
        """Extract arguments of a function call starting at start_idx."""
        if start_idx + 1 >= len(self.tokens) or self.tokens[start_idx + 1].type != 'PAREN_OPEN':
            return []

        args = []
        current_arg: List[Token] = []
        depth = 0

        for i in range(start_idx + 2, len(self.tokens)):
            token = self.tokens[i]

            if token.type == 'PAREN_OPEN':
                depth += 1
                current_arg.append(token)
            elif token.type == 'PAREN_CLOSE':
                if depth == 0:
                    if current_arg:
                        args.append(current_arg)
                    return args
                depth -= 1
                current_arg.append(token)
            elif token.type == 'COMMA':
                if depth == 0:
                    args.append(current_arg)
                    current_arg = []
                else:
                    current_arg.append(token)
            else:
                current_arg.append(token)

        return args

    def _extract_references(self) -> tuple[List[str], List[str], List[str]]:
        """Extract table, column, and measure references."""
        tables = set()
        columns = set()
        measures = set()

        for token in self.tokens:
            if token.type == 'TABLE_COL':
                # 'Table'[Column]
                parts = token.value.split('[')
                tables.add(parts[0].strip("'"))
                columns.add('[' + parts[1])
            elif token.type == 'TABLE_REF':
                # 'Table Name'
                tables.add(token.value.strip("'"))
            elif token.type == 'COLUMN_REF':
                # [Column] or [Measure]
                # Heuristic: if standalone (not preceded by table), likely a measure
                columns.add(token.value)
                # Check if it's likely a measure (no table prefix in nearby context)
                measures.add(token.value)

        return sorted(tables), sorted(columns), sorted(measures)

    def _extract_variables(self) -> tuple[bool, List[str]]:
        """Extract VAR declarations."""
        var_names = []

        for i, token in enumerate(self.tokens):
            if token.type == 'FUNC' and token.value.upper() == 'VAR':
                # Next token should be the variable name
                if i + 1 < len(self.tokens) and self.tokens[i + 1].type == 'FUNC':
                    var_names.append(self.tokens[i + 1].value)

        return len(var_names) > 0, var_names

    def is_table_reference(self, tokens: List[Token]) -> bool:
        """Check if a list of tokens represents a naked table reference."""
        significant = [t for t in tokens if t.type in ('FUNC', 'TABLE_REF', 'TABLE_COL')]

        if len(significant) == 1:
            val = significant[0].value
            if significant[0].type == 'TABLE_REF':
                return True
            if significant[0].type == 'FUNC' and '[' not in val:
                return True
        return False


def analyze_dax(code: str) -> DAXMetadata:
    """Convenience function to analyze DAX code."""
    parser = DAXParser(code)
    return parser.analyze()
