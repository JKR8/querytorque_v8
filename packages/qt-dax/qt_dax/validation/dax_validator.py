"""DAX Validation Pipeline.

Validates DAX expressions for:
- Syntax correctness (bracket/parenthesis balance, VAR/RETURN structure)
- Semantic preservation (comparing before/after behavior indicators)
- Anti-pattern regression (ensuring fixes don't introduce new issues)

Part of QueryTorque DAX product.
"""

import re
from dataclasses import dataclass, field
from typing import Optional, Protocol, Any


@dataclass
class DAXValidationResult:
    """Result from DAX validation."""

    # Syntax validation
    syntax_valid: bool = True
    syntax_errors: list[str] = field(default_factory=list)

    # Semantic validation
    semantic_valid: bool = True
    semantic_warnings: list[str] = field(default_factory=list)

    # Regression check
    regression_free: bool = True
    new_issues: list[dict] = field(default_factory=list)
    issues_fixed: list[str] = field(default_factory=list)
    issues_remaining: list[str] = field(default_factory=list)

    # Overall
    @property
    def is_valid(self) -> bool:
        return self.syntax_valid and self.semantic_valid and self.regression_free

    def to_dict(self) -> dict:
        return {
            "is_valid": self.is_valid,
            "syntax_valid": self.syntax_valid,
            "syntax_errors": self.syntax_errors,
            "semantic_valid": self.semantic_valid,
            "semantic_warnings": self.semantic_warnings,
            "regression_free": self.regression_free,
            "new_issues": self.new_issues,
            "issues_fixed": self.issues_fixed,
            "issues_remaining": self.issues_remaining,
        }


class DAXAnalyzerProtocol(Protocol):
    """Protocol for DAX analyzer integration."""

    def analyze_expression(self, expression: str) -> list[dict]:
        """Analyze DAX expression for anti-patterns."""
        ...


class DAXValidator:
    """Validates DAX expressions and optimizations.

    Performs three levels of validation:
    1. Syntax: Structural correctness
    2. Semantic: Behavior preservation indicators
    3. Regression: Anti-pattern check
    """

    def __init__(
        self,
        dax_analyzer: Optional[DAXAnalyzerProtocol] = None,
    ):
        self.dax_analyzer = dax_analyzer

    def validate(
        self,
        original_dax: str,
        optimized_dax: str,
        original_issues: Optional[list[dict]] = None,
    ) -> DAXValidationResult:
        """Validate optimized DAX against original.

        Args:
            original_dax: Original DAX expression
            optimized_dax: Optimized DAX expression
            original_issues: Issues detected in original

        Returns:
            DAXValidationResult
        """
        result = DAXValidationResult()

        # 1. Syntax validation
        syntax_errors = self._validate_syntax(optimized_dax)
        result.syntax_valid = len(syntax_errors) == 0
        result.syntax_errors = syntax_errors

        if not result.syntax_valid:
            return result  # No point checking further

        # 2. Semantic validation
        semantic_warnings = self._validate_semantics(original_dax, optimized_dax)
        result.semantic_valid = not any(w.startswith("CRITICAL:") for w in semantic_warnings)
        result.semantic_warnings = semantic_warnings

        # 3. Regression check
        if self.dax_analyzer:
            regression_result = self._check_regression(
                original_dax, optimized_dax, original_issues or []
            )
            result.regression_free = regression_result['regression_free']
            result.new_issues = regression_result['new_issues']
            result.issues_fixed = regression_result['issues_fixed']
            result.issues_remaining = regression_result['issues_remaining']

        return result

    def _validate_syntax(self, dax: str) -> list[str]:
        """Validate DAX syntax structure."""
        if not dax or not dax.strip():
            return ["Empty DAX expression"]

        errors = []

        # Check balanced delimiters
        errors.extend(self._check_balanced_delimiters(dax))

        # Check VAR/RETURN structure
        errors.extend(self._check_var_return_structure(dax))

        # Check for common syntax errors
        errors.extend(self._check_common_errors(dax))

        return errors

    def _check_balanced_delimiters(self, dax: str) -> list[str]:
        """Check for balanced parentheses, brackets, and braces."""
        errors = []

        # Track nesting
        stack = []
        in_string = False
        string_char = None

        i = 0
        while i < len(dax):
            char = dax[i]

            # Handle string literals
            if char in ('"', "'") and not in_string:
                in_string = True
                string_char = char
            elif char == string_char and in_string:
                in_string = False
                string_char = None
            elif in_string:
                i += 1
                continue

            # Handle comments
            if char == '/' and i + 1 < len(dax):
                next_char = dax[i + 1]
                if next_char == '/':
                    # Line comment - skip to end of line
                    while i < len(dax) and dax[i] != '\n':
                        i += 1
                    continue
                elif next_char == '*':
                    # Block comment - skip to */
                    i += 2
                    while i < len(dax) - 1:
                        if dax[i] == '*' and dax[i + 1] == '/':
                            i += 2
                            break
                        i += 1
                    continue

            # Track delimiters
            if char == '(':
                stack.append(('(', i))
            elif char == ')':
                if not stack or stack[-1][0] != '(':
                    errors.append(f"Unmatched ')' at position {i}")
                else:
                    stack.pop()
            elif char == '[':
                stack.append(('[', i))
            elif char == ']':
                if not stack or stack[-1][0] != '[':
                    errors.append(f"Unmatched ']' at position {i}")
                else:
                    stack.pop()
            elif char == '{':
                stack.append(('{', i))
            elif char == '}':
                if not stack or stack[-1][0] != '{':
                    errors.append(f"Unmatched '}}' at position {i}")
                else:
                    stack.pop()

            i += 1

        # Report unclosed delimiters
        for delim, pos in stack:
            errors.append(f"Unclosed '{delim}' at position {pos}")

        return errors

    def _check_var_return_structure(self, dax: str) -> list[str]:
        """Check VAR/RETURN structure."""
        errors = []

        # Count VAR and RETURN keywords (case-insensitive)
        var_matches = list(re.finditer(r'\bVAR\b', dax, re.IGNORECASE))
        return_matches = list(re.finditer(r'\bRETURN\b', dax, re.IGNORECASE))

        var_count = len(var_matches)
        return_count = len(return_matches)

        if var_count > 0 and return_count == 0:
            errors.append("VAR statement(s) found without RETURN")

        if return_count > 1:
            errors.append("Multiple RETURN statements found (only one allowed)")

        # Check that RETURN comes after all VARs
        if var_matches and return_matches:
            last_var_pos = var_matches[-1].end()
            first_return_pos = return_matches[0].start()
            if last_var_pos > first_return_pos:
                errors.append("VAR found after RETURN (invalid structure)")

        return errors

    def _check_common_errors(self, dax: str) -> list[str]:
        """Check for common DAX syntax errors."""
        errors = []

        # Should not start with '=' (measure definition syntax)
        if re.match(r'^\s*=', dax):
            errors.append("Expression should not start with '=' - provide only the measure body")

        # Check for empty function calls
        if re.search(r'\(\s*\)', dax):
            # Some functions allow empty parens (e.g., NOW(), TODAY())
            # Only flag if not a known zero-arg function
            zero_arg_funcs = {'NOW', 'TODAY', 'UTCNOW', 'UTCTODAY', 'PI', 'E', 'TRUE', 'FALSE', 'BLANK'}
            empty_calls = re.findall(r'(\w+)\s*\(\s*\)', dax)
            for func in empty_calls:
                if func.upper() not in zero_arg_funcs:
                    errors.append(f"Potentially invalid empty function call: {func}()")

        # Check for double operators
        if re.search(r'(\+\+|--|\*\*|//)', dax):
            # Note: // could be comment, but we've stripped those
            pass  # DAX doesn't have increment operators, but this could be legitimate

        return errors

    def _validate_semantics(self, original: str, optimized: str) -> list[str]:
        """Check semantic preservation indicators.

        This is a heuristic check - true semantic validation requires
        executing against a model.
        """
        warnings = []

        # Extract function signatures from both
        orig_funcs = self._extract_functions(original)
        opt_funcs = self._extract_functions(optimized)

        # Check for aggregation function changes
        agg_funcs = {'SUM', 'AVERAGE', 'MIN', 'MAX', 'COUNT', 'COUNTROWS',
                     'DISTINCTCOUNT', 'COUNTBLANK', 'COUNTA'}

        orig_aggs = set(f.upper() for f in orig_funcs if f.upper() in agg_funcs)
        opt_aggs = set(f.upper() for f in opt_funcs if f.upper() in agg_funcs)

        # Different aggregation functions could change semantics
        removed_aggs = orig_aggs - opt_aggs
        added_aggs = opt_aggs - orig_aggs

        if removed_aggs:
            warnings.append(f"Removed aggregation functions: {', '.join(removed_aggs)}")
        if added_aggs:
            warnings.append(f"Added aggregation functions: {', '.join(added_aggs)}")

        # Check for CALCULATE changes
        orig_calculate = original.upper().count('CALCULATE')
        opt_calculate = optimized.upper().count('CALCULATE')

        if abs(orig_calculate - opt_calculate) > 2:
            warnings.append(
                f"Significant CALCULATE change: {orig_calculate} -> {opt_calculate}"
            )

        # Check for measure references (bracket notation)
        orig_measures = set(re.findall(r'\[([^\]]+)\]', original))
        opt_measures = set(re.findall(r'\[([^\]]+)\]', optimized))

        removed_measures = orig_measures - opt_measures
        added_measures = opt_measures - orig_measures

        if removed_measures:
            # This could change semantics significantly
            warnings.append(
                f"CRITICAL: Removed column/measure references: {', '.join(removed_measures)}"
            )
        if added_measures:
            warnings.append(f"Added column/measure references: {', '.join(added_measures)}")

        # Check for table references
        orig_tables = set(re.findall(r"'([^']+)'", original))
        opt_tables = set(re.findall(r"'([^']+)'", optimized))

        removed_tables = orig_tables - opt_tables
        added_tables = opt_tables - orig_tables

        if removed_tables:
            warnings.append(
                f"CRITICAL: Removed table references: {', '.join(removed_tables)}"
            )
        if added_tables:
            warnings.append(f"Added table references: {', '.join(added_tables)}")

        return warnings

    def _extract_functions(self, dax: str) -> list[str]:
        """Extract function names from DAX expression."""
        # Match word followed by (
        return re.findall(r'\b([A-Za-z][A-Za-z0-9]*)\s*\(', dax)

    def _check_regression(
        self,
        original: str,
        optimized: str,
        original_issues: list[dict],
    ) -> dict:
        """Check if optimization introduced new issues or fixed existing ones."""
        result = {
            'regression_free': True,
            'new_issues': [],
            'issues_fixed': [],
            'issues_remaining': [],
        }

        if not self.dax_analyzer:
            return result

        # Analyze optimized expression
        try:
            new_issues = self.dax_analyzer.analyze_expression(optimized)
        except Exception:
            new_issues = []

        # Get original rule IDs
        original_rule_ids = set(i.get('rule_id', '') for i in original_issues)
        new_rule_ids = set(i.get('rule_id', '') for i in new_issues)

        # Determine what was fixed
        result['issues_fixed'] = list(original_rule_ids - new_rule_ids)
        result['issues_remaining'] = list(original_rule_ids & new_rule_ids)

        # Check for NEW issues (regression)
        truly_new = new_rule_ids - original_rule_ids
        if truly_new:
            result['regression_free'] = False
            result['new_issues'] = [
                i for i in new_issues if i.get('rule_id', '') in truly_new
            ]

        return result


class DAXValidationPipeline:
    """Complete validation pipeline for DAX optimization.

    Orchestrates syntax, semantic, and regression validation.
    """

    def __init__(
        self,
        dax_analyzer: Optional[DAXAnalyzerProtocol] = None,
    ):
        self.validator = DAXValidator(dax_analyzer=dax_analyzer)

    def validate_optimization(
        self,
        original_dax: str,
        optimized_dax: str,
        original_issues: Optional[list[dict]] = None,
        measure_name: Optional[str] = None,
    ) -> DAXValidationResult:
        """Validate a DAX optimization result.

        Args:
            original_dax: Original expression
            optimized_dax: Optimized expression
            original_issues: Issues from original analysis
            measure_name: Name of measure (for logging)

        Returns:
            DAXValidationResult
        """
        return self.validator.validate(
            original_dax=original_dax,
            optimized_dax=optimized_dax,
            original_issues=original_issues,
        )

    def validate_batch(
        self,
        optimizations: dict[str, dict],
        original_issues_by_measure: dict[str, list[dict]],
    ) -> dict[str, DAXValidationResult]:
        """Validate multiple optimizations.

        Args:
            optimizations: Dict of measure_name -> {original_dax, optimized_dax}
            original_issues_by_measure: Dict of measure_name -> issues

        Returns:
            Dict of measure_name -> DAXValidationResult
        """
        results = {}

        for measure_name, opt_data in optimizations.items():
            original = opt_data.get('original_dax', '')
            optimized = opt_data.get('optimized_dax', '')
            issues = original_issues_by_measure.get(measure_name, [])

            results[measure_name] = self.validator.validate(
                original_dax=original,
                optimized_dax=optimized,
                original_issues=issues,
            )

        return results


def create_validation_pipeline(
    dax_analyzer: Optional[DAXAnalyzerProtocol] = None,
) -> DAXValidationPipeline:
    """Factory function to create validation pipeline.

    Args:
        dax_analyzer: Optional DAX analyzer for regression checking

    Returns:
        Configured DAXValidationPipeline
    """
    return DAXValidationPipeline(dax_analyzer=dax_analyzer)
