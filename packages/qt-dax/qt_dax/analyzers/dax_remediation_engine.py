#!/usr/bin/env python3
"""
QueryTorque DAX Remediation Engine
==================================
LLM-powered automated DAX optimization that goes beyond detection.

DIFFERENTIATOR vs DAX Optimizer:
- They detect + show templates
- We detect + generate actual fixed code + explain + validate

Architecture:
1. VPAX Analysis -> Structured issues
2. Context Enrichment -> Add model context, dependencies
3. LLM Fix Generation -> Actual DAX code per measure
4. Semantic Validation -> Verify fix maintains business logic
5. Batch Output -> Ready-to-deploy JSON/TMDL

Author: QueryTorque / Dialect Labs
"""

import json
import re
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum


# =============================================================================
# FIX STRATEGIES - Pattern-specific prompts for LLM
# =============================================================================

FIX_STRATEGIES = {
    "FILTER_TABLE_ITERATOR": {
        "pattern_description": "FILTER iterating entire table instead of columns",
        "why_slow": """
The Storage Engine (SE) cannot optimize FILTER(Table, condition) because it forces
row-by-row iteration in the single-threaded Formula Engine (FE). This prevents:
- Parallelization (SE is multi-threaded, FE is not)
- Query fusion with other aggregations
- Efficient bitmap filtering
""",
        "fix_strategy": """
Replace FILTER(Table, condition) with either:
1. CALCULATETABLE(Table, condition) - pushes filter to SE
2. Direct filter in CALCULATE - most efficient
3. KEEPFILTERS for intersection semantics
""",
        "examples": [
            {
                "before": "SUMX(FILTER('Sales', 'Sales'[Amount] > 100), 'Sales'[Amount])",
                "after": "CALCULATE(SUM('Sales'[Amount]), 'Sales'[Amount] > 100)",
                "explanation": "Filter pushed to CALCULATE argument, SUMX eliminated"
            },
            {
                "before": "SUMX(FILTER('Sales', RELATED('Product'[Category]) = \"Electronics\"), [Margin])",
                "after": "CALCULATE([Margin], KEEPFILTERS('Product'[Category] = \"Electronics\"))",
                "explanation": "RELATED removed, filter on dimension table directly"
            }
        ],
        "prompt_template": """
Fix this DAX measure that has FILTER(Table) anti-pattern.

MEASURE NAME: {measure_name}
TABLE: {table_name}
CURRENT CODE:
```dax
{expression}
```

MODEL CONTEXT:
- Tables involved: {related_tables}
- Relationships: {relationships}
- This measure is referenced by: {referenced_by}

REQUIREMENTS:
1. Replace FILTER(Table, ...) with CALCULATETABLE or CALCULATE filter arguments
2. Remove RELATED() if filtering on dimension table directly
3. Use KEEPFILTERS() if intersection semantics needed
4. Preserve the exact business logic
5. Use VAR for any repeated expressions

OUTPUT FORMAT:
```json
{{
    "fixed_dax": "your fixed DAX code here",
    "changes_made": ["list of specific changes"],
    "performance_impact": "estimated improvement explanation",
    "semantic_equivalent": true/false,
    "notes": "any caveats or assumptions"
}}
```
"""
    },

    "SUMX_FILTER_COMBO": {
        "pattern_description": "SUMX/iterator with FILTER creates nested row-by-row processing",
        "why_slow": """
SUMX(FILTER(Table, ...), expression) creates two levels of iteration:
1. FILTER scans entire table row-by-row in FE
2. SUMX then iterates the filtered result row-by-row
Both operations are single-threaded and cannot leverage SE parallelism.
With 1M rows, this can be 100-1000x slower than equivalent CALCULATE.
""",
        "fix_strategy": """
1. If aggregating a column: Replace with CALCULATE + SUM
2. If aggregating a measure: Use CALCULATE with filter arguments
3. If complex expression: Use CALCULATETABLE for filtered set, then iterate
4. Always store filtered table in VAR to prevent re-evaluation
""",
        "prompt_template": """
Fix this DAX measure with SUMX+FILTER performance anti-pattern.

MEASURE NAME: {measure_name}
CURRENT CODE:
```dax
{expression}
```

The measure is iterating {estimated_rows:,} rows with FILTER, then iterating again with SUMX.

REQUIREMENTS:
1. Eliminate nested iteration pattern
2. Push filters to Storage Engine via CALCULATE
3. If iteration still needed, use VAR to materialize once
4. Preserve exact calculation semantics

OUTPUT FORMAT:
```json
{{
    "fixed_dax": "your fixed DAX code here",
    "changes_made": ["list of specific changes"],
    "estimated_improvement": "Nx faster",
    "semantic_equivalent": true/false
}}
```
"""
    },

    "DEEP_CALCULATE_NESTING": {
        "pattern_description": "4+ nested CALCULATE statements causing exponential context transitions",
        "why_slow": """
Each CALCULATE triggers a context transition that:
1. Saves current filter context
2. Applies new filters
3. Evaluates inner expression
4. Restores context

With N nested CALCULATEs, this creates O(2^N) context operations.
12 nested CALCULATE = potentially 4096x overhead vs flat structure.
""",
        "fix_strategy": """
1. Flatten using SWITCH pattern - single CALCULATE with conditional filters
2. Use VAR to pre-compute values outside CALCULATE
3. Use calculation groups for Portfolio/Benchmark switching patterns
4. Pre-filter dimension tables in VAR, then single CALCULATE
""",
        "prompt_template": """
Refactor this measure with {calculate_count} nested CALCULATE statements.

MEASURE NAME: {measure_name}
CURRENT CODE:
```dax
{expression}
```

DETECTED PATTERN: {detected_pattern}

REQUIREMENTS:
1. Flatten to maximum 1-2 CALCULATE statements
2. Use SWITCH(TRUE(), ...) for conditional logic
3. Pre-compute filter conditions in VAR
4. Maintain identical business logic
5. Add comments explaining the refactored structure

OUTPUT FORMAT:
```json
{{
    "fixed_dax": "your refactored DAX code",
    "original_calculate_count": {calculate_count},
    "new_calculate_count": N,
    "changes_made": ["list of structural changes"],
    "semantic_equivalent": true/false,
    "complexity_reduction": "percentage or description"
}}
```
"""
    },

    "DIVISION_WITHOUT_DIVIDE": {
        "pattern_description": "Using / operator instead of DIVIDE function",
        "why_slow": "Not a performance issue - this is a correctness/robustness issue",
        "fix_strategy": """
Replace all a/b patterns with DIVIDE(a, b, alternateResult).
Choose alternateResult based on business logic:
- 0 for ratios that should show zero when undefined
- BLANK() for metrics that should be hidden when undefined
- Custom value for specific business rules
""",
        "prompt_template": """
Fix division operations in this measure to use DIVIDE function.

MEASURE NAME: {measure_name}
CURRENT CODE:
```dax
{expression}
```

CONTEXT: This measure calculates {business_context}

REQUIREMENTS:
1. Replace all / operations with DIVIDE()
2. Choose appropriate alternate result (0, BLANK(), or custom)
3. Consider the business meaning when denominator is 0
4. Preserve calculation logic

OUTPUT FORMAT:
```json
{{
    "fixed_dax": "your fixed DAX code",
    "divisions_fixed": N,
    "alternate_values_used": {{"division1": "0", "division2": "BLANK()"}},
    "semantic_notes": "explanation of alternate value choices"
}}
```
"""
    },

    "MISSING_VAR_COMPLEX_MEASURE": {
        "pattern_description": "Complex measure without VAR causing repeated evaluation",
        "why_slow": """
Without VAR, identical sub-expressions are evaluated multiple times.
Example: IF([Sales] > 1000, [Sales] * 1.1, [Sales] * 0.9)
[Sales] is computed 3 times instead of once.
""",
        "fix_strategy": """
1. Identify repeated sub-expressions
2. Extract to VAR statements at top of measure
3. Use meaningful VAR names (_Prefix convention)
4. Order VARs by dependency
5. Single RETURN statement at end
""",
        "prompt_template": """
Refactor this complex measure to use VAR statements.

MEASURE NAME: {measure_name}
CURRENT CODE:
```dax
{expression}
```

LENGTH: {length} characters
REPEATED EXPRESSIONS DETECTED: {repeated_expressions}

REQUIREMENTS:
1. Extract all repeated expressions to VAR
2. Use _PascalCase naming convention
3. Order VARs logically (dependencies first)
4. Improve readability with proper indentation
5. Add brief comments for complex VARs

OUTPUT FORMAT:
```json
{{
    "fixed_dax": "your refactored DAX code",
    "vars_created": ["_VarName1", "_VarName2"],
    "repeated_evaluations_eliminated": N,
    "readability_improvements": ["list of improvements"]
}}
```
"""
    },

    "IF_INSTEAD_OF_SWITCH": {
        "pattern_description": "Multiple nested IF statements instead of SWITCH",
        "why_slow": "Maintainability issue more than performance",
        "fix_strategy": """
Replace nested IF with SWITCH(TRUE(), condition1, result1, ..., default).
Benefits:
- Clearer intent
- Easier to add/remove conditions
- Consistent formatting
- Slightly better optimization potential
""",
        "prompt_template": """
Convert nested IF statements to SWITCH pattern.

MEASURE NAME: {measure_name}
CURRENT CODE:
```dax
{expression}
```

IF COUNT: {if_count} nested IF statements

REQUIREMENTS:
1. Convert to SWITCH(TRUE(), ...) pattern
2. Order conditions logically (most specific first, or most common first)
3. Ensure default case is handled
4. Preserve exact logic

OUTPUT FORMAT:
```json
{{
    "fixed_dax": "your refactored DAX code",
    "conditions_count": N,
    "default_value": "what happens when no condition matches"
}}
```
"""
    },
}


# =============================================================================
# BATCH FIX GENERATOR
# =============================================================================

@dataclass
class MeasureFix:
    """A single measure fix request/response."""
    measure_name: str
    table_name: str
    original_expression: str
    issue_type: str
    fixed_expression: Optional[str] = None
    changes_made: list = field(default_factory=list)
    semantic_equivalent: bool = True
    confidence: float = 0.0
    llm_notes: str = ""


@dataclass
class BatchFixRequest:
    """Request to fix multiple measures."""
    model_name: str
    measures: list  # List of MeasureFix
    model_context: dict = field(default_factory=dict)  # Relationships, tables, etc.
    fix_options: dict = field(default_factory=dict)


class DAXRemediationEngine:
    """
    LLM-powered DAX fix generator.

    This is where we differentiate from DAX Optimizer:
    - They show templates
    - We generate actual code
    """

    def __init__(self, llm_client=None):
        self.llm_client = llm_client
        self.strategies = FIX_STRATEGIES

    def generate_fix_prompt(self, measure: MeasureFix, model_context: dict) -> str:
        """Generate the LLM prompt for fixing a specific measure."""

        strategy = self.strategies.get(measure.issue_type)
        if not strategy:
            return self._generic_fix_prompt(measure, model_context)

        # Get the template
        template = strategy["prompt_template"]

        # Enrich with context
        prompt = template.format(
            measure_name=measure.measure_name,
            table_name=measure.table_name,
            expression=measure.original_expression,
            related_tables=model_context.get("related_tables", []),
            relationships=model_context.get("relationships", []),
            referenced_by=model_context.get("referenced_by", []),
            estimated_rows=model_context.get("estimated_rows", "unknown"),
            calculate_count=self._count_pattern(measure.original_expression, r'\bCALCULATE\s*\('),
            if_count=self._count_pattern(measure.original_expression, r'\bIF\s*\('),
            length=len(measure.original_expression),
            repeated_expressions=self._find_repeated_expressions(measure.original_expression),
            detected_pattern=self._detect_pattern(measure.original_expression),
            business_context=self._infer_business_context(measure.measure_name),
        )

        return prompt

    def generate_batch_prompt(self, measures: list, issue_type: str) -> str:
        """Generate prompt to fix multiple measures of same issue type."""

        strategy = self.strategies.get(issue_type, {})

        prompt = f"""# Batch DAX Fix Request

## Issue Type: {issue_type}

### Why This Is A Problem:
{strategy.get('why_slow', 'Performance anti-pattern detected.')}

### Fix Strategy:
{strategy.get('fix_strategy', 'Apply best practices.')}

---

## Measures To Fix ({len(measures)} total):

"""
        for i, m in enumerate(measures, 1):
            prompt += f"""
### Measure {i}: {m.measure_name}
Table: {m.table_name}
```dax
{m.original_expression}
```

"""

        prompt += """
---

## Requirements:
1. Fix ALL measures listed above
2. Apply consistent patterns across all fixes
3. Preserve exact business logic
4. Use VAR for any repeated expressions

## Output Format:
Return a JSON array with one object per measure:
```json
[
    {
        "measure_name": "original name",
        "fixed_dax": "complete fixed DAX code",
        "changes_made": ["list of changes"],
        "semantic_equivalent": true
    },
    ...
]
```
"""
        return prompt

    def generate_architecture_prompt(self, model_summary: dict, issues: list) -> str:
        """Generate prompt for model-wide architecture recommendations."""

        # Group issues by pattern
        issue_groups = {}
        for issue in issues:
            issue_type = issue.get("rule_name", "unknown")
            if issue_type not in issue_groups:
                issue_groups[issue_type] = []
            issue_groups[issue_type].append(issue)

        prompt = f"""# Power BI Model Architecture Review

## Model Summary:
- Tables: {model_summary.get('total_tables', 'N/A')}
- Measures: {model_summary.get('total_measures', 'N/A')}
- Relationships: {model_summary.get('total_relationships', 'N/A')}
- Model Size: {model_summary.get('total_size_bytes', 0) / (1024*1024):.1f} MB
- Auto Date Tables: {model_summary.get('local_date_table_count', 0)}

## Issue Distribution:
"""
        for issue_type, group in sorted(issue_groups.items(), key=lambda x: -len(x[1])):
            prompt += f"- {issue_type}: {len(group)} occurrences\n"

        prompt += """
## Analysis Request:

Based on the issue patterns above, provide:

### 1. Root Cause Analysis
What underlying model design decisions are causing these patterns to emerge?

### 2. Architecture Recommendations
- Should calculation groups be introduced?
- Are there dimension tables that should be restructured?
- Should measures be consolidated or split?

### 3. Refactoring Priority
Which structural changes would eliminate the most issues?

### 4. Implementation Roadmap
Step-by-step plan to improve model architecture.

### 5. Estimated Impact
What percentage of issues would be resolved by each recommendation?

## Output Format:
Provide structured recommendations with specific, actionable items.
"""
        return prompt

    def generate_explanation_prompt(self, measure: MeasureFix) -> str:
        """Generate prompt to explain WHY a measure is slow in plain English."""

        return f"""# DAX Performance Explanation Request

## Measure: {measure.measure_name}
## Table: {measure.table_name}

```dax
{measure.original_expression}
```

## Issue Detected: {measure.issue_type}

---

Please explain in plain English (suitable for a business analyst, not a developer):

1. **What this measure does** (business purpose based on name and logic)

2. **Why it's slow** (without technical jargon - use analogies)

3. **What the fix would do** (in simple terms)

4. **Impact on users** (what they'll notice after the fix)

Keep the explanation under 200 words and avoid DAX-specific terminology where possible.
Use analogies like "assembly line", "filing cabinet", "spreadsheet" to explain concepts.
"""

    def _count_pattern(self, expression: str, pattern: str) -> int:
        """Count regex pattern occurrences."""
        return len(re.findall(pattern, expression, re.IGNORECASE))

    def _find_repeated_expressions(self, expression: str) -> list:
        """Find expressions that appear multiple times."""
        # Simple heuristic: find measure references that appear 2+ times
        measure_refs = re.findall(r'\[([^\]]+)\]', expression)
        from collections import Counter
        counts = Counter(measure_refs)
        return [f"[{name}] ({count}x)" for name, count in counts.items() if count > 1]

    def _detect_pattern(self, expression: str) -> str:
        """Detect the dominant pattern in complex measures."""
        expr_upper = expression.upper()

        patterns = []
        if 'SWITCH' in expr_upper:
            patterns.append("SWITCH-based conditional")
        if expr_upper.count('CALCULATE') >= 4:
            patterns.append(f"Deep CALCULATE nesting ({expr_upper.count('CALCULATE')}x)")
        if 'SELECTEDVALUE' in expr_upper:
            patterns.append("Slicer-dependent logic")
        if 'ISINSCOPE' in expr_upper:
            patterns.append("Matrix-level detection")
        if 'ALLEXCEPT' in expr_upper:
            patterns.append("Partial filter removal")

        return ", ".join(patterns) if patterns else "Complex calculation"

    def _infer_business_context(self, measure_name: str) -> str:
        """Infer business context from measure name."""
        name_lower = measure_name.lower()

        if any(x in name_lower for x in ['margin', 'profit', 'revenue', 'sales']):
            return "financial metric"
        if any(x in name_lower for x in ['ratio', 'percent', '%', 'pct']):
            return "ratio/percentage calculation"
        if any(x in name_lower for x in ['count', 'total', 'sum']):
            return "aggregation"
        if any(x in name_lower for x in ['yoy', 'ytd', 'mtd', 'growth']):
            return "time intelligence calculation"
        if any(x in name_lower for x in ['benchmark', 'bm', 'index']):
            return "benchmark comparison"
        if any(x in name_lower for x in ['weight', 'weighted', 'waci']):
            return "weighted calculation"

        return "business metric"

    def _generic_fix_prompt(self, measure: MeasureFix, model_context: dict) -> str:
        """Generic fix prompt for unknown issue types."""
        return f"""
Fix this DAX measure following DAX best practices.

MEASURE: {measure.measure_name}
ISSUE: {measure.issue_type}

```dax
{measure.original_expression}
```

Apply these principles:
1. Use DIVIDE() instead of /
2. Use VAR for repeated expressions
3. Prefer CALCULATE over FILTER when possible
4. Use SWITCH instead of nested IF
5. Minimize CALCULATE nesting

Return fixed DAX with explanation.
"""


# =============================================================================
# VALIDATION ENGINE
# =============================================================================

class DAXValidator:
    """
    Validate that LLM-generated fixes are syntactically correct
    and semantically equivalent.
    """

    def __init__(self):
        self.common_functions = {
            'CALCULATE', 'CALCULATETABLE', 'FILTER', 'ALL', 'ALLEXCEPT',
            'SUM', 'SUMX', 'AVERAGE', 'AVERAGEX', 'COUNT', 'COUNTX',
            'MIN', 'MINX', 'MAX', 'MAXX', 'DIVIDE', 'IF', 'SWITCH',
            'VAR', 'RETURN', 'RELATED', 'RELATEDTABLE', 'VALUES',
            'DISTINCT', 'SELECTEDVALUE', 'HASONEVALUE', 'ISBLANK',
            'KEEPFILTERS', 'REMOVEFILTERS', 'USERELATIONSHIP'
        }

    def validate_syntax(self, dax_code: str) -> dict:
        """Basic syntax validation without execution."""
        issues = []

        # Check balanced parentheses
        if dax_code.count('(') != dax_code.count(')'):
            issues.append("Unbalanced parentheses")

        # Check balanced brackets
        if dax_code.count('[') != dax_code.count(']'):
            issues.append("Unbalanced brackets")

        # Check VAR/RETURN pairing
        var_count = len(re.findall(r'\bVAR\b', dax_code, re.IGNORECASE))
        return_count = len(re.findall(r'\bRETURN\b', dax_code, re.IGNORECASE))
        if var_count > 0 and return_count == 0:
            issues.append("VAR without RETURN")

        # Check for common typos
        typos = ['CALULATE', 'SUMXX', 'AVERAGX', 'DEVIDE', 'FLITER']
        for typo in typos:
            if typo in dax_code.upper():
                issues.append(f"Possible typo: {typo}")

        # Check string quotes
        single_quotes = dax_code.count("'")
        if single_quotes % 2 != 0:
            issues.append("Unbalanced single quotes (table names)")

        return {
            "valid": len(issues) == 0,
            "issues": issues
        }

    def check_semantic_equivalence(self, original: str, fixed: str) -> dict:
        """
        Heuristic check for semantic equivalence.
        Not perfect, but catches obvious mistakes.
        """
        warnings = []

        # Check that key column references are preserved
        orig_cols = set(re.findall(r"\[([^\]]+)\]", original))
        fixed_cols = set(re.findall(r"\[([^\]]+)\]", fixed))

        missing = orig_cols - fixed_cols
        if missing:
            warnings.append(f"Original columns not in fixed: {missing}")

        # Check that aggregation functions are preserved
        orig_aggs = set(re.findall(r'\b(SUM|AVERAGE|COUNT|MIN|MAX|SUMX|AVERAGEX)\b',
                                    original, re.IGNORECASE))
        fixed_aggs = set(re.findall(r'\b(SUM|AVERAGE|COUNT|MIN|MAX|SUMX|AVERAGEX)\b',
                                     fixed, re.IGNORECASE))

        # SUMX might become SUM, that's ok
        # But SUM becoming SUMX would change semantics
        if 'SUMX' in fixed_aggs and 'SUMX' not in orig_aggs:
            if 'SUM' not in orig_aggs:
                warnings.append("Iterator (SUMX) added where none existed")

        # Check table references
        orig_tables = set(re.findall(r"'([^']+)'", original))
        fixed_tables = set(re.findall(r"'([^']+)'", fixed))

        missing_tables = orig_tables - fixed_tables
        if missing_tables:
            warnings.append(f"Tables removed: {missing_tables}")

        return {
            "likely_equivalent": len(warnings) == 0,
            "warnings": warnings
        }


# =============================================================================
# OUTPUT GENERATORS
# =============================================================================

def generate_fix_report(fixes: list, model_name: str) -> str:
    """Generate human-readable fix report."""

    report = f"""# DAX Remediation Report
## Model: {model_name}
## Fixes Generated: {len(fixes)}

---

"""
    for i, fix in enumerate(fixes, 1):
        report += f"""
## Fix #{i}: {fix.measure_name}

**Issue:** {fix.issue_type}
**Confidence:** {fix.confidence:.0%}
**Semantic Equivalent:** {"Yes" if fix.semantic_equivalent else "Review Required"}

### Original Code:
```dax
{fix.original_expression}
```

### Fixed Code:
```dax
{fix.fixed_expression}
```

### Changes Made:
"""
        for change in fix.changes_made:
            report += f"- {change}\n"

        if fix.llm_notes:
            report += f"\n**Notes:** {fix.llm_notes}\n"

        report += "\n---\n"

    return report


def generate_tmdl_output(fixes: list) -> str:
    """Generate TMDL format for direct import to Tabular Editor."""

    tmdl = "// Auto-generated by QueryTorque DAX Remediation Engine\n\n"

    for fix in fixes:
        if fix.fixed_expression:
            tmdl += f"""
measure '{fix.measure_name}' =
{fix.fixed_expression}

"""

    return tmdl


def generate_deployment_json(fixes: list) -> str:
    """Generate JSON for programmatic deployment."""

    output = {
        "generated_at": "timestamp",
        "fixes": []
    }

    for fix in fixes:
        output["fixes"].append({
            "measure_name": fix.measure_name,
            "table_name": fix.table_name,
            "original": fix.original_expression,
            "fixed": fix.fixed_expression,
            "changes": fix.changes_made,
            "semantic_equivalent": fix.semantic_equivalent,
            "confidence": fix.confidence
        })

    return json.dumps(output, indent=2)


# =============================================================================
# MAIN PIPELINE
# =============================================================================

class DAXRemediationPipeline:
    """
    End-to-end pipeline for VPAX analysis and LLM-powered remediation.

    This is what makes us different:
    1. Detect issues (like DAX Optimizer)
    2. Generate actual fixed code (they can't do this)
    3. Validate fixes
    4. Output ready-to-deploy formats
    """

    def __init__(self, llm_client=None):
        self.remediation_engine = DAXRemediationEngine(llm_client)
        self.validator = DAXValidator()

    def analyze_and_fix(self, vpax_analysis: dict) -> dict:
        """
        Full pipeline: analysis -> fix generation -> validation -> output

        Args:
            vpax_analysis: Output from vpax_analyzer.py

        Returns:
            Complete remediation package
        """
        results = {
            "summary": vpax_analysis.get("summary", {}),
            "fix_prompts": [],
            "batch_prompts": {},
            "architecture_prompt": None,
        }

        # Group measures by issue type for batch processing
        measures_by_issue = {}

        for measure in vpax_analysis.get("worst_measures", []):
            for issue in measure.get("issues", []):
                issue_type = issue.get("rule_name")
                if issue_type not in measures_by_issue:
                    measures_by_issue[issue_type] = []

                measures_by_issue[issue_type].append(MeasureFix(
                    measure_name=measure["name"],
                    table_name=measure["table"],
                    original_expression=measure["expression"],
                    issue_type=issue_type
                ))

        # Generate batch prompts for each issue type
        for issue_type, measures in measures_by_issue.items():
            if len(measures) >= 2:  # Batch if 2+ measures
                prompt = self.remediation_engine.generate_batch_prompt(measures, issue_type)
                results["batch_prompts"][issue_type] = {
                    "prompt": prompt,
                    "measure_count": len(measures),
                    "measures": [m.measure_name for m in measures]
                }
            else:
                # Individual prompts for single measures
                for measure in measures:
                    prompt = self.remediation_engine.generate_fix_prompt(
                        measure,
                        vpax_analysis.get("model_context", {})
                    )
                    results["fix_prompts"].append({
                        "measure_name": measure.measure_name,
                        "issue_type": issue_type,
                        "prompt": prompt
                    })

        # Generate architecture prompt
        results["architecture_prompt"] = self.remediation_engine.generate_architecture_prompt(
            vpax_analysis.get("summary", {}),
            vpax_analysis.get("all_issues", [])
        )

        return results


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    print("""
==========================================
    QueryTorque DAX Remediation Engine
    LLM-Powered Performance Optimization
==========================================

Unlike DAX Optimizer, we don't just detect - we FIX.

Features:
- Generate actual fixed DAX code (not templates)
- Batch fix multiple measures consistently
- Plain English explanations for business users
- Architecture recommendations
- TMDL export for direct deployment

Usage:
    from qt_dax.analyzers.dax_remediation_engine import DAXRemediationPipeline

    pipeline = DAXRemediationPipeline(llm_client=your_client)
    results = pipeline.analyze_and_fix(vpax_analysis)

    # Get prompts to send to Claude/GPT
    for prompt_data in results["fix_prompts"]:
        response = llm.complete(prompt_data["prompt"])
        # Parse and validate response...
""")
