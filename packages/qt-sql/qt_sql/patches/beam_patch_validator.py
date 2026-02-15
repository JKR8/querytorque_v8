"""Validate beam patch plan responses from LLM.

Parses 4 patch plans from JSON, applies each to IR copy,
validates with gates (parse, columns, semantics, speedup),
and returns validation results with correlation analysis.
"""

import json
import copy
import logging
import re
import time
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass, field
from pathlib import Path

from qt_sql.ir import build_script_ir, dict_to_plan, apply_patch_plan, Dialect
from qt_sql.execution.factory import create_executor_from_dsn
from qt_sql.validation.mini_validator import MiniValidator
from qt_sql.schemas import SemanticValidationResult


logger = logging.getLogger(__name__)


# ── Validation Result Classes ────────────────────────────────────────────────

@dataclass
class PatchValidationGate:
    """Single validation gate (parse, columns, semantics, speedup)."""
    gate_name: str
    passed: bool
    error: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PatchValidationResult:
    """Complete validation result for one patch."""
    patch_id: str  # e.g., "t1_family_a"
    family: str  # A, B, C, D, or E
    transform: str  # e.g., "date_cte_isolate"
    llm_relevance_score: float  # 0.0-1.0 from LLM

    # Validation gates
    gates: List[PatchValidationGate] = field(default_factory=list)

    # Overall outcome
    status: str = "PENDING"  # PASS, FAIL, NEUTRAL
    speedup: Optional[float] = None  # Measured speedup (e.g., 2.15 = 2.15x)
    output_sql: Optional[str] = None

    # Diagnostics
    llm_reasoning: str = ""
    error_messages: List[str] = field(default_factory=list)
    semantic_diffs: Optional[SemanticValidationResult] = None

    # Correlation
    correlation_note: str = ""  # "Score predicts outcome well", "Underestimated", etc.


@dataclass
class BeamValidationReport:
    """Complete validation report for all 4 patches."""
    query_id: str
    dialect: str
    llm_chosen_families: List[str] = field(default_factory=list)
    overall_status: str = "PENDING"  # "4_of_4_pass", "3_of_4_pass", etc.
    patches: List[PatchValidationResult] = field(default_factory=list)

    # Summary
    pass_count: int = 0
    fail_count: int = 0
    neutral_count: int = 0
    avg_speedup: float = 0.0

    # Correlation analysis
    correlation_analysis: str = ""


# ── Gate Validators ──────────────────────────────────────────────────────────

class PatchGateValidator:
    """Validates patches against gates: parse, columns, semantics, speedup."""

    def __init__(self, dialect: str, dsn: str, executor):
        """Initialize with dialect, DSN, and database executor."""
        self.dialect = dialect
        self.dsn = dsn
        self.executor = executor
        # MiniValidator expects (db_path, sample_pct, timeout_ms, dialect)
        self.semantic_validator = MiniValidator(
            db_path=dsn,
            sample_pct=2.0,
            timeout_ms=30_000,
            dialect=dialect,
        )

    def validate_parse(self, output_sql: str) -> PatchValidationGate:
        """Gate 1: Output SQL parses without error."""
        try:
            build_script_ir(output_sql, Dialect[self.dialect.upper()])
            return PatchValidationGate(
                gate_name="PARSE_OK",
                passed=True,
                details={"message": "SQL parses successfully"}
            )
        except Exception as e:
            return PatchValidationGate(
                gate_name="PARSE_OK",
                passed=False,
                error=f"Parse error: {str(e)}"
            )

    def validate_columns(self, original_sql: str, output_sql: str) -> PatchValidationGate:
        """Gate 2: Output columns match original exactly."""
        try:
            ir_orig = build_script_ir(original_sql, Dialect[self.dialect.upper()])
            ir_out = build_script_ir(output_sql, Dialect[self.dialect.upper()])

            # Extract column names (simplified - assumes SELECT top-level)
            orig_cols = _extract_output_columns(ir_orig)
            out_cols = _extract_output_columns(ir_out)

            if orig_cols == out_cols:
                return PatchValidationGate(
                    gate_name="COLUMN_COMPLETENESS",
                    passed=True,
                    details={"columns": orig_cols}
                )
            else:
                return PatchValidationGate(
                    gate_name="COLUMN_COMPLETENESS",
                    passed=False,
                    error=f"Column mismatch: original={orig_cols}, output={out_cols}",
                    details={"original": orig_cols, "output": out_cols}
                )
        except Exception as e:
            return PatchValidationGate(
                gate_name="COLUMN_COMPLETENESS",
                passed=False,
                error=f"Column extraction failed: {str(e)}"
            )

    def validate_semantics(self, original_sql: str, output_sql: str, worker_id: int = 0) -> PatchValidationGate:
        """Gate 3: Semantics preserved (row count + sample values match)."""
        try:
            result = self.semantic_validator.validate_rewrite(
                original_sql, output_sql, worker_id=worker_id
            )

            if result.passed:
                return PatchValidationGate(
                    gate_name="SEMANTIC_MATCH",
                    passed=True,
                    details={
                        "tier_passed": result.tier_passed,
                        "row_count_match": True
                    }
                )
            else:
                error_msg = "; ".join(result.errors[:3])  # First 3 errors
                return PatchValidationGate(
                    gate_name="SEMANTIC_MATCH",
                    passed=False,
                    error=error_msg,
                    details={"errors": result.errors}
                )
        except Exception as e:
            return PatchValidationGate(
                gate_name="SEMANTIC_MATCH",
                passed=False,
                error=f"Semantic validation failed: {str(e)}"
            )

    def validate_speedup(
        self, original_sql: str, output_sql: str, min_speedup: float = 1.1
    ) -> Tuple[PatchValidationGate, Optional[float]]:
        """Gate 4: Speedup measured and >= min_speedup (default 1.1x)."""
        try:
            # Benchmark 3 runs: warmup + 2 measured
            orig_times = self._benchmark(original_sql, runs=3)
            out_times = self._benchmark(output_sql, runs=3)

            # Average of last 2 runs (discard warmup)
            orig_avg = sum(orig_times[1:]) / len(orig_times[1:])
            out_avg = sum(out_times[1:]) / len(out_times[1:])

            speedup = orig_avg / out_avg if out_avg > 0 else 1.0

            if speedup >= min_speedup:
                return (
                    PatchValidationGate(
                        gate_name="SPEEDUP_ACHIEVED",
                        passed=True,
                        details={
                            "original_ms": round(orig_avg, 2),
                            "output_ms": round(out_avg, 2),
                            "speedup": round(speedup, 2),
                            "threshold": min_speedup
                        }
                    ),
                    speedup
                )
            else:
                return (
                    PatchValidationGate(
                        gate_name="SPEEDUP_ACHIEVED",
                        passed=False,
                        error=f"Speedup {speedup:.2f}x below threshold {min_speedup}x",
                        details={
                            "original_ms": round(orig_avg, 2),
                            "output_ms": round(out_avg, 2),
                            "speedup": round(speedup, 2)
                        }
                    ),
                    speedup
                )
        except Exception as e:
            return (
                PatchValidationGate(
                    gate_name="SPEEDUP_ACHIEVED",
                    passed=False,
                    error=f"Speedup measurement failed: {str(e)}"
                ),
                None
            )

    def _benchmark(self, sql: str, runs: int = 3) -> List[float]:
        """Benchmark SQL execution time in milliseconds.

        Uses executor.execute() with perf_counter timing,
        same pattern as QueryBenchmarker._execute_timed().
        """
        times = []
        for _ in range(runs):
            try:
                start = time.perf_counter()
                self.executor.execute(sql)
                elapsed_ms = (time.perf_counter() - start) * 1000
                times.append(elapsed_ms)
            except Exception as e:
                logger.warning(f"Benchmark run failed: {e}")
                times.append(float('inf'))
        return times


# ── Main Validator ───────────────────────────────────────────────────────────

class BeamPatchValidator:
    """Orchestrates validation of 4 patch plans from beam LLM response."""

    def __init__(self, dsn: str, dialect: str):
        """Initialize with database connection and dialect."""
        self.dsn = dsn
        self.dialect = dialect
        self.executor = create_executor_from_dsn(dsn)
        self.gate_validator = PatchGateValidator(dialect, dsn, self.executor)

    def validate_response(
        self,
        query_id: str,
        original_sql: str,
        original_ir: Any,  # ScriptIR
        llm_response: str,  # JSON string with 4 patches
    ) -> BeamValidationReport:
        """Validate 4 patches from LLM response.

        Args:
            query_id: Query identifier (e.g., "query_21")
            original_sql: Original query SQL
            original_ir: Parsed IR of original query
            llm_response: Raw JSON response from LLM with 4 patches

        Returns:
            BeamValidationReport with all validation results
        """

        # Parse JSON response — LLM may include analysis text after JSON
        patches_data = _extract_json_array(llm_response)
        if patches_data is None:
            logger.error("Failed to extract JSON array from LLM response")
            return BeamValidationReport(
                query_id=query_id,
                dialect=self.dialect,
                overall_status="PARSE_ERROR",
            )

        n_patches = len(patches_data)
        if n_patches < 4:
            logger.warning(f"LLM returned {n_patches} patches (expected 4)")
        if n_patches > 4:
            logger.warning(f"LLM returned {n_patches} patches, truncating to 4")
            patches_data = patches_data[:4]

        # Validate each patch
        results = []
        for i, patch_data in enumerate(patches_data):
            if not isinstance(patch_data, dict):
                logger.warning(f"Patch {i}: expected dict, got {type(patch_data).__name__}")
                continue
            result = self._validate_single_patch(
                patch_data=patch_data,
                original_sql=original_sql,
                original_ir=original_ir,
                patch_index=i
            )
            results.append(result)

        # Build report
        report = self._build_report(query_id, results)
        return report

    def _validate_single_patch(
        self,
        patch_data: Dict[str, Any],
        original_sql: str,
        original_ir: Any,
        patch_index: int
    ) -> PatchValidationResult:
        """Validate a single patch plan.

        Process:
        1. Deep copy original IR
        2. Apply patch plan
        3. Render output SQL
        4. Run gates: parse, columns, semantics, speedup
        5. Return result
        """

        try:
            # Extract metadata
            patch_id = patch_data.get("plan_id", f"patch_{patch_index}")
            family = patch_data.get("family", "?")
            transform = patch_data.get("transform", "unknown")
            try:
                relevance_score = float(patch_data.get("relevance_score", 0.0))
            except (TypeError, ValueError):
                relevance_score = 0.0
            reasoning = patch_data.get("reasoning", "")
        except Exception as e:
            return PatchValidationResult(
                patch_id=f"patch_{patch_index}",
                family="?",
                transform="unknown",
                llm_relevance_score=0.0,
                status="FAIL",
                error_messages=[f"Metadata extraction failed: {e}"],
            )

        result = PatchValidationResult(
            patch_id=patch_id,
            family=family,
            transform=transform,
            llm_relevance_score=relevance_score,
            llm_reasoning=reasoning
        )

        try:
            # Deep copy original IR
            ir_copy = copy.deepcopy(original_ir)

            # Apply patch plan
            patch_plan = dict_to_plan(patch_data)
            patch_result = apply_patch_plan(ir_copy, patch_plan)

            if not patch_result.success:
                result.status = "FAIL"
                result.error_messages = patch_result.errors
                result.gates.append(PatchValidationGate(
                    gate_name="PATCH_APPLICATION",
                    passed=False,
                    error=f"Patch failed: {'; '.join(patch_result.errors[:2])}"
                ))
                return result

            output_sql = patch_result.output_sql
            result.output_sql = output_sql

            # Gate 1: Parse
            gate_parse = self.gate_validator.validate_parse(output_sql)
            result.gates.append(gate_parse)
            if not gate_parse.passed:
                result.status = "FAIL"
                result.error_messages.append(gate_parse.error)
                return result

            # Gate 2: Columns
            gate_cols = self.gate_validator.validate_columns(original_sql, output_sql)
            result.gates.append(gate_cols)
            if not gate_cols.passed:
                result.status = "FAIL"
                result.error_messages.append(gate_cols.error)
                return result

            # Gate 3: Semantics
            gate_sem = self.gate_validator.validate_semantics(
                original_sql, output_sql, worker_id=patch_index
            )
            result.gates.append(gate_sem)
            if not gate_sem.passed:
                result.status = "NEUTRAL"
                result.error_messages.append(gate_sem.error)
                return result

            # Gate 4: Speedup
            gate_speed, speedup = self.gate_validator.validate_speedup(
                original_sql, output_sql, min_speedup=1.1
            )
            result.gates.append(gate_speed)
            result.speedup = speedup

            if gate_speed.passed:
                result.status = "PASS"
                result.correlation_note = self._analyze_correlation(relevance_score, speedup)
            else:
                result.status = "NEUTRAL"
                result.correlation_note = self._analyze_correlation(relevance_score, speedup)

            return result

        except Exception as e:
            result.status = "FAIL"
            result.error_messages.append(f"Unexpected error: {str(e)}")
            logger.exception(f"Patch {patch_id} validation failed")
            return result

    def _analyze_correlation(self, relevance_score: float, speedup: Optional[float]) -> str:
        """Analyze correlation between LLM relevance score and actual speedup."""
        if speedup is None:
            return "Speedup not measured"

        if speedup >= 1.1:
            if relevance_score >= 0.8:
                return "✓ Score predicts success (high score, high speedup)"
            elif relevance_score >= 0.5:
                return "✓ Conservative estimate (mid score, good speedup)"
            else:
                return "⚠ Underestimated (low score, unexpected speedup)"
        else:
            if relevance_score >= 0.8:
                return "⚠ Overestimated (high score, no speedup)"
            elif relevance_score >= 0.5:
                return "≈ Neutral outcome (mid score, no speedup)"
            else:
                return "✓ Correctly rejected (low score, no speedup)"

    def _build_report(self, query_id: str, results: List[PatchValidationResult]) -> BeamValidationReport:
        """Build summary report from all patch results."""
        report = BeamValidationReport(
            query_id=query_id,
            dialect=self.dialect,
            llm_chosen_families=[r.family for r in results],
            patches=results
        )

        # Count outcomes
        for result in results:
            if result.status == "PASS":
                report.pass_count += 1
            elif result.status == "FAIL":
                report.fail_count += 1
            else:  # NEUTRAL
                report.neutral_count += 1

        # Overall status — use actual count, not hardcoded 4
        total = len(results)
        report.overall_status = f"{report.pass_count}_of_{total}_pass"

        # Average speedup (only for PASS patches)
        pass_speedups = [r.speedup for r in results if r.status == "PASS" and r.speedup]
        if pass_speedups:
            report.avg_speedup = sum(pass_speedups) / len(pass_speedups)

        # Correlation analysis
        report.correlation_analysis = self._build_correlation_summary(results)

        return report

    def _build_correlation_summary(self, results: List[PatchValidationResult]) -> str:
        """Build summary of LLM relevance score vs actual outcome correlation."""
        lines = ["## Correlation Analysis (LLM Score vs Actual Outcome)"]
        lines.append("")

        for r in results:
            status_emoji = "✅" if r.status == "PASS" else "⚠️" if r.status == "NEUTRAL" else "❌"
            speedup_str = f"{r.speedup:.2f}x" if r.speedup else "N/A"
            lines.append(f"{status_emoji} {r.family}: LLM {r.llm_relevance_score:.2f} → {speedup_str} ({r.status})")
            lines.append(f"   {r.correlation_note}")

        return "\n".join(lines)


# ── Helper Functions ─────────────────────────────────────────────────────────

def _extract_json_array(text: str) -> Optional[List[Dict[str, Any]]]:
    """Extract a JSON array from LLM response text.

    The LLM may wrap JSON in ```json fences or include analysis text
    after the JSON. This function finds the outermost [...] array and
    parses it, ignoring surrounding prose.

    Returns:
        Parsed list of dicts, or None on failure.
    """
    # Strip markdown code fences if present
    text = text.strip()

    # Try direct parse first (fast path)
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    # Remove ```json ... ``` fences
    fenced = re.search(r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", text)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass

    # Find the first '[' and its matching ']' using bracket counting
    start = text.find('[')
    if start == -1:
        return None

    depth = 0
    in_string = False
    escape = False
    for i in range(start, len(text)):
        c = text[i]
        if escape:
            escape = False
            continue
        if c == '\\' and in_string:
            escape = True
            continue
        if c == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                candidate = text[start:i + 1]
                try:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, list):
                        return parsed
                except json.JSONDecodeError:
                    pass
                break

    return None


def _extract_output_columns(ir) -> List[str]:
    """Extract output column names from IR.

    Walks the first SELECT statement's select_list and extracts
    column names/aliases. Falls back to sql_text for complex expressions.
    """
    try:
        from sqlglot import exp

        for stmt in ir.statements:
            if stmt.query and stmt.query.select_list:
                columns = []
                for sel in stmt.query.select_list:
                    node = getattr(sel, '_ast_node', None)
                    if node is not None:
                        if isinstance(node, exp.Alias):
                            columns.append(node.alias.lower())
                        elif isinstance(node, exp.Column):
                            columns.append(node.name.lower())
                        elif isinstance(node, exp.Star):
                            columns.append("*")
                        else:
                            # Use the sql_text as fallback
                            columns.append(sel.sql_text.strip().lower())
                    else:
                        columns.append(sel.sql_text.strip().lower())
                return columns
    except Exception:
        pass
    return ["*"]


def save_validation_report(report: BeamValidationReport, output_path: Path):
    """Save validation report to JSON file."""
    data = {
        "query_id": report.query_id,
        "dialect": report.dialect,
        "timestamp": str(Path(output_path).parent.name),
        "overall_status": report.overall_status,
        "llm_chosen_families": report.llm_chosen_families,
        "summary": {
            "pass_count": report.pass_count,
            "fail_count": report.fail_count,
            "neutral_count": report.neutral_count,
            "avg_speedup": round(report.avg_speedup, 2)
        },
        "patches": [
            {
                "patch_id": p.patch_id,
                "family": p.family,
                "transform": p.transform,
                "llm_relevance_score": round(p.llm_relevance_score, 2),
                "status": p.status,
                "speedup": round(p.speedup, 2) if p.speedup else None,
                "gates": [
                    {
                        "gate": g.gate_name,
                        "passed": g.passed,
                        "error": g.error,
                        "details": g.details
                    }
                    for g in p.gates
                ],
                "errors": p.error_messages[:3],
                "correlation": p.correlation_note
            }
            for p in report.patches
        ],
        "correlation_analysis": report.correlation_analysis
    }

    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)

    logger.info(f"Validation report saved to {output_path}")
