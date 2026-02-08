#!/usr/bin/env python3
"""ADO Swarm Pipeline Smoke Test — Full Health Check

Runs the complete swarm pipeline on a single TPC-DS query, decomposed
into 15 numbered steps (01–15).  Each step:
  - Saves input/output artifacts to a numbered directory
  - Validates structural contracts (type, shape, invariants)
  - Tags checks against code-review findings (F1–F7)

This is the gate before committing to the full TPC-DS benchmark.

Usage:
    cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.smoke_test
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.smoke_test --query query_67

Code-review findings under test:
    F1  Full prompt+response persistence (every iteration)
    F2  Swarm fan-out: all 4 worker IDs must be distinct
    F4  Structured validation diagnostics (errors as list, error_category set)
    F5  Structured logging (logger, not bare print)
    F6  DRY: worker strategy header via shared build_worker_strategy_header()
    F7  No dead code (api_calls count correct, no unused timing vars)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Bootstrap ────────────────────────────────────────────────────────
PROJECT_ROOT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
os.chdir(PROJECT_ROOT)
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    if p not in sys.path:
        sys.path.insert(0, p)

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")


# ── Data structures ──────────────────────────────────────────────────

@dataclass
class Check:
    """Single contract assertion."""
    name: str
    passed: bool
    detail: str = ""
    finding: str = ""       # e.g. "F2"


@dataclass
class StepResult:
    """Result of one pipeline step."""
    step: int
    name: str
    title: str
    elapsed_s: float
    checks: List[Check] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and all(c.passed for c in self.checks)

    @property
    def summary(self) -> str:
        n = len(self.checks)
        p = sum(1 for c in self.checks if c.passed)
        tag = "PASS" if self.ok else "FAIL"
        return f"[{tag}] Step {self.step:02d} {self.title}  ({p}/{n} checks, {self.elapsed_s:.1f}s)"


# ── Helpers ──────────────────────────────────────────────────────────

def _jsave(path: Path, data: Any) -> None:
    """JSON-serialize to file, handling arbitrary objects."""
    def _default(o):
        if hasattr(o, "to_dict"):
            return o.to_dict()
        if hasattr(o, "__dict__"):
            return {k: v for k, v in o.__dict__.items() if not k.startswith("_")}
        return str(o)
    path.write_text(json.dumps(data, indent=2, default=_default))


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


# =====================================================================
# SmokeTest
# =====================================================================

class SmokeTest:
    """Full pipeline smoke test with per-step contract validation."""

    BENCHMARK_DIR = Path("packages/qt-sql/ado/benchmarks/duckdb_tpcds")
    DIALECT = "duckdb"
    ENGINE = "duckdb"

    def __init__(self, query_id: str = "query_42"):
        self.query_id = query_id
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.out = self.BENCHMARK_DIR / f"smoke_test_{ts}"
        self.out.mkdir(parents=True, exist_ok=True)

        # Accumulated state
        self.pipeline = None
        self.generator = None
        self.sql: str = ""
        self.dag = None
        self.costs: Dict = {}
        self.explain_result = None
        self.faiss_examples: List[Dict] = []
        self.regression_warnings: List[Dict] = []
        self.all_available: List[Dict] = []
        self.fan_out_prompt: str = ""
        self.analyst_response: str = ""
        self.assignments: list = []
        self.worker_prompts: Dict[int, str] = {}
        self.candidates: Dict[int, Any] = {}       # wid -> Candidate
        self.final_sqls: Dict[int, str] = {}        # wid -> post-syntax-check SQL
        self.syntax_valid: Dict[int, bool] = {}
        self.baseline = None
        self.val_results: Dict[int, Any] = {}       # wid -> (status, speedup, errors, cat)
        self.learning_records: list = []

        self.results: List[StepResult] = []
        self._setup_logging()

    # ── Logging ──────────────────────────────────────────────────────

    def _setup_logging(self):
        log_path = self.out / "smoke_test.log"
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
            datefmt="%H:%M:%S",
            handlers=[logging.StreamHandler(), logging.FileHandler(log_path)],
            force=True,
        )
        self.log = logging.getLogger("smoke_test")

    # ── Step directory + check helpers ───────────────────────────────

    def _sdir(self, num: int, name: str) -> Path:
        d = self.out / f"{num:02d}_{name}"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _chk(self, name: str, cond: bool, detail: str = "", finding: str = "") -> Check:
        return Check(name=name, passed=cond, detail=detail, finding=finding)

    # =================================================================
    # STEPS
    # =================================================================

    def step_01_config(self) -> StepResult:
        """Load and validate BenchmarkConfig."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(1, "config")

        from ado.pipeline import Pipeline
        self.pipeline = Pipeline(
            str(self.BENCHMARK_DIR),
            provider="deepseek",
            model="deepseek-reasoner",
        )
        cfg = self.pipeline.config

        _jsave(sdir / "output.json", {
            "engine": cfg.engine,
            "benchmark": cfg.benchmark,
            "db_path_or_dsn": cfg.db_path_or_dsn,
            "scale_factor": cfg.scale_factor,
            "timeout_seconds": cfg.timeout_seconds,
            "validation_method": cfg.validation_method,
            "n_queries": cfg.n_queries,
            "workers_state_0": cfg.workers_state_0,
            "promote_threshold": cfg.promote_threshold,
        })

        checks.append(self._chk("engine_duckdb", cfg.engine == "duckdb", cfg.engine))
        checks.append(self._chk("db_exists", Path(cfg.db_path_or_dsn).exists(), cfg.db_path_or_dsn))
        checks.append(self._chk("timeout_positive", cfg.timeout_seconds > 0, str(cfg.timeout_seconds)))
        checks.append(self._chk("validation_method_valid",
                                 cfg.validation_method in ("3-run", "5-run"), cfg.validation_method))
        checks.append(self._chk("scale_factor_10", cfg.scale_factor == 10, str(cfg.scale_factor)))

        # F5: Pipeline should have logger, not bare print
        from ado.pipeline import logger as pipeline_logger
        checks.append(self._chk("pipeline_has_logger", pipeline_logger is not None,
                                 type(pipeline_logger).__name__, finding="F5"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=1, name="config", title="Load Configuration",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_02_query(self) -> StepResult:
        """Load and validate query SQL."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(2, "query")

        _jsave(sdir / "input.json", {"query_id": self.query_id})

        self.sql = self.pipeline.load_query(self.query_id)
        (sdir / "output.sql").write_text(self.sql)

        checks.append(self._chk("sql_non_empty", bool(self.sql and self.sql.strip())))
        checks.append(self._chk("sql_has_select", "SELECT" in self.sql.upper() if self.sql else False))

        # Parse with sqlglot
        parse_ok = False
        try:
            import sqlglot
            sqlglot.parse_one(self.sql, dialect=self.DIALECT)
            parse_ok = True
        except Exception as e:
            checks.append(self._chk("sql_parses", False, str(e)))
        if parse_ok:
            checks.append(self._chk("sql_parses", True))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=2, name="query", title="Load Query SQL",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_03_dag(self) -> StepResult:
        """Parse SQL into DAG + EXPLAIN costs."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(3, "dag")

        (sdir / "input.sql").write_text(self.sql)

        self.dag, self.costs, self.explain_result = self.pipeline._parse_dag(
            self.sql, dialect=self.DIALECT, query_id=self.query_id,
        )

        # dag.nodes is a dict {node_id: DagNode}
        node_ids = list(self.dag.nodes.keys()) if isinstance(self.dag.nodes, dict) else [n.node_id for n in self.dag.nodes]

        _jsave(sdir / "output.json", {
            "n_nodes": len(self.dag.nodes),
            "n_edges": len(self.dag.edges),
            "node_ids": node_ids,
            "cost_keys": list(self.costs.keys()) if isinstance(self.costs, dict) else "non-dict",
            "has_explain": self.explain_result is not None,
        })

        checks.append(self._chk("dag_has_nodes", len(self.dag.nodes) > 0, str(len(self.dag.nodes))))
        checks.append(self._chk("costs_non_empty",
                                 bool(self.costs) if isinstance(self.costs, dict) else hasattr(self.costs, "__len__")))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=3, name="dag", title="Parse DAG + EXPLAIN",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_04_faiss(self) -> StepResult:
        """FAISS gold example retrieval + full catalog."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(4, "faiss")

        self.faiss_examples = self.pipeline._find_examples(self.sql, engine=self.ENGINE, k=12)
        self.all_available = self.pipeline._list_gold_examples(self.ENGINE)

        _jsave(sdir / "output.json", {
            "faiss_count": len(self.faiss_examples),
            "faiss_ids": [e.get("id", "?") for e in self.faiss_examples],
            "catalog_count": len(self.all_available),
            "catalog_ids": [e.get("id", "?") for e in self.all_available],
        })

        checks.append(self._chk("faiss_returns_examples", len(self.faiss_examples) > 0,
                                 str(len(self.faiss_examples))))
        checks.append(self._chk("faiss_max_12", len(self.faiss_examples) <= 12))

        ids = [e.get("id") for e in self.faiss_examples]
        checks.append(self._chk("faiss_ids_unique", len(ids) == len(set(ids)), str(ids)))

        for ex in self.faiss_examples:
            has_id = bool(ex.get("id"))
            if not has_id:
                checks.append(self._chk("example_has_id", False, str(ex.keys())))
                break
        else:
            checks.append(self._chk("all_examples_have_id", True))

        checks.append(self._chk("catalog_non_empty", len(self.all_available) > 0))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=4, name="faiss", title="FAISS Example Retrieval",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_05_regressions(self) -> StepResult:
        """Regression anti-pattern retrieval."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(5, "regressions")

        self.regression_warnings = self.pipeline._find_regression_warnings(
            self.sql, engine=self.ENGINE, k=2,
        )

        _jsave(sdir / "output.json", {
            "count": len(self.regression_warnings),
            "ids": [r.get("id", "?") for r in self.regression_warnings],
        })

        checks.append(self._chk("regressions_is_list", isinstance(self.regression_warnings, list)))

        # No overlap with gold examples
        gold_ids = {e.get("id") for e in self.faiss_examples}
        reg_ids = {r.get("id") for r in self.regression_warnings}
        overlap = gold_ids & reg_ids
        checks.append(self._chk("no_gold_regression_overlap", len(overlap) == 0,
                                 str(overlap) if overlap else ""))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=5, name="regressions", title="Regression Warnings",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_06_fan_out_prompt(self) -> StepResult:
        """Build analyst fan-out prompt."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(6, "fan_out_prompt")

        from ado.prompts import build_fan_out_prompt
        self.fan_out_prompt = build_fan_out_prompt(
            query_id=self.query_id,
            sql=self.sql,
            dag=self.dag,
            costs=self.costs,
            faiss_examples=self.faiss_examples,
            all_available_examples=self.all_available,
            dialect=self.DIALECT,
        )

        (sdir / "output.txt").write_text(self.fan_out_prompt)
        _jsave(sdir / "meta.json", {"chars": len(self.fan_out_prompt), "sha256": _sha256(self.fan_out_prompt)})

        checks.append(self._chk("prompt_non_empty", len(self.fan_out_prompt) > 0))
        checks.append(self._chk("prompt_substantial", len(self.fan_out_prompt) > 1000,
                                 f"{len(self.fan_out_prompt)} chars"))
        checks.append(self._chk("prompt_contains_sql", "SELECT" in self.fan_out_prompt.upper()))
        checks.append(self._chk("prompt_mentions_workers",
                                 any(w in self.fan_out_prompt.upper() for w in ["WORKER", "SPECIALIST"])))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=6, name="fan_out_prompt", title="Build Fan-out Prompt",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_07_analyst_call(self) -> StepResult:
        """LLM analyst call: distribute strategies to 4 workers."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(7, "analyst_call")

        (sdir / "input_prompt.txt").write_text(self.fan_out_prompt)

        from ado.generate import CandidateGenerator
        self.generator = CandidateGenerator(provider="deepseek", model="deepseek-reasoner")

        self.analyst_response = self.generator._analyze(self.fan_out_prompt)

        (sdir / "output_response.txt").write_text(self.analyst_response)
        _jsave(sdir / "meta.json", {
            "elapsed_s": round(time.time() - t0, 1),
            "response_chars": len(self.analyst_response),
        })

        checks.append(self._chk("response_non_empty", len(self.analyst_response) > 0))
        checks.append(self._chk("response_substantial", len(self.analyst_response) > 100,
                                 f"{len(self.analyst_response)} chars"))
        checks.append(self._chk("response_has_worker_refs",
                                 "WORKER" in self.analyst_response.upper(),
                                 "should contain WORKER_N blocks"))

        # F1: analyst prompt + response both persisted
        checks.append(self._chk("analyst_prompt_saved",
                                 (sdir / "input_prompt.txt").exists() and
                                 (sdir / "input_prompt.txt").stat().st_size > 0,
                                 finding="F1"))
        checks.append(self._chk("analyst_response_saved",
                                 (sdir / "output_response.txt").exists() and
                                 (sdir / "output_response.txt").stat().st_size > 0,
                                 finding="F1"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=7, name="analyst_call", title="Analyst LLM Call",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_08_parse_assignments(self) -> StepResult:
        """Parse analyst response into 4 worker assignments."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(8, "parse_assignments")

        from ado.prompts import parse_fan_out_response
        self.assignments = parse_fan_out_response(self.analyst_response)

        _jsave(sdir / "output.json", [
            {"worker_id": a.worker_id, "strategy": a.strategy,
             "examples": a.examples, "hint": a.hint}
            for a in self.assignments
        ])

        # F2: exactly 4 workers, all IDs distinct
        checks.append(self._chk("exactly_4_assignments", len(self.assignments) == 4,
                                 f"got {len(self.assignments)}", finding="F2"))
        wids = [a.worker_id for a in self.assignments]
        checks.append(self._chk("worker_ids_unique", len(set(wids)) == len(wids),
                                 f"ids={wids}", finding="F2"))
        checks.append(self._chk("worker_ids_are_1_to_4", sorted(wids) == [1, 2, 3, 4],
                                 f"ids={sorted(wids)}", finding="F2"))

        for a in self.assignments:
            checks.append(self._chk(f"w{a.worker_id}_has_strategy", bool(a.strategy),
                                     a.strategy or "(empty)"))
            checks.append(self._chk(f"w{a.worker_id}_has_hint", bool(a.hint),
                                     (a.hint or "(empty)")[:60]))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=8, name="parse_assignments", title="Parse Worker Assignments",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_09_worker_prompts(self) -> StepResult:
        """Build 4 specialized worker prompts."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(9, "worker_prompts")

        global_learnings = self.pipeline.learner.build_learning_summary() or None

        from ado.prompts import build_worker_strategy_header

        for a in self.assignments:
            examples = self.pipeline._load_examples_by_id(a.examples, self.ENGINE)

            base_prompt = self.pipeline.prompter.build_prompt(
                query_id=f"{self.query_id}_w{a.worker_id}",
                full_sql=self.sql,
                dag=self.dag,
                costs=self.costs,
                history=None,
                examples=examples,
                expert_analysis=None,
                global_learnings=global_learnings,
                regression_warnings=self.regression_warnings,
                dialect=self.DIALECT,
                semantic_intents=self.pipeline.get_semantic_intents(self.query_id),
                engine_version=self.pipeline._engine_version,
            )

            # F6: Use the shared header builder
            header = build_worker_strategy_header(a.strategy, a.hint)
            self.worker_prompts[a.worker_id] = header + base_prompt

            (sdir / f"worker_{a.worker_id}_prompt.txt").write_text(self.worker_prompts[a.worker_id])

        checks.append(self._chk("four_prompts_built", len(self.worker_prompts) == 4,
                                 f"got {len(self.worker_prompts)}"))

        for wid, prompt in self.worker_prompts.items():
            checks.append(self._chk(f"w{wid}_prompt_substantial",
                                     len(prompt) > 1000, f"{len(prompt)} chars"))
            checks.append(self._chk(f"w{wid}_contains_sql", "SELECT" in prompt.upper()))

        # F6: verify header format via shared builder
        for a in self.assignments:
            prompt_text = self.worker_prompts[a.worker_id]
            checks.append(self._chk(f"w{a.worker_id}_header_has_strategy_title",
                                     f"## Optimization Strategy: {a.strategy}" in prompt_text,
                                     finding="F6"))
            checks.append(self._chk(f"w{a.worker_id}_header_has_approach",
                                     "**Your approach**:" in prompt_text,
                                     finding="F6"))
            checks.append(self._chk(f"w{a.worker_id}_header_has_focus",
                                     "**Focus**:" in prompt_text,
                                     finding="F6"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=9, name="worker_prompts", title="Build Worker Prompts",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_10_generate(self) -> StepResult:
        """4 parallel LLM worker calls → optimized SQL candidates."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(10, "generate")

        def _gen_one(wid: int):
            a = [x for x in self.assignments if x.worker_id == wid][0]
            prompt = self.worker_prompts[wid]
            candidate = self.generator.generate_one(
                sql=self.sql,
                prompt=prompt,
                examples_used=a.examples,
                worker_id=wid,
                dialect=self.DIALECT,
            )
            return wid, candidate

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = {pool.submit(_gen_one, wid): wid for wid in self.worker_prompts}
            for fut in as_completed(futures):
                wid = futures[fut]
                try:
                    wid, cand = fut.result()
                    self.candidates[wid] = cand
                except Exception as e:
                    self.log.error(f"Worker {wid} generation failed: {e}")
                    self.candidates[wid] = None

        # Save artifacts per worker
        for wid in sorted(self.candidates):
            wdir = sdir / f"worker_{wid}"
            wdir.mkdir(parents=True, exist_ok=True)
            cand = self.candidates[wid]
            if cand is None:
                (wdir / "error.txt").write_text("generation failed")
                continue
            (wdir / "prompt.txt").write_text(cand.prompt)
            (wdir / "response.txt").write_text(cand.response)
            (wdir / "optimized.sql").write_text(cand.optimized_sql)
            _jsave(wdir / "candidate.json", {
                "worker_id": cand.worker_id,
                "transforms": cand.transforms,
                "examples_used": cand.examples_used,
                "error": cand.error,
                "optimized_sql_sha256": _sha256(cand.optimized_sql),
                "original_sql_sha256": _sha256(self.sql),
                "sql_changed": cand.optimized_sql.strip() != self.sql.strip(),
            })

        _jsave(sdir / "meta.json", {
            "elapsed_s": round(time.time() - t0, 1),
            "n_candidates": len(self.candidates),
        })

        checks.append(self._chk("four_candidates", len(self.candidates) == 4,
                                 f"got {len(self.candidates)}"))

        n_with_response = sum(1 for c in self.candidates.values()
                              if c is not None and c.response)
        checks.append(self._chk("all_have_response", n_with_response == 4,
                                 f"{n_with_response}/4"))

        n_changed = sum(1 for c in self.candidates.values()
                        if c is not None and c.optimized_sql.strip() != self.sql.strip())
        checks.append(self._chk("at_least_one_changed", n_changed >= 1,
                                 f"{n_changed}/4 differ from original"))

        # F1: every worker's prompt and response saved to disk
        for wid in sorted(self.candidates):
            wdir = sdir / f"worker_{wid}"
            checks.append(self._chk(f"w{wid}_prompt_persisted",
                                     (wdir / "prompt.txt").exists() and
                                     (wdir / "prompt.txt").stat().st_size > 0,
                                     finding="F1"))
            checks.append(self._chk(f"w{wid}_response_persisted",
                                     (wdir / "response.txt").exists() and
                                     (wdir / "response.txt").stat().st_size > 0,
                                     finding="F1"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=10, name="generate", title="Worker LLM Generation",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_11_syntax(self) -> StepResult:
        """Syntax-check each candidate with sqlglot; revert invalid to original."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(11, "syntax")

        import sqlglot

        for wid in sorted(self.candidates):
            cand = self.candidates[wid]
            if cand is None or not cand.optimized_sql:
                self.final_sqls[wid] = self.sql
                self.syntax_valid[wid] = False
                continue
            try:
                sqlglot.parse_one(cand.optimized_sql, dialect=self.DIALECT)
                self.final_sqls[wid] = cand.optimized_sql
                self.syntax_valid[wid] = True
            except Exception:
                self.final_sqls[wid] = self.sql
                self.syntax_valid[wid] = False

        results = {}
        for wid in sorted(self.final_sqls):
            results[f"worker_{wid}"] = {
                "valid": self.syntax_valid[wid],
                "sql_sha256": _sha256(self.final_sqls[wid]),
                "reverted_to_original": not self.syntax_valid[wid],
            }
        _jsave(sdir / "output.json", results)

        n_valid = sum(1 for v in self.syntax_valid.values() if v)
        checks.append(self._chk("at_least_one_valid", n_valid >= 1, f"{n_valid}/4 valid"))

        # If invalid, verify it was reverted to original
        for wid in sorted(self.final_sqls):
            if not self.syntax_valid[wid]:
                checks.append(self._chk(f"w{wid}_invalid_reverted",
                                         self.final_sqls[wid].strip() == self.sql.strip(),
                                         "invalid SQL should revert to original"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=11, name="syntax", title="Syntax Validation",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_12_baseline(self) -> StepResult:
        """Benchmark original SQL (3-run warmup pattern)."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(12, "baseline")

        (sdir / "input.sql").write_text(self.sql)

        from ado.validate import Validator
        self._validator = Validator(sample_db=self.pipeline.config.db_path_or_dsn)
        self.baseline = self._validator.benchmark_baseline(self.sql)

        _jsave(sdir / "output.json", {
            "measured_time_ms": self.baseline.measured_time_ms,
            "row_count": self.baseline.row_count,
            "has_checksum": self.baseline.checksum is not None,
            "has_rows": self.baseline.rows is not None,
        })

        checks.append(self._chk("time_positive", self.baseline.measured_time_ms > 0,
                                 f"{self.baseline.measured_time_ms:.1f}ms"))
        checks.append(self._chk("rows_positive", self.baseline.row_count > 0,
                                 str(self.baseline.row_count)))
        checks.append(self._chk("checksum_present", self.baseline.checksum is not None,
                                 str(self.baseline.checksum)))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=12, name="baseline", title="Benchmark Original (Baseline)",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_13_validate(self) -> StepResult:
        """Validate all candidates against baseline (timing + semantic check)."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(13, "validate")

        for wid in sorted(self.final_sqls):
            sql = self.final_sqls[wid]
            vr = self._validator.validate_against_baseline(
                baseline=self.baseline,
                candidate_sql=sql,
                worker_id=wid,
            )
            self.val_results[wid] = vr

            _jsave(sdir / f"worker_{wid}.json", {
                "worker_id": vr.worker_id,
                "status": vr.status.value,
                "speedup": vr.speedup,
                "error": vr.error,
                "errors": vr.errors,
                "error_category": vr.error_category,
                "optimized_sql_sha256": _sha256(vr.optimized_sql),
            })

        self._validator.close()

        # Summary
        summary = {}
        for wid, vr in sorted(self.val_results.items()):
            summary[f"worker_{wid}"] = {
                "status": vr.status.value,
                "speedup": round(vr.speedup, 4),
            }
        _jsave(sdir / "summary.json", summary)

        checks.append(self._chk("all_validated", len(self.val_results) == 4,
                                 f"{len(self.val_results)}/4"))

        for wid, vr in sorted(self.val_results.items()):
            checks.append(self._chk(f"w{wid}_has_status",
                                     vr.status.value in ("pass", "fail", "error"),
                                     vr.status.value))
            checks.append(self._chk(f"w{wid}_speedup_is_float",
                                     isinstance(vr.speedup, (int, float)),
                                     str(type(vr.speedup).__name__)))

            # F4: errors must be a list, not a flattened string
            checks.append(self._chk(f"w{wid}_errors_is_list",
                                     isinstance(vr.errors, list),
                                     f"type={type(vr.errors).__name__}",
                                     finding="F4"))

            # F4: if errors present, error_category must be set
            if vr.errors:
                checks.append(self._chk(f"w{wid}_error_category_set",
                                         vr.error_category is not None,
                                         f"category={vr.error_category}",
                                         finding="F4"))

        # At least one should pass
        n_pass = sum(1 for vr in self.val_results.values() if vr.status.value == "pass")
        checks.append(self._chk("at_least_one_passes", n_pass >= 1, f"{n_pass}/4 pass"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=13, name="validate", title="Validate Candidates",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_14_learning(self) -> StepResult:
        """Create and persist learning records for each worker."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(14, "learning")

        for wid, vr in sorted(self.val_results.items()):
            cand = self.candidates.get(wid)
            a = [x for x in self.assignments if x.worker_id == wid]
            examples_used = a[0].examples if a else []
            transforms_used = cand.transforms if cand else []

            status_str = "pass" if vr.status.value == "pass" else "error"

            lr = self.pipeline.learner.create_learning_record(
                query_id=self.query_id,
                examples_recommended=examples_used,
                transforms_recommended=examples_used,
                status=status_str,
                speedup=vr.speedup,
                transforms_used=transforms_used,
                worker_id=wid,
                error_category=vr.error_category,
                error_messages=vr.errors,
            )
            self.pipeline.learner.save_learning_record(lr)
            self.learning_records.append(lr)

        _jsave(sdir / "records.json", [asdict(lr) for lr in self.learning_records])

        checks.append(self._chk("four_records_created", len(self.learning_records) == 4,
                                 f"got {len(self.learning_records)}"))

        for lr in self.learning_records:
            checks.append(self._chk(f"lr_w{lr.worker_id}_has_timestamp", bool(lr.timestamp)))
            checks.append(self._chk(f"lr_w{lr.worker_id}_has_query_id",
                                     lr.query_id == self.query_id))
            checks.append(self._chk(f"lr_w{lr.worker_id}_transforms_is_list",
                                     isinstance(lr.transforms_used, list),
                                     f"type={type(lr.transforms_used).__name__}"))
            # F4: error_messages persisted as list
            checks.append(self._chk(f"lr_w{lr.worker_id}_error_msgs_is_list",
                                     isinstance(lr.error_messages, list),
                                     f"type={type(lr.error_messages).__name__}",
                                     finding="F4"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=14, name="learning", title="Learning Records",
                          elapsed_s=time.time() - t0, checks=checks)

    def step_15_session_save(self) -> StepResult:
        """Save full session artifacts + audit trail completeness check."""
        t0 = time.time()
        checks = []
        sdir = self._sdir(15, "session_save")

        # Build iteration data the same way SwarmSession does
        from ado.schemas import WorkerResult

        worker_results = []
        worker_prompts_map = {}  # wid -> (prompt, response)
        for wid in sorted(self.candidates):
            cand = self.candidates[wid]
            vr = self.val_results.get(wid)
            a = [x for x in self.assignments if x.worker_id == wid]
            if not a or cand is None or vr is None:
                continue

            wr = WorkerResult(
                worker_id=wid,
                strategy=a[0].strategy,
                examples_used=a[0].examples,
                optimized_sql=self.final_sqls[wid],
                speedup=vr.speedup,
                status=self._classify(vr),
                transforms=cand.transforms if cand else [],
                hint=a[0].hint,
                error_message=vr.error or "",
            )
            worker_results.append(wr)
            worker_prompts_map[wid] = (cand.prompt, cand.response)

        iteration_data = {
            "iteration": 0,
            "phase": "fan_out",
            "analyst_prompt": self.fan_out_prompt,
            "analyst_response": self.analyst_response,
            "worker_prompts": worker_prompts_map,
            "worker_results": [wr.to_dict() for wr in worker_results],
            "best_speedup": max((wr.speedup for wr in worker_results), default=0.0),
        }

        # Save using the same layout SwarmSession.save_session() uses
        session_dir = sdir / "session_artifacts"
        session_dir.mkdir(parents=True, exist_ok=True)

        best_wr = max(worker_results, key=lambda w: w.speedup) if worker_results else None

        session_meta = {
            "query_id": self.query_id,
            "mode": "swarm",
            "target_speedup": 2.0,
            "max_iterations": 1,
            "n_iterations": 1,
            "best_speedup": best_wr.speedup if best_wr else 0.0,
            "best_worker_id": best_wr.worker_id if best_wr else None,
            "best_strategy": best_wr.strategy if best_wr else None,
            "total_workers": len(worker_results),
        }
        _jsave(session_dir / "session.json", session_meta)

        it_dir = session_dir / "iteration_00_fan_out"
        it_dir.mkdir(parents=True, exist_ok=True)
        (it_dir / "analyst_prompt.txt").write_text(self.fan_out_prompt)
        (it_dir / "analyst_response.txt").write_text(self.analyst_response)

        for wr in worker_results:
            w_dir = it_dir / f"worker_{wr.worker_id:02d}"
            w_dir.mkdir(parents=True, exist_ok=True)
            _jsave(w_dir / "result.json", wr.to_dict())
            (w_dir / "optimized.sql").write_text(wr.optimized_sql)
            if wr.worker_id in worker_prompts_map:
                wp, wresp = worker_prompts_map[wr.worker_id]
                if wp:
                    (w_dir / "prompt.txt").write_text(wp)
                if wresp:
                    (w_dir / "response.txt").write_text(wresp)

        # ── Audit trail checks ──

        checks.append(self._chk("session_json_exists",
                                 (session_dir / "session.json").exists()))
        checks.append(self._chk("iteration_dir_exists", it_dir.exists()))

        # F1: analyst prompt + response saved
        checks.append(self._chk("analyst_prompt_in_session",
                                 (it_dir / "analyst_prompt.txt").exists() and
                                 (it_dir / "analyst_prompt.txt").stat().st_size > 0,
                                 finding="F1"))
        checks.append(self._chk("analyst_response_in_session",
                                 (it_dir / "analyst_response.txt").exists() and
                                 (it_dir / "analyst_response.txt").stat().st_size > 0,
                                 finding="F1"))

        # F2: all 4 worker dirs exist
        for wid in range(1, 5):
            w_dir = it_dir / f"worker_{wid:02d}"
            checks.append(self._chk(f"worker_{wid:02d}_dir_exists", w_dir.exists(),
                                     finding="F2"))

        # F1: each worker has prompt + response + result + optimized sql
        for wid in range(1, 5):
            w_dir = it_dir / f"worker_{wid:02d}"
            if not w_dir.exists():
                continue
            for fname in ["result.json", "optimized.sql", "prompt.txt", "response.txt"]:
                exists = (w_dir / fname).exists() and (w_dir / fname).stat().st_size > 0
                checks.append(self._chk(f"worker_{wid:02d}_{fname.replace('.','_')}_saved",
                                         exists, finding="F1"))

        # F4: result.json has structured error data
        for wid in range(1, 5):
            result_path = it_dir / f"worker_{wid:02d}" / "result.json"
            if result_path.exists():
                data = json.loads(result_path.read_text())
                # error_message should be a string (flattened for WorkerResult.to_dict)
                # but the raw validation had structured errors — verify session captured something
                checks.append(self._chk(f"worker_{wid:02d}_result_has_status",
                                         "status" in data, finding="F4"))
                checks.append(self._chk(f"worker_{wid:02d}_result_has_speedup",
                                         "speedup" in data, finding="F4"))

        # F7: api_calls count sanity
        # Fan-out: 1 analyst + 4 workers = 5 API calls
        expected_api_calls = 1 + len(worker_results)
        checks.append(self._chk("api_call_count_correct",
                                 expected_api_calls == 5,
                                 f"expected 5, got {expected_api_calls}",
                                 finding="F7"))

        _jsave(sdir / "contract.json", [asdict(c) for c in checks])
        return StepResult(step=15, name="session_save", title="Session Save + Audit",
                          elapsed_s=time.time() - t0, checks=checks)

    # ── Classification helper ────────────────────────────────────────

    @staticmethod
    def _classify(vr) -> str:
        if vr.status.value == "error":
            return "ERROR"
        if vr.status.value == "fail":
            return "FAIL"
        if vr.speedup >= 1.10:
            return "WIN"
        if vr.speedup >= 1.05:
            return "IMPROVED"
        if vr.speedup >= 0.95:
            return "NEUTRAL"
        return "REGRESSION"

    # =================================================================
    # RUNNER
    # =================================================================

    def run(self) -> List[StepResult]:
        self.log.info(f"{'='*60}")
        self.log.info(f"  ADO SWARM PIPELINE SMOKE TEST")
        self.log.info(f"  Query: {self.query_id}")
        self.log.info(f"  Output: {self.out}")
        self.log.info(f"{'='*60}")

        t_total = time.time()

        steps = [
            self.step_01_config,
            self.step_02_query,
            self.step_03_dag,
            self.step_04_faiss,
            self.step_05_regressions,
            self.step_06_fan_out_prompt,
            self.step_07_analyst_call,
            self.step_08_parse_assignments,
            self.step_09_worker_prompts,
            self.step_10_generate,
            self.step_11_syntax,
            self.step_12_baseline,
            self.step_13_validate,
            self.step_14_learning,
            self.step_15_session_save,
        ]

        for step_fn in steps:
            t0 = time.time()
            try:
                result = step_fn()
            except Exception as e:
                import traceback
                result = StepResult(
                    step=len(self.results) + 1,
                    name=step_fn.__name__.replace("step_", ""),
                    title=step_fn.__doc__.split("\n")[0] if step_fn.__doc__ else "?",
                    elapsed_s=time.time() - t0,
                    error=f"{type(e).__name__}: {e}",
                )
                # Save traceback
                err_dir = self._sdir(result.step, result.name)
                (err_dir / "traceback.txt").write_text(traceback.format_exc())

            self.results.append(result)
            self.log.info(f"  {result.summary}")

            # Save progress
            _jsave(self.out / "progress.json", {
                "query_id": self.query_id,
                "steps_completed": len(self.results),
                "steps_total": len(steps),
                "last_step": result.name,
                "last_ok": result.ok,
                "elapsed_s": round(time.time() - t_total, 1),
            })

            if result.error:
                self.log.error(f"    FATAL: {result.error}")
                self.log.error(f"    Stopping — cannot continue without step {result.step}")
                break

        total_elapsed = time.time() - t_total
        report_path = self._write_report(total_elapsed)

        self.log.info(f"\n{'='*60}")
        all_ok = all(r.ok for r in self.results)
        total_checks = sum(len(r.checks) for r in self.results)
        total_passed = sum(sum(1 for c in r.checks if c.passed) for r in self.results)
        status = "PASS" if all_ok else "FAIL"
        self.log.info(f"  RESULT: {status}  ({total_passed}/{total_checks} checks, "
                       f"{len(self.results)}/{len(steps)} steps, {total_elapsed:.1f}s)")
        self.log.info(f"  Report: {report_path}")
        self.log.info(f"{'='*60}\n")

        return self.results

    # =================================================================
    # REPORT
    # =================================================================

    def _write_report(self, total_elapsed: float) -> Path:
        report_path = self.out / "REPORT.md"

        total_checks = sum(len(r.checks) for r in self.results)
        total_passed = sum(sum(1 for c in r.checks if c.passed) for r in self.results)
        all_ok = all(r.ok for r in self.results)

        lines = [
            "# ADO Swarm Pipeline Smoke Test Report",
            "",
            f"**Query**: `{self.query_id}`",
            f"**Engine**: DuckDB SF10",
            f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**Duration**: {total_elapsed:.1f}s",
            f"**Status**: {'PASS' if all_ok else 'FAIL'} ({total_passed}/{total_checks} checks)",
            "",
            "## Step Summary",
            "",
            "| Step | Name | Time | Checks | Status |",
            "|-----:|------|-----:|-------:|:------:|",
        ]

        for r in self.results:
            p = sum(1 for c in r.checks if c.passed)
            n = len(r.checks)
            tag = "PASS" if r.ok else "**FAIL**"
            lines.append(f"| {r.step:02d} | {r.title} | {r.elapsed_s:.1f}s | {p}/{n} | {tag} |")

        # Finding coverage
        finding_checks: Dict[str, List[Check]] = {}
        for r in self.results:
            for c in r.checks:
                if c.finding:
                    finding_checks.setdefault(c.finding, []).append(c)

        if finding_checks:
            lines.extend(["", "## Code Review Finding Coverage", "",
                          "| Finding | Description | Checks | Status |",
                          "|---------|-------------|-------:|:------:|"])
            descriptions = {
                "F1": "Full prompt+response persistence",
                "F2": "Unique worker IDs (no overwrites)",
                "F4": "Structured validation diagnostics",
                "F5": "Structured logging (logger exists)",
                "F6": "DRY worker strategy header",
                "F7": "API call count correctness",
            }
            for f_id in sorted(finding_checks):
                cs = finding_checks[f_id]
                p = sum(1 for c in cs if c.passed)
                n = len(cs)
                tag = "PASS" if p == n else "**FAIL**"
                desc = descriptions.get(f_id, "")
                lines.append(f"| {f_id} | {desc} | {p}/{n} | {tag} |")

        # Best result
        if self.val_results:
            best_wid = max(self.val_results, key=lambda w: self.val_results[w].speedup)
            best_vr = self.val_results[best_wid]
            best_a = [a for a in self.assignments if a.worker_id == best_wid]
            strategy = best_a[0].strategy if best_a else "?"
            cand = self.candidates.get(best_wid)
            transforms = cand.transforms if cand else []

            lines.extend([
                "",
                "## Best Result",
                "",
                f"- **Worker**: {best_wid} ({strategy})",
                f"- **Status**: {self._classify(best_vr)}",
                f"- **Speedup**: {best_vr.speedup:.2f}x",
                f"- **Transforms**: {', '.join(transforms) if transforms else '(none)'}",
                f"- **Baseline**: {self.baseline.measured_time_ms:.1f}ms" if self.baseline else "",
            ])

        # Per-step detail
        lines.extend(["", "## Step Details", ""])
        for r in self.results:
            lines.append(f"### Step {r.step:02d}: {r.title}")
            if r.error:
                lines.append(f"\n**ERROR**: `{r.error}`\n")
            if r.checks:
                lines.append("")
                lines.append("| Check | Result | Finding | Detail |")
                lines.append("|-------|:------:|:-------:|--------|")
                for c in r.checks:
                    tag = "PASS" if c.passed else "FAIL"
                    detail = c.detail[:80] if c.detail else ""
                    lines.append(f"| {c.name} | {tag} | {c.finding} | {detail} |")
            lines.append("")

        # Artifact inventory
        lines.extend(["## Artifact Inventory", ""])
        for d in sorted(self.out.iterdir()):
            if d.is_dir() and d.name[0].isdigit():
                files = sorted(f.name for f in d.rglob("*") if f.is_file())
                lines.append(f"- `{d.name}/` ({len(files)} files)")
                for f in files[:10]:
                    lines.append(f"  - `{f}`")
                if len(files) > 10:
                    lines.append(f"  - ... and {len(files) - 10} more")

        lines.append("")
        report_path.write_text("\n".join(lines))
        return report_path


# =====================================================================
# CLI
# =====================================================================

def main():
    parser = argparse.ArgumentParser(description="ADO Swarm Pipeline Smoke Test")
    parser.add_argument("--query", default="query_42",
                        help="Query ID to smoke test (default: query_42)")
    args = parser.parse_args()

    smoke = SmokeTest(query_id=args.query)
    results = smoke.run()

    # Exit code: 0 if all pass, 1 if any fail
    sys.exit(0 if all(r.ok for r in results) else 1)


if __name__ == "__main__":
    main()
