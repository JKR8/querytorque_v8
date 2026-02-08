"""Case study exporter for qt-dax manual optimization datasets.

Normalizes rows from extracted case files (JSONL/CSV) and emits
ADO-style artifact folders for reproducible review and replay.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from qt_dax.analyzers.measure_dependencies import MeasureDependencyAnalyzer


_MEASURE_DEF_RE = re.compile(r"MEASURE\s+'([^']+)'\[([^\]]+)\]\s*=\s*", re.IGNORECASE)
_SECONDS_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*s\s*$", re.IGNORECASE)


@dataclass
class ParsedMeasure:
    table: str
    name: str
    expression: str


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")
    return slug or "case"


def _parse_seconds(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = _SECONDS_RE.match(text)
    if not match:
        return None
    return float(match.group(1))


def _extract_measures_block(dax_text: str) -> list[ParsedMeasure]:
    if not dax_text:
        return []

    matches = list(_MEASURE_DEF_RE.finditer(dax_text))
    measures: list[ParsedMeasure] = []
    for idx, match in enumerate(matches):
        table = match.group(1)
        name = match.group(2)
        start = match.end()
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(dax_text)
        expression = dax_text[start:end].strip()
        measures.append(ParsedMeasure(table=table, name=name, expression=expression))
    return measures


def _dependency_payload(
    measures: list[ParsedMeasure],
    target_measure_name: str | None,
) -> dict[str, Any]:
    if not measures:
        return {
            "declared_measure_count": 0,
            "target_found": False,
            "closure_count": 0,
            "closure_measures": [],
        }

    analyzer = MeasureDependencyAnalyzer()
    measure_dicts = [
        {"name": m.name, "table": m.table, "expression": m.expression}
        for m in measures
    ]
    result = analyzer.analyze(measure_dicts)

    target_found = False
    closure_names: list[str] = []
    if target_measure_name:
        target_key = target_measure_name.lower()
        if target_key in result.nodes:
            target_found = True
            chain = analyzer.get_dependency_chain(result, target_measure_name)
            key_by_name = {m.name.lower(): m for m in measures}
            for name in chain:
                m = key_by_name.get(name.lower())
                if m is not None:
                    closure_names.append(f"{m.table}[{m.name}]")
                else:
                    closure_names.append(name)

    return {
        "declared_measure_count": len(measures),
        "target_found": target_found,
        "closure_count": len(closure_names),
        "closure_measures": closure_names,
    }


def _string_is_likely_complete(text: str) -> bool:
    if not text:
        return False
    # Simple structural check; catches obvious truncation in extracted helpers.
    return text.count("(") == text.count(")")


def _write_text(path: Path, content: str) -> None:
    path.write_text(content if content else "", encoding="utf-8")


def _case_id(index_1_based: int, measure_name: str | None) -> str:
    base = _slugify(measure_name or f"case_{index_1_based:03d}")
    return f"{index_1_based:03d}_{base}"


def load_case_rows(input_path: str | Path) -> list[dict[str, Any]]:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Case input file not found: {path}")

    if path.suffix.lower() == ".jsonl":
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                rows.append(json.loads(line))
        return rows

    if path.suffix.lower() == ".csv":
        with path.open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    raise ValueError(f"Unsupported case input format: {path.suffix}")


def export_cases(
    input_path: str | Path,
    output_dir: str | Path,
    measure_filters: tuple[str, ...] = (),
    limit: int | None = None,
) -> dict[str, Any]:
    rows = load_case_rows(input_path)
    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    indexed_rows = list(enumerate(rows, start=1))
    selected = indexed_rows
    if measure_filters:
        filter_set = {m.strip().lower() for m in measure_filters if m.strip()}
        selected = [
            pair for pair in indexed_rows
            if str(pair[1].get("measure_name", "")).strip().lower() in filter_set
        ]
    if limit is not None and limit >= 0:
        selected = selected[:limit]

    exported_cases: list[dict[str, Any]] = []
    for index, row in selected:
        measure_name = row.get("measure_name")
        case_id = _case_id(index, measure_name)
        case_dir = out_root / case_id
        case_dir.mkdir(parents=True, exist_ok=True)

        original_dax = row.get("original_dax") or ""
        optimized_dax = row.get("optimized_dax") or ""
        helper1 = row.get("helper1_dax") or ""
        helper2 = row.get("helper2_dax") or ""
        single_query = row.get("single_measure_dax") or ""

        measures = _extract_measures_block(original_dax)
        deps = _dependency_payload(measures, str(measure_name or ""))

        original_seconds = _parse_seconds(row.get("original_time"))
        optimized_seconds = _parse_seconds(row.get("optimized_time"))
        speedup = (
            (original_seconds / optimized_seconds)
            if original_seconds and optimized_seconds and optimized_seconds > 0
            else None
        )

        summary = {
            "schema_version": "v1",
            "case_id": case_id,
            "source_row_index_1_based": index,
            "measure_name": measure_name,
            "original_measure_ref": row.get("original_measure_ref"),
            "optimized_measure_ref": row.get("optimized_measure_ref"),
            "timing": {
                "recorded_original": row.get("original_time"),
                "recorded_optimized": row.get("optimized_time"),
                "original_seconds": original_seconds,
                "optimized_seconds": optimized_seconds,
                "recorded_speedup": speedup,
            },
            "presence": {
                "has_original_dax": bool(original_dax),
                "has_optimized_dax": bool(optimized_dax),
                "has_helper1_dax": bool(helper1),
                "has_helper2_dax": bool(helper2),
                "has_single_measure_query": bool(single_query),
            },
            "quality_flags": {
                "helper1_likely_complete": _string_is_likely_complete(helper1) if helper1 else None,
                "helper2_likely_complete": _string_is_likely_complete(helper2) if helper2 else None,
            },
            "dependency": deps,
        }

        (case_dir / "00_case_summary.json").write_text(
            json.dumps(summary, indent=2),
            encoding="utf-8",
        )
        _write_text(case_dir / "01_original_measure_forest.dax", original_dax)
        _write_text(case_dir / "02_optimized_measure_raw.dax", optimized_dax)
        _write_text(case_dir / "03_helper1_calc_column.dax", helper1)
        _write_text(case_dir / "04_helper2_calc_column.dax", helper2)
        _write_text(case_dir / "05_single_measure_query.dax", single_query)

        validation_template = {
            "status": "TBD",
            "speedup": None,
            "baseline_ms": None,
            "optimized_ms": None,
            "baseline_runs_ms": [],
            "optimized_runs_ms": [],
            "baseline_rows": None,
            "optimized_rows": None,
            "rows_match": None,
            "notes": [
                "Discard first run (warmup) before timing.",
                "Use >=3 timed runs and compare exact row/value equivalence.",
            ],
        }
        (case_dir / "06_validation_template.json").write_text(
            json.dumps(validation_template, indent=2),
            encoding="utf-8",
        )

        repro = (
            "# Repro Steps\n\n"
            "1. Load helper definitions if required by optimized measure.\n"
            "2. Execute original and optimized DAX in identical filter context.\n"
            "3. Record validation outputs in `06_validation_template.json`.\n"
        )
        _write_text(case_dir / "07_repro_steps.md", repro)

        dependency_json = {
            "target_measure": measure_name,
            "declared_measure_count": deps["declared_measure_count"],
            "closure_count": deps["closure_count"],
            "closure_measures": deps["closure_measures"],
        }
        (case_dir / "08_dependency_closure.json").write_text(
            json.dumps(dependency_json, indent=2),
            encoding="utf-8",
        )

        exported_cases.append({
            "case_id": case_id,
            "measure_name": measure_name,
            "path": str(case_dir),
        })

    manifest = {
        "schema_version": "v1",
        "input_path": str(Path(input_path)),
        "output_dir": str(out_root),
        "exported_count": len(exported_cases),
        "total_input_rows": len(rows),
        "cases": exported_cases,
    }
    (out_root / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest
