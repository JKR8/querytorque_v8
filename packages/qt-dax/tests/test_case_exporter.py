"""Tests for case study exporter."""

import json
from pathlib import Path

from qt_dax.case_exporter import export_cases


def test_export_cases_jsonl_to_artifacts(tmp_path):
    input_path = tmp_path / "cases.jsonl"
    output_dir = tmp_path / "out"

    row = {
        "measure_name": "Portfolio_Asset_Matrix MV CR Intensity Switch_BM",
        "original_measure_ref": "ESG Trucost Climate[Portfolio_Asset_Matrix MV CR Intensity Switch_BM]",
        "optimized_measure_ref": "Portfolio_Asset_Matrix MV CR Intensity Switch_BM New",
        "original_time": "60s",
        "optimized_time": "0.4s",
        "original_dax": (
            "DEFINE\n"
            "MEASURE 'ESG Trucost Climate'[Base A] = SUM('T'[X])\n"
            "MEASURE 'ESG Trucost Climate'[Portfolio_Asset_Matrix MV CR Intensity Switch_BM] = [Base A]\n"
        ),
        "optimized_dax": "Portfolio_Asset_Matrix MV CR Intensity Switch_BM New = [Base A]",
        "helper1_dax": "Helper1 = 1",
        "helper2_dax": "Helper2 = (1 + 2",
        "single_measure_dax": "EVALUATE ROW(\"Result\", 1)",
    }
    input_path.write_text(json.dumps(row) + "\n", encoding="utf-8")

    manifest = export_cases(input_path=input_path, output_dir=output_dir)

    assert manifest["exported_count"] == 1
    case_dir = Path(manifest["cases"][0]["path"])
    assert case_dir.exists()

    summary = json.loads((case_dir / "00_case_summary.json").read_text(encoding="utf-8"))
    assert summary["measure_name"] == row["measure_name"]
    assert summary["timing"]["original_seconds"] == 60.0
    assert summary["timing"]["optimized_seconds"] == 0.4
    assert summary["timing"]["recorded_speedup"] == 150.0
    assert summary["dependency"]["target_found"] is True
    assert summary["dependency"]["closure_count"] >= 1
    assert summary["quality_flags"]["helper2_likely_complete"] is False

    assert (case_dir / "01_original_measure_forest.dax").exists()
    assert (case_dir / "02_optimized_measure_raw.dax").exists()
    assert (case_dir / "06_validation_template.json").exists()
    assert (output_dir / "manifest.json").exists()


def test_export_cases_filter_by_measure(tmp_path):
    input_path = tmp_path / "cases.jsonl"
    output_dir = tmp_path / "out"
    rows = [
        {"measure_name": "A", "original_dax": "", "optimized_dax": ""},
        {"measure_name": "B", "original_dax": "", "optimized_dax": ""},
    ]
    input_path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    manifest = export_cases(
        input_path=input_path,
        output_dir=output_dir,
        measure_filters=("B",),
    )

    assert manifest["exported_count"] == 1
    assert manifest["cases"][0]["measure_name"] == "B"

