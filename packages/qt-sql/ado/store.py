"""Store artifacts and update indexes for ADO."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class StoredArtifact:
    prompt_path: str
    response_path: str
    optimized_sql_path: str
    validation_path: str


class Store:
    """Persist artifacts and structured summaries."""

    def __init__(self, run_dir: Path):
        self.run_dir = run_dir
        self.run_dir.mkdir(parents=True, exist_ok=True)

    def save_candidate(self, query_id: str, worker_id: int, prompt: str, response: str, optimized_sql: str, validation: dict[str, Any]) -> StoredArtifact:
        qdir = self.run_dir / query_id / f"worker_{worker_id:02d}"
        qdir.mkdir(parents=True, exist_ok=True)

        prompt_path = qdir / "prompt.txt"
        response_path = qdir / "response.txt"
        sql_path = qdir / "optimized.sql"
        validation_path = qdir / "validation.json"

        prompt_path.write_text(prompt)
        response_path.write_text(response)
        sql_path.write_text(optimized_sql)
        validation_path.write_text(json.dumps(validation, indent=2))

        return StoredArtifact(
            prompt_path=str(prompt_path),
            response_path=str(response_path),
            optimized_sql_path=str(sql_path),
            validation_path=str(validation_path),
        )
