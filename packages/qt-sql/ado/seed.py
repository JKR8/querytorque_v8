"""Seed folder management — consolidated initialization for ADO benchmarks.

Three main classes:
- SeedLoader: Load queries, explains, intents, config from seed/
- SeedBuilder: Migrate scattered files into seed/ structure (one-time)
- SeedValidator: Validate seed readiness via checklist

Seed structure:
    benchmarks/<name>/seed/
    ├── manifest.yaml
    ├── queries/*.sql
    ├── explains/*.json
    ├── intents/*.json
    ├── catalog_rules/*.json
    └── config.json
"""

from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schemas import BenchmarkConfig, ChecklistItem, SeedManifest

logger = logging.getLogger(__name__)

# Optional YAML support — fall back to JSON if PyYAML not installed
try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


def _load_manifest(manifest_path: Path) -> Dict[str, Any]:
    """Load manifest from YAML or JSON."""
    if not manifest_path.exists():
        return {}
    text = manifest_path.read_text()
    if HAS_YAML and manifest_path.suffix in (".yaml", ".yml"):
        return yaml.safe_load(text) or {}
    return json.loads(text)


def _save_manifest(manifest_path: Path, data: Dict[str, Any]) -> None:
    """Save manifest as YAML (preferred) or JSON."""
    if HAS_YAML and manifest_path.suffix in (".yaml", ".yml"):
        manifest_path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    else:
        manifest_path.write_text(json.dumps(data, indent=2))


class SeedLoader:
    """Load all initialization data from a seed folder."""

    def __init__(self, seed_dir: Path):
        self.seed_dir = Path(seed_dir)
        self._manifest: Optional[SeedManifest] = None
        self._config: Optional[BenchmarkConfig] = None

    @property
    def manifest(self) -> SeedManifest:
        if self._manifest is None:
            self._manifest = self._load_manifest()
        return self._manifest

    @property
    def config(self) -> BenchmarkConfig:
        if self._config is None:
            self._config = self._load_config()
        return self._config

    def _load_manifest(self) -> SeedManifest:
        """Load manifest.yaml from seed directory."""
        for ext in (".yaml", ".yml", ".json"):
            path = self.seed_dir / f"manifest{ext}"
            if path.exists():
                data = _load_manifest(path)
                return SeedManifest.from_dict(data)
        # Return empty manifest if not found
        return SeedManifest(name="", engine="")

    def _load_config(self) -> BenchmarkConfig:
        """Load config.json from seed directory."""
        config_path = self.seed_dir / "config.json"
        if config_path.exists():
            return BenchmarkConfig.from_file(config_path)
        # Fallback: check parent directory
        parent_config = self.seed_dir.parent / "config.json"
        if parent_config.exists():
            return BenchmarkConfig.from_file(parent_config)
        raise FileNotFoundError(f"No config.json found in {self.seed_dir} or parent")

    def load_queries(self, query_ids: Optional[List[str]] = None) -> Dict[str, str]:
        """Load SQL queries from seed/queries/."""
        queries_dir = self.seed_dir / "queries"
        if not queries_dir.exists():
            return {}
        queries = {}
        for path in sorted(queries_dir.glob("*.sql")):
            qid = path.stem
            if query_ids and qid not in query_ids:
                continue
            queries[qid] = path.read_text()
        return queries

    def load_explain(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Load EXPLAIN plan for a query."""
        path = self.seed_dir / "explains" / f"{query_id}.json"
        if path.exists():
            return json.loads(path.read_text())
        return None

    def load_intent(self, query_id: str) -> Optional[Dict[str, Any]]:
        """Load semantic intent for a query."""
        path = self.seed_dir / "intents" / f"{query_id}.json"
        if path.exists():
            return json.loads(path.read_text())
        return None

    def load_all_intents(self) -> Dict[str, Dict[str, Any]]:
        """Load all semantic intents, keyed by query_id."""
        intents_dir = self.seed_dir / "intents"
        if not intents_dir.exists():
            return {}
        result = {}
        for path in sorted(intents_dir.glob("*.json")):
            try:
                data = json.loads(path.read_text())
                qid = data.get("query_id", path.stem)
                result[qid] = data
            except Exception:
                continue
        return result

    def load_catalog_rules(self) -> List[Dict[str, Any]]:
        """Load catalog rules from seed/catalog_rules/."""
        rules_dir = self.seed_dir / "catalog_rules"
        if not rules_dir.exists():
            return []
        rules = []
        for path in sorted(rules_dir.glob("*.json")):
            try:
                rules.append(json.loads(path.read_text()))
            except Exception:
                continue
        return rules

    def explains_dir(self) -> Path:
        """Return path to explains directory."""
        return self.seed_dir / "explains"

    def queries_dir(self) -> Path:
        """Return path to queries directory."""
        return self.seed_dir / "queries"


class SeedBuilder:
    """Migrate scattered benchmark files into the seed/ structure.

    One-time migration from the old layout:
        benchmarks/<name>/queries/         → seed/queries/
        benchmarks/<name>/explains/        → seed/explains/
        benchmarks/<name>/semantic_intents.json → seed/intents/ (split per query)
        benchmarks/<name>/state_0/seed/    → seed/catalog_rules/
        benchmarks/<name>/config.json      → seed/config.json
    """

    def __init__(self, benchmark_dir: Path):
        self.benchmark_dir = Path(benchmark_dir)
        self.seed_dir = self.benchmark_dir / "seed"

    def build(self, force: bool = False) -> Path:
        """Build seed directory from scattered files.

        Args:
            force: If True, overwrite existing seed directory.

        Returns:
            Path to created seed directory.
        """
        if self.seed_dir.exists() and not force:
            logger.info(f"Seed directory already exists: {self.seed_dir}")
            return self.seed_dir

        self.seed_dir.mkdir(parents=True, exist_ok=True)
        for subdir in ("queries", "explains", "intents", "catalog_rules"):
            (self.seed_dir / subdir).mkdir(exist_ok=True)

        self._migrate_queries()
        self._migrate_explains()
        self._migrate_intents()
        self._migrate_catalog_rules()
        self._migrate_config()
        self._create_manifest()

        logger.info(f"Built seed directory: {self.seed_dir}")
        return self.seed_dir

    def _migrate_queries(self) -> int:
        """Copy queries/*.sql → seed/queries/."""
        src = self.benchmark_dir / "queries"
        dst = self.seed_dir / "queries"
        count = 0
        if src.exists():
            for path in sorted(src.glob("*.sql")):
                shutil.copy2(path, dst / path.name)
                count += 1
        logger.info(f"Migrated {count} queries")
        return count

    def _migrate_explains(self) -> int:
        """Copy explains/*.json → seed/explains/."""
        src = self.benchmark_dir / "explains"
        dst = self.seed_dir / "explains"
        count = 0
        if src.exists():
            for path in sorted(src.glob("*.json")):
                shutil.copy2(path, dst / path.name)
                count += 1
        logger.info(f"Migrated {count} explain plans")
        return count

    def _migrate_intents(self) -> int:
        """Split semantic_intents.json → seed/intents/<query_id>.json."""
        src = self.benchmark_dir / "semantic_intents.json"
        dst = self.seed_dir / "intents"
        count = 0
        if src.exists():
            try:
                data = json.loads(src.read_text())
                for q in data.get("queries", []):
                    qid = q.get("query_id", "")
                    if qid:
                        (dst / f"{qid}.json").write_text(
                            json.dumps(q, indent=2)
                        )
                        count += 1
            except Exception as e:
                logger.warning(f"Failed to split intents: {e}")
        logger.info(f"Migrated {count} semantic intents")
        return count

    def _migrate_catalog_rules(self) -> int:
        """Copy state_0/seed/*.json → seed/catalog_rules/."""
        src = self.benchmark_dir / "state_0" / "seed"
        dst = self.seed_dir / "catalog_rules"
        count = 0
        if src.exists():
            for path in sorted(src.glob("*.json")):
                shutil.copy2(path, dst / path.name)
                count += 1
        logger.info(f"Migrated {count} catalog rules")
        return count

    def _migrate_config(self) -> None:
        """Copy config.json → seed/config.json."""
        src = self.benchmark_dir / "config.json"
        if src.exists():
            shutil.copy2(src, self.seed_dir / "config.json")
            logger.info("Migrated config.json")

    def _create_manifest(self) -> None:
        """Create initial manifest.yaml."""
        config_path = self.seed_dir / "config.json"
        config_data = {}
        if config_path.exists():
            config_data = json.loads(config_path.read_text())

        queries_count = len(list((self.seed_dir / "queries").glob("*.sql")))
        explains_count = len(list((self.seed_dir / "explains").glob("*.json")))
        intents_count = len(list((self.seed_dir / "intents").glob("*.json")))
        rules_count = len(list((self.seed_dir / "catalog_rules").glob("*.json")))

        manifest = SeedManifest(
            name=config_data.get("benchmark", self.benchmark_dir.name),
            engine=config_data.get("engine", ""),
            scale_factor=config_data.get("scale_factor", 10),
            created=datetime.now().isoformat(),
            queries_loaded=ChecklistItem(
                status="pass" if queries_count > 0 else "pending",
                count=queries_count,
            ),
            explains_gathered=ChecklistItem(
                status="pass" if explains_count > 0 else "pending",
                count=explains_count,
            ),
            intents_attached=ChecklistItem(
                status="pass" if intents_count > 0 else "pending",
                count=intents_count,
            ),
            catalog_rules_loaded=ChecklistItem(
                status="pass" if rules_count > 0 else "pending",
                count=rules_count,
            ),
            validation_method=config_data.get("validation_method", "3-run"),
            timeout_seconds=config_data.get("timeout_seconds", 300),
        )

        manifest_data = manifest.to_dict()
        manifest_data["checklist"]["db_connection"]["dsn"] = (
            config_data.get("db_path") or config_data.get("dsn", "")
        )

        manifest_path = self.seed_dir / "manifest.yaml"
        _save_manifest(manifest_path, manifest_data)
        logger.info(f"Created manifest: {manifest_path}")


class SeedValidator:
    """Validate seed readiness via checklist."""

    def __init__(self, seed_dir: Path):
        self.seed_dir = Path(seed_dir)
        self.manifest_path = self.seed_dir / "manifest.yaml"

    def validate_all(self, update_manifest: bool = True) -> SeedManifest:
        """Run all checklist validations and return updated manifest.

        1. Test DB connection
        2. Count queries
        3. Parse each SQL with sqlglot
        4. Check EXPLAIN coverage
        5. Check intents coverage
        6. Count catalog rules
        7. Check FAISS index

        Args:
            update_manifest: If True, write updated manifest.yaml back to disk.

        Returns:
            Updated SeedManifest with all checklist items validated.
        """
        manifest_data = _load_manifest(self.manifest_path)
        manifest = SeedManifest.from_dict(manifest_data) if manifest_data else SeedManifest(
            name=self.seed_dir.parent.name, engine=""
        )

        # 1. DB connection
        manifest.db_connection = self._check_db_connection(manifest)

        # 2. Queries loaded
        manifest.queries_loaded = self._check_queries()

        # 3. Queries parseable
        manifest.queries_parseable = self._check_parseable(manifest.engine)

        # 4. Explains coverage
        manifest.explains_gathered = self._check_explains()

        # 5. Intents coverage
        manifest.intents_attached = self._check_intents()

        # 6. Catalog rules
        manifest.catalog_rules_loaded = self._check_catalog_rules()

        # 7. FAISS index
        manifest.faiss_index_ready = self._check_faiss()

        if update_manifest:
            # Preserve DSN from existing manifest
            manifest_out = manifest.to_dict()
            dsn = manifest_data.get("checklist", {}).get("db_connection", {}).get("dsn", "")
            if dsn:
                manifest_out["checklist"]["db_connection"]["dsn"] = dsn
            _save_manifest(self.manifest_path, manifest_out)

        self._print_checklist(manifest)
        return manifest

    def _check_db_connection(self, manifest: SeedManifest) -> ChecklistItem:
        """Test database connection."""
        config_path = self.seed_dir / "config.json"
        if not config_path.exists():
            return ChecklistItem(status="fail", errors=["config.json not found"])

        try:
            config = BenchmarkConfig.from_file(config_path)
            dsn = config.db_path_or_dsn
            if not dsn:
                return ChecklistItem(status="fail", errors=["No DSN/db_path in config"])

            if config.engine == "duckdb":
                import duckdb
                conn = duckdb.connect(dsn, read_only=True)
                conn.execute("SELECT 1")
                conn.close()
            elif config.engine in ("postgresql", "postgres"):
                import psycopg2
                conn = psycopg2.connect(dsn)
                conn.close()

            return ChecklistItem(status="pass", extra={"dsn": dsn})
        except Exception as e:
            return ChecklistItem(status="fail", errors=[str(e)])

    def _check_queries(self) -> ChecklistItem:
        """Count SQL queries in seed/queries/."""
        queries_dir = self.seed_dir / "queries"
        if not queries_dir.exists():
            return ChecklistItem(status="fail", errors=["queries/ not found"])
        files = list(queries_dir.glob("*.sql"))
        status = "pass" if files else "fail"
        return ChecklistItem(status=status, count=len(files))

    def _check_parseable(self, engine: str) -> ChecklistItem:
        """Parse each SQL file with sqlglot."""
        queries_dir = self.seed_dir / "queries"
        if not queries_dir.exists():
            return ChecklistItem(status="fail")

        dialect = "postgres" if engine in ("postgresql", "postgres") else engine
        errors = []
        count = 0

        try:
            import sqlglot
        except ImportError:
            return ChecklistItem(status="fail", errors=["sqlglot not installed"])

        for path in sorted(queries_dir.glob("*.sql")):
            try:
                sqlglot.parse_one(path.read_text(), dialect=dialect)
                count += 1
            except Exception as e:
                errors.append(f"{path.stem}: {str(e)[:100]}")

        status = "pass" if not errors else ("fail" if count == 0 else "pass")
        return ChecklistItem(status=status, count=count, errors=errors[:20])

    def _check_explains(self) -> ChecklistItem:
        """Check EXPLAIN coverage against queries."""
        explains_dir = self.seed_dir / "explains"
        queries_dir = self.seed_dir / "queries"
        if not explains_dir.exists():
            return ChecklistItem(status="fail", errors=["explains/ not found"])

        explain_ids = {p.stem for p in explains_dir.glob("*.json")}
        query_ids = {p.stem for p in queries_dir.glob("*.sql")} if queries_dir.exists() else set()
        missing = sorted(query_ids - explain_ids)

        count = len(explain_ids)
        status = "pass" if not missing else "pass"  # explains are optional
        return ChecklistItem(status=status, count=count, missing=missing[:20])

    def _check_intents(self) -> ChecklistItem:
        """Check intents coverage."""
        intents_dir = self.seed_dir / "intents"
        if not intents_dir.exists():
            return ChecklistItem(status="pending", count=0)

        count = len(list(intents_dir.glob("*.json")))
        status = "pass" if count > 0 else "pending"
        return ChecklistItem(status=status, count=count)

    def _check_catalog_rules(self) -> ChecklistItem:
        """Count catalog rules."""
        rules_dir = self.seed_dir / "catalog_rules"
        if not rules_dir.exists():
            return ChecklistItem(status="pending", count=0)

        count = len(list(rules_dir.glob("*.json")))
        status = "pass" if count > 0 else "pending"
        return ChecklistItem(status=status, count=count)

    def _check_faiss(self) -> ChecklistItem:
        """Check if FAISS index exists and loads."""
        models_dir = Path(__file__).resolve().parent / "models"
        index_path = models_dir / "similarity_index.faiss"
        meta_path = models_dir / "similarity_metadata.json"

        if not index_path.exists() or not meta_path.exists():
            return ChecklistItem(status="fail", errors=["FAISS index not found"])

        try:
            import faiss
            index = faiss.read_index(str(index_path))
            meta = json.loads(meta_path.read_text())
            n_vectors = index.ntotal
            return ChecklistItem(
                status="pass", count=n_vectors,
                extra={"vector_count": n_vectors},
            )
        except ImportError:
            return ChecklistItem(status="fail", errors=["faiss not installed"])
        except Exception as e:
            return ChecklistItem(status="fail", errors=[str(e)])

    @staticmethod
    def _print_checklist(manifest: SeedManifest) -> None:
        """Print checklist table to console."""
        items = [
            ("db_connection", manifest.db_connection),
            ("queries_loaded", manifest.queries_loaded),
            ("queries_parseable", manifest.queries_parseable),
            ("explains_gathered", manifest.explains_gathered),
            ("intents_attached", manifest.intents_attached),
            ("catalog_rules_loaded", manifest.catalog_rules_loaded),
            ("faiss_index_ready", manifest.faiss_index_ready),
        ]

        status_icons = {"pass": "PASS", "fail": "FAIL", "pending": "----"}

        print(f"\n{'='*55}")
        print(f"  Seed Checklist: {manifest.name} ({manifest.engine})")
        print(f"{'='*55}")

        for name, item in items:
            icon = status_icons.get(item.status, "????")
            detail = ""
            if item.count:
                detail = f" ({item.count})"
            if item.errors:
                detail += f" [{len(item.errors)} errors]"
            if item.missing:
                detail += f" [{len(item.missing)} missing]"
            print(f"  [{icon}] {name}{detail}")

        n_pass = sum(1 for _, i in items if i.status == "pass")
        print(f"\n  {n_pass}/{len(items)} checks passed")
        print(f"{'='*55}\n")


# =============================================================================
# CLI entry point: python3 -m ado.seed validate <benchmark_dir>
# =============================================================================

def main():
    """CLI entry point for seed operations."""
    import sys

    if len(sys.argv) < 2:
        print("Usage:")
        print("  python3 -m ado.seed validate <benchmark_dir>")
        print("  python3 -m ado.seed build <benchmark_dir> [--force]")
        sys.exit(1)

    command = sys.argv[1]
    benchmark_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else None

    if command == "validate" and benchmark_dir:
        seed_dir = benchmark_dir / "seed"
        if not seed_dir.exists():
            print(f"Seed directory not found: {seed_dir}")
            print("Run 'python3 -m ado.seed build <benchmark_dir>' first.")
            sys.exit(1)
        validator = SeedValidator(seed_dir)
        validator.validate_all()

    elif command == "build" and benchmark_dir:
        force = "--force" in sys.argv
        builder = SeedBuilder(benchmark_dir)
        builder.build(force=force)
        # Auto-validate after build
        validator = SeedValidator(benchmark_dir / "seed")
        validator.validate_all()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
