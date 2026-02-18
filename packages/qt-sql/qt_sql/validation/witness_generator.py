"""Multi-row witness generation for SQL equivalence testing.

Generates three witness variants to increase semantic equivalence recall:

1. **Golden witness**: Satisfies all predicates, produces result rows.
2. **Clone witness**: Shifted surrogate keys, same values — structural invariance.
3. **Boundary-fail witness**: Violates exactly one predicate by epsilon — catches
   predicate relaxation/tightening in optimized rewrites.

Usage::

    from qt_sql.validation.witness_generator import MultiRowWitnessGenerator

    gen = MultiRowWitnessGenerator(conn, tables, filter_values)
    for witness_name, populate_fn in gen.witness_variants():
        populate_fn()
        # execute orig + opt, compare results
"""

import hashlib
import random
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

import duckdb


class MultiRowWitnessGenerator:
    """Generates multiple witness data sets for stronger equivalence testing.

    Each witness variant populates the DuckDB tables with a different data set,
    then the caller executes both original and optimized SQL and compares results.
    """

    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        tables: Dict[str, Dict[str, Any]],
        filter_values: Dict[str, Dict[str, list]],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        generation_order: List[str],
        table_row_counts: Dict[str, int],
        *,
        seed: int = 42,
    ):
        self.conn = conn
        self.tables = tables
        self.filter_values = filter_values
        self.fk_relationships = fk_relationships
        self.generation_order = generation_order
        self.table_row_counts = table_row_counts
        self.seed = seed

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def witness_variants(
        self,
    ) -> List[Tuple[str, Callable[[], None]]]:
        """Return named witness variants as ``(name, populate_fn)`` pairs.

        The caller should iterate, call ``populate_fn()`` to fill the DuckDB
        tables, execute both queries, and compare results.
        """
        return [
            ("clone", self._populate_clone_witness),
            ("boundary_fail", self._populate_boundary_fail_witness),
        ]

    # ------------------------------------------------------------------
    # Clone witness
    # ------------------------------------------------------------------

    def _populate_clone_witness(self) -> None:
        """Re-populate tables with shifted surrogate keys, same filter values.

        Shifts all ``*_sk``, ``*_id``, ``id`` columns by +10000 so the data
        is structurally equivalent but has different key values.
        """
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        # Drop and recreate
        for table_name in reversed(self.generation_order):
            try:
                self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            except Exception:
                pass

        self._create_schema()

        generator = SyntheticDataGenerator(self.conn, all_schemas=self.tables)
        generator.filter_literal_values = self.filter_values

        clone_rng = random.Random(self.seed + 1)

        for table_name in self.generation_order:
            schema = self.tables[table_name]
            row_count = max(10, min(200000, self.table_row_counts.get(table_name, 100)))
            table_fks = self.fk_relationships.get(table_name, {})
            generator.generate_table_data(
                table_name=table_name,
                schema=schema,
                row_count=row_count,
                foreign_keys=table_fks,
            )

        # Shift surrogate keys by +10000
        for table_name in self.generation_order:
            columns = self.tables[table_name].get("columns", {})
            for col_name in columns:
                cl = col_name.lower()
                if cl.endswith("_sk") or cl.endswith("_id") or cl == "id":
                    col_info = columns[col_name]
                    col_type = (
                        col_info.get("type", "").upper()
                        if isinstance(col_info, dict)
                        else str(col_info).upper()
                    )
                    if "INT" in col_type:
                        try:
                            self.conn.execute(
                                f'UPDATE "{table_name}" SET "{col_name}" = "{col_name}" + 10000'
                            )
                        except Exception:
                            pass

    # ------------------------------------------------------------------
    # Boundary-fail witness
    # ------------------------------------------------------------------

    def _populate_boundary_fail_witness(self) -> None:
        """Re-populate with values that violate exactly one predicate per table.

        For each table with BETWEEN or range filters, shift the column value
        just past the boundary (high + 1 for integers, high + 1 day for dates).
        This catches predicate relaxation bugs in optimized rewrites.
        """
        from qt_sql.validation.synthetic_validator import SyntheticDataGenerator

        # Drop and recreate
        for table_name in reversed(self.generation_order):
            try:
                self.conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
            except Exception:
                pass

        self._create_schema()

        # Generate normal data first
        generator = SyntheticDataGenerator(self.conn, all_schemas=self.tables)
        generator.filter_literal_values = self.filter_values

        for table_name in self.generation_order:
            schema = self.tables[table_name]
            row_count = max(10, min(200000, self.table_row_counts.get(table_name, 100)))
            table_fks = self.fk_relationships.get(table_name, {})
            generator.generate_table_data(
                table_name=table_name,
                schema=schema,
                row_count=row_count,
                foreign_keys=table_fks,
            )

        # Perturb boundary values
        for table_name, col_filters in self.filter_values.items():
            columns = self.tables.get(table_name, {}).get("columns", {})
            for col_name, vals in col_filters.items():
                for val in vals:
                    if not isinstance(val, str) or not val.startswith("BETWEEN:"):
                        continue
                    parts = val.split(":")
                    if len(parts) < 3:
                        continue
                    _, low_str, high_str = parts[0], parts[1], parts[2]

                    col_info = columns.get(col_name, {})
                    col_type = (
                        col_info.get("type", "").upper()
                        if isinstance(col_info, dict)
                        else str(col_info).upper()
                    )

                    try:
                        if "DATE" in col_type:
                            # Shift past the high boundary by 1 day
                            high_date = datetime.strptime(high_str.strip("'\""), "%Y-%m-%d")
                            out_of_range = (high_date + timedelta(days=1)).strftime("%Y-%m-%d")
                            self.conn.execute(
                                f"UPDATE \"{table_name}\" SET \"{col_name}\" = '{out_of_range}' "
                                f"WHERE \"{col_name}\" IS NOT NULL LIMIT 1"
                            )
                        elif "INT" in col_type:
                            high_val = int(float(high_str))
                            self.conn.execute(
                                f'UPDATE "{table_name}" SET "{col_name}" = {high_val + 1} '
                                f'WHERE "{col_name}" IS NOT NULL LIMIT 1'
                            )
                        elif "DECIMAL" in col_type or "NUMERIC" in col_type or "FLOAT" in col_type:
                            high_val = float(high_str)
                            self.conn.execute(
                                f'UPDATE "{table_name}" SET "{col_name}" = {high_val + 0.01} '
                                f'WHERE "{col_name}" IS NOT NULL LIMIT 1'
                            )
                    except Exception:
                        pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_schema(self) -> None:
        """Create tables in DuckDB from the schema dict."""
        for table_name in self.generation_order:
            columns = self.tables[table_name].get("columns", {})
            col_defs = []
            for col_name, col_info in columns.items():
                col_type = (
                    col_info.get("type", "VARCHAR(50)")
                    if isinstance(col_info, dict)
                    else str(col_info or "VARCHAR(50)")
                )
                col_defs.append(f'"{col_name}" {col_type}')
            if col_defs:
                ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({", ".join(col_defs)})'
                try:
                    self.conn.execute(ddl)
                except Exception:
                    pass
