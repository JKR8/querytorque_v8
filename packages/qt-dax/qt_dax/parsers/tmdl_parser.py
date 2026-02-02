"""Parse TMDL files from PBIP semantic model folders."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


@dataclass
class TMDLMeasure:
    name: str
    table: str
    expression: str


@dataclass
class TMDLColumn:
    name: str
    table: str
    data_type: str


@dataclass
class TMDLRelationship:
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    cross_filter: str
    is_active: bool


class TMDLParser:
    """Parse PBIP semantic model TMDL files."""

    def __init__(self, semantic_model_dir: Path):
        self.semantic_model_dir = semantic_model_dir
        self.definition_dir = semantic_model_dir / "definition"
        self.tables_dir = self.definition_dir / "tables"
        self.relationships_file = self.definition_dir / "relationships.tmdl"

    def parse(self) -> dict:
        """Parse tables, columns, measures, and relationships."""
        if not self.definition_dir.exists():
            raise FileNotFoundError(f"definition folder not found: {self.definition_dir}")
        if not self.tables_dir.exists():
            raise FileNotFoundError(f"tables folder not found: {self.tables_dir}")

        tables = []
        columns: list[TMDLColumn] = []
        measures: list[TMDLMeasure] = []

        for table_file in sorted(self.tables_dir.glob("*.tmdl")):
            table_name, table_columns, table_measures = self._parse_table_file(table_file)
            if not table_name:
                continue
            tables.append({
                "name": table_name,
                "is_local_date_table": "LocalDateTable" in table_name,
            })
            columns.extend(table_columns)
            measures.extend(table_measures)

        relationships = self._parse_relationships(self.relationships_file) if self.relationships_file.exists() else []

        return {
            "tables": tables,
            "columns": columns,
            "measures": measures,
            "relationships": relationships,
        }

    def _parse_table_file(self, path: Path) -> tuple[str, list[TMDLColumn], list[TMDLMeasure]]:
        text = path.read_text(encoding="utf-8")
        lines = text.splitlines()
        table_name = ""
        columns: list[TMDLColumn] = []
        measures: list[TMDLMeasure] = []

        for line in lines:
            if line.startswith("table "):
                table_name = self._strip_name(line[len("table "):].strip())
                break

        if not table_name:
            return "", [], []

        i = 0
        while i < len(lines):
            line = lines[i]
            if self._is_block_start(line, "column"):
                col_name = self._strip_name(line.strip()[len("column "):])
                col_indent = self._indent_level(line)
                data_type = ""
                i += 1
                while i < len(lines) and self._indent_level(lines[i]) > col_indent:
                    inner = lines[i].strip()
                    if inner.startswith("dataType:"):
                        data_type = inner.split(":", 1)[1].strip()
                    i += 1
                columns.append(TMDLColumn(name=col_name, table=table_name, data_type=data_type))
                continue

            if self._is_block_start(line, "measure"):
                measure_name, expr, new_index = self._parse_measure(lines, i)
                measures.append(TMDLMeasure(name=measure_name, table=table_name, expression=expr))
                i = new_index
                continue

            i += 1

        return table_name, columns, measures

    def _parse_measure(self, lines: list[str], start_index: int) -> tuple[str, str, int]:
        line = lines[start_index]
        indent = self._indent_level(line)
        content = line.strip()[len("measure "):].strip()

        if "=" not in content:
            return self._strip_name(content), "", start_index + 1

        name_part, expr_part = content.split("=", 1)
        measure_name = self._strip_name(name_part.strip())
        expr = expr_part.strip()

        # Triple backtick block
        if expr.startswith("```"):
            expr_lines: list[str] = []
            # If there's content after opening fence, capture it
            remainder = expr[len("```"):].strip()
            if remainder:
                expr_lines.append(remainder)
            i = start_index + 1
            while i < len(lines):
                if "```" in lines[i]:
                    break
                expr_lines.append(lines[i].strip())
                i += 1
            return measure_name, "\n".join(expr_lines).strip(), i + 1

        # Inline expression
        if expr:
            return measure_name, expr, start_index + 1

        # Multiline expression without backticks
        expr_lines = []
        i = start_index + 1
        while i < len(lines):
            if self._indent_level(lines[i]) <= indent:
                break
            expr_lines.append(lines[i].strip())
            i += 1
        return measure_name, "\n".join(expr_lines).strip(), i

    def _parse_relationships(self, path: Path) -> list[TMDLRelationship]:
        lines = path.read_text(encoding="utf-8").splitlines()
        relationships: list[TMDLRelationship] = []

        i = 0
        while i < len(lines):
            line = lines[i]
            if line.startswith("relationship "):
                block = {
                    "from": "",
                    "to": "",
                    "cross_filter": "Single",
                    "is_active": True,
                }
                i += 1
                while i < len(lines) and not lines[i].startswith("relationship "):
                    inner = lines[i].strip()
                    if inner.startswith("fromColumn:"):
                        block["from"] = inner.split(":", 1)[1].strip()
                    elif inner.startswith("toColumn:"):
                        block["to"] = inner.split(":", 1)[1].strip()
                    elif inner.startswith("crossFilteringBehavior:"):
                        value = inner.split(":", 1)[1].strip()
                        block["cross_filter"] = "Both" if value == "bothDirections" else "Single"
                    elif inner.startswith("isActive:"):
                        value = inner.split(":", 1)[1].strip().lower()
                        block["is_active"] = value != "false"
                    i += 1

                from_table, from_column = self._split_column_ref(block["from"])
                to_table, to_column = self._split_column_ref(block["to"])

                if from_table and to_table:
                    relationships.append(TMDLRelationship(
                        from_table=from_table,
                        from_column=from_column,
                        to_table=to_table,
                        to_column=to_column,
                        cross_filter=block["cross_filter"],
                        is_active=block["is_active"],
                    ))
                continue

            i += 1

        return relationships

    def _split_column_ref(self, ref: str) -> tuple[str, str]:
        if not ref:
            return "", ""
        # Examples: 'Table'.'Column' or Table.Column or 'Table'.Column
        match = re.match(r"^'?([^'.]+)'?\.'?([^']+)'?$", ref)
        if match:
            return match.group(1), match.group(2)
        if "." in ref:
            parts = ref.split(".", 1)
            return parts[0].strip("'"), parts[1].strip("'")
        return ref.strip("'"), ""

    def _strip_name(self, raw: str) -> str:
        return raw.strip().strip("'")

    def _indent_level(self, line: str) -> int:
        return len(line) - len(line.lstrip("\t"))

    def _is_block_start(self, line: str, keyword: str) -> bool:
        return line.startswith("\t" + keyword + " ")
