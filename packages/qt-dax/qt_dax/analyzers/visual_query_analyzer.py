"""Visual Query Analyzer for Power BI Performance Analyzer queries.

Parses DAX queries captured from Power BI visuals (via Performance Analyzer),
extracts measure definitions, resolves dependencies from the live model,
and orchestrates optimization.

Usage:
    from qt_dax.analyzers.visual_query_analyzer import VisualQueryAnalyzer
    from qt_dax.connections import PBIDesktopConnection

    # Paste query from Performance Analyzer
    query = '''
    DEFINE
        MEASURE 'Table'[MyMeasure] = SUM('Sales'[Amount])
    EVALUATE
        SUMMARIZECOLUMNS(...)
    '''

    with PBIDesktopConnection(port) as conn:
        analyzer = VisualQueryAnalyzer(conn)
        result = analyzer.analyze(query)

        print(f"Found {len(result.measures)} measures")
        print(f"Dependency depth: {result.max_depth}")

        # Optimize
        optimized = analyzer.optimize(result, provider="deepseek")
"""

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from qt_dax.connections import PBIDesktopConnection


@dataclass
class ParsedMeasure:
    """A measure definition parsed from a query or model."""
    name: str
    table: str
    expression: str
    source: str  # "query" (inline) or "model" (fetched from PBI)
    line_number: int = 0


@dataclass
class VisualQueryAnalysis:
    """Result of analyzing a visual query."""
    # Original query
    original_query: str

    # Parsed components
    define_block: str
    evaluate_block: str
    order_by_block: str

    # Measures found
    inline_measures: List[ParsedMeasure]  # Defined in DEFINE block
    model_measures: List[ParsedMeasure]   # Fetched from PBI model
    all_measures: Dict[str, ParsedMeasure]  # Combined lookup

    # Dependencies
    measure_dependencies: Dict[str, Set[str]]  # measure -> measures it references
    dependency_order: List[str]  # Topological order for optimization
    max_depth: int

    # Filters/context from query
    filter_tables: List[str]  # Tables referenced in filter context
    output_columns: List[str]  # Columns in the output


@dataclass
class OptimizedQuery:
    """Result of optimizing a visual query."""
    original_query: str
    optimized_query: str
    measures_optimized: List[Dict]  # {name, original, optimized, rationale}
    validation_results: List[Dict]  # {name, status, speedup}


class VisualQueryParser:
    """Parses Performance Analyzer DAX queries."""

    # Pattern to extract DEFINE block measures
    MEASURE_PATTERN = re.compile(
        r"MEASURE\s+"
        r"'?([^'[\]]+)'?"  # Table name (group 1)
        r"\s*\[\s*"
        r"([^\]]+)"        # Measure name (group 2)
        r"\s*\]\s*=\s*"
        r"((?:(?!(?:^MEASURE\s|^EVALUATE\b|^ORDER\s+BY\b))[\s\S])+)",  # Expression (group 3)
        re.MULTILINE | re.IGNORECASE
    )

    # Pattern to split DEFINE/EVALUATE/ORDER BY blocks
    BLOCK_PATTERN = re.compile(
        r"(DEFINE\s+.*?)(?=EVALUATE\b)"
        r"(EVALUATE\s+.*?)(?=ORDER\s+BY\b|$)"
        r"(ORDER\s+BY\s+.*)?",
        re.DOTALL | re.IGNORECASE
    )

    def parse(self, query: str) -> Tuple[str, str, str, List[ParsedMeasure]]:
        """
        Parse a visual query into its components.

        Returns:
            Tuple of (define_block, evaluate_block, order_by_block, measures)
        """
        # Clean up the query
        query = query.strip()

        # Remove leading comments
        lines = query.split('\n')
        while lines and lines[0].strip().startswith('//'):
            lines.pop(0)
        query = '\n'.join(lines)

        # Extract blocks
        define_block = ""
        evaluate_block = ""
        order_by_block = ""

        # Find DEFINE block
        define_match = re.search(r'DEFINE\b(.*?)(?=EVALUATE\b)', query, re.DOTALL | re.IGNORECASE)
        if define_match:
            define_block = "DEFINE" + define_match.group(1)

        # Find EVALUATE block
        eval_match = re.search(r'EVALUATE\b(.*?)(?=ORDER\s+BY\b|$)', query, re.DOTALL | re.IGNORECASE)
        if eval_match:
            evaluate_block = "EVALUATE" + eval_match.group(1)

        # Find ORDER BY block
        order_match = re.search(r'ORDER\s+BY\b(.*?)$', query, re.DOTALL | re.IGNORECASE)
        if order_match:
            order_by_block = "ORDER BY" + order_match.group(1)

        # Extract measures from DEFINE block
        measures = self._extract_measures(define_block)

        return define_block, evaluate_block, order_by_block, measures

    def _extract_measures(self, define_block: str) -> List[ParsedMeasure]:
        """Extract MEASURE definitions from DEFINE block."""
        measures = []

        # Split by MEASURE keyword and process each
        parts = re.split(r'\bMEASURE\b', define_block, flags=re.IGNORECASE)

        for i, part in enumerate(parts[1:], 1):  # Skip first empty part
            # Parse table and measure name
            match = re.match(
                r"\s*'?([^'[\]\s]+)'?\s*\[\s*([^\]]+)\s*\]\s*=\s*([\s\S]+)",
                part
            )
            if match:
                table = match.group(1).strip("'")
                name = match.group(2).strip()
                expression = match.group(3).strip()

                # Find where this expression ends (next MEASURE, VAR, or EVALUATE)
                end_markers = [
                    (expression.upper().find('\nMEASURE '), 'MEASURE'),
                    (expression.upper().find('\nVAR '), 'VAR'),
                    (expression.upper().find('\nEVALUATE'), 'EVALUATE'),
                ]
                end_markers = [(pos, marker) for pos, marker in end_markers if pos > 0]

                if end_markers:
                    min_pos = min(pos for pos, _ in end_markers)
                    expression = expression[:min_pos].strip()

                measures.append(ParsedMeasure(
                    name=name,
                    table=table,
                    expression=expression,
                    source="query",
                    line_number=i
                ))

        return measures


class VisualQueryAnalyzer:
    """Analyzes and optimizes Power BI visual queries."""

    def __init__(self, connection: Optional["PBIDesktopConnection"] = None):
        """
        Initialize analyzer.

        Args:
            connection: Optional PBI Desktop connection for fetching model measures
        """
        self.connection = connection
        self.parser = VisualQueryParser()
        self._model_measures: Optional[Dict[str, ParsedMeasure]] = None

    def analyze(self, query: str) -> VisualQueryAnalysis:
        """
        Analyze a visual query.

        Args:
            query: DAX query from Performance Analyzer

        Returns:
            VisualQueryAnalysis with parsed components and dependencies
        """
        # Parse the query
        define_block, evaluate_block, order_by, inline_measures = self.parser.parse(query)

        # Build lookup for inline measures
        all_measures: Dict[str, ParsedMeasure] = {}
        for m in inline_measures:
            key = m.name.lower()
            all_measures[key] = m

        # Find measure references in inline measure expressions
        measure_refs = self._find_measure_references(inline_measures)

        # Also find measure references in the full query (DEFINE + EVALUATE blocks)
        # This catches 'Table'[Measure] references in SUMMARIZECOLUMNS etc.
        query_refs = self._extract_measure_refs_from_query(define_block + "\n" + evaluate_block)
        measure_refs.update(query_refs)

        # Fetch missing measures from model
        model_measures = []
        if self.connection:
            missing = self._find_missing_measures(measure_refs, all_measures)
            model_measures = self._fetch_model_measures(missing)
            for m in model_measures:
                all_measures[m.name.lower()] = m

            # Recursively find dependencies of model measures
            model_measures = self._resolve_deep_dependencies(model_measures, all_measures)

        # Build dependency graph
        dependencies = self._build_dependencies(all_measures)

        # Calculate dependency order (topological sort)
        order, max_depth = self._topological_sort(dependencies)

        # Extract filter context
        filter_tables = self._extract_filter_tables(evaluate_block)
        output_columns = self._extract_output_columns(evaluate_block)

        return VisualQueryAnalysis(
            original_query=query,
            define_block=define_block,
            evaluate_block=evaluate_block,
            order_by_block=order_by,
            inline_measures=inline_measures,
            model_measures=model_measures,
            all_measures=all_measures,
            measure_dependencies=dependencies,
            dependency_order=order,
            max_depth=max_depth,
            filter_tables=filter_tables,
            output_columns=output_columns,
        )

    def _find_measure_references(self, measures: List[ParsedMeasure]) -> Set[str]:
        """Find all measure references in expressions."""
        refs = set()
        pattern = re.compile(r'\[([^\]]+)\]')

        for m in measures:
            for match in pattern.finditer(m.expression):
                ref_name = match.group(1)
                # Skip if it looks like a column (preceded by table reference)
                refs.add(ref_name.lower())

        return refs

    def _extract_measure_refs_from_query(self, query: str) -> Set[str]:
        """
        Extract measure references from query text.

        Looks for patterns like 'Table'[Measure] which indicate measure references
        (as opposed to 'Table'[Column] in aggregations).
        """
        refs = set()

        # Pattern for 'Table'[Name] - table-qualified references
        table_ref_pattern = re.compile(r"'([^']+)'\s*\[\s*([^\]]+)\s*\]")

        for match in table_ref_pattern.finditer(query):
            table = match.group(1)
            name = match.group(2)
            # Add as potential measure reference
            refs.add(name.lower())

        return refs

    def _find_missing_measures(
        self,
        refs: Set[str],
        known: Dict[str, ParsedMeasure]
    ) -> Set[str]:
        """Find referenced measures not in known set."""
        return refs - set(known.keys())

    def _fetch_model_measures(self, names: Set[str]) -> List[ParsedMeasure]:
        """Fetch measure definitions from PBI model."""
        if not self.connection or not names:
            return []

        # Cache model measures if not already done
        if self._model_measures is None:
            self._model_measures = {}
            try:
                model_measures = self.connection.get_measures()
                for m in model_measures:
                    name = m.get("Measure", "")
                    key = name.lower()
                    self._model_measures[key] = ParsedMeasure(
                        name=name,
                        table=m.get("Table", "").strip("[]"),
                        expression=m.get("Expression", ""),
                        source="model"
                    )
            except Exception:
                pass

        # Return requested measures
        result = []
        for name in names:
            if name in self._model_measures:
                result.append(self._model_measures[name])

        return result

    def _resolve_deep_dependencies(
        self,
        measures: List[ParsedMeasure],
        known: Dict[str, ParsedMeasure]
    ) -> List[ParsedMeasure]:
        """Recursively resolve dependencies of measures."""
        all_fetched = list(measures)
        to_process = list(measures)

        while to_process:
            current = to_process.pop(0)
            refs = self._find_measure_references([current])
            missing = self._find_missing_measures(refs, known)

            if missing:
                new_measures = self._fetch_model_measures(missing)
                for m in new_measures:
                    key = m.name.lower()
                    if key not in known:
                        known[key] = m
                        all_fetched.append(m)
                        to_process.append(m)

        return all_fetched

    def _build_dependencies(
        self,
        measures: Dict[str, ParsedMeasure]
    ) -> Dict[str, Set[str]]:
        """Build dependency graph."""
        deps: Dict[str, Set[str]] = {}
        pattern = re.compile(r'\[([^\]]+)\]')

        for key, m in measures.items():
            deps[key] = set()
            for match in pattern.finditer(m.expression):
                ref = match.group(1).lower()
                if ref in measures and ref != key:
                    deps[key].add(ref)

        return deps

    def _topological_sort(
        self,
        deps: Dict[str, Set[str]]
    ) -> Tuple[List[str], int]:
        """Topological sort with depth calculation."""
        # Calculate depths
        depths: Dict[str, int] = {}

        def get_depth(key: str, visiting: Set[str]) -> int:
            if key in depths:
                return depths[key]
            if key in visiting:
                return 0  # Cycle
            if key not in deps:
                return 0

            visiting.add(key)
            max_dep = 0
            for dep in deps.get(key, set()):
                max_dep = max(max_dep, get_depth(dep, visiting) + 1)
            visiting.remove(key)

            depths[key] = max_dep
            return max_dep

        for key in deps:
            get_depth(key, set())

        # Sort by depth (leaves first)
        order = sorted(deps.keys(), key=lambda k: depths.get(k, 0))
        max_depth = max(depths.values()) if depths else 0

        return order, max_depth

    def _extract_filter_tables(self, evaluate_block: str) -> List[str]:
        """Extract tables used in filter context."""
        tables = set()
        # Look for TREATAS, FILTER table refs
        pattern = re.compile(r"'([^']+)'")
        for match in pattern.finditer(evaluate_block):
            tables.add(match.group(1))
        return sorted(tables)

    def _extract_output_columns(self, evaluate_block: str) -> List[str]:
        """Extract output columns from SUMMARIZECOLUMNS etc."""
        columns = []
        # Look for column refs in SUMMARIZECOLUMNS first arg
        pattern = re.compile(r"'[^']+'\[([^\]]+)\]")
        for match in pattern.finditer(evaluate_block):
            columns.append(match.group(1))
        return columns

    def rebuild_query(
        self,
        analysis: VisualQueryAnalysis,
        optimized_measures: Dict[str, str]
    ) -> str:
        """
        Rebuild the query with optimized measures.

        Args:
            analysis: Original analysis
            optimized_measures: Dict of measure_name -> optimized_expression

        Returns:
            Complete DAX query with optimized measures
        """
        lines = ["DEFINE"]

        # Add measures in dependency order
        for key in analysis.dependency_order:
            m = analysis.all_measures.get(key)
            if m:
                expr = optimized_measures.get(m.name, m.expression)
                lines.append(f"    MEASURE '{m.table}'[{m.name}] = {expr}")
                lines.append("")

        lines.append(analysis.evaluate_block)

        if analysis.order_by_block:
            lines.append(analysis.order_by_block)

        return "\n".join(lines)


def analyze_visual_query(
    query: str,
    connection: Optional["PBIDesktopConnection"] = None
) -> VisualQueryAnalysis:
    """Convenience function to analyze a visual query."""
    analyzer = VisualQueryAnalyzer(connection)
    return analyzer.analyze(query)
