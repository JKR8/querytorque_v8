#!/usr/bin/env python3
"""
Measure Dependency Analyzer - DAX-004
=====================================
Analyzes measure-to-measure references to build dependency graphs.
Used for optimization ordering and circular dependency detection.

Author: QueryTorque / Dialect Labs
Version: 1.0.0
"""

import re
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

class DependencyType(Enum):
    """Type of dependency between measures."""
    DIRECT = "direct"           # [Measure] reference
    CALCULATE = "calculate"     # Inside CALCULATE modifier
    ITERATOR = "iterator"       # Inside iterator (SUMX, etc.)
    CONDITIONAL = "conditional" # Inside IF/SWITCH branches


@dataclass
class MeasureReference:
    """A reference from one measure to another."""
    source_measure: str
    source_table: str
    target_measure: str
    target_table: Optional[str]
    dependency_type: DependencyType
    context: str  # Snippet showing the reference context
    line_number: int = 0


@dataclass
class MeasureNode:
    """Node in the dependency graph representing a measure."""
    name: str
    table: str
    expression: str

    # Dependencies
    depends_on: Set[str] = field(default_factory=set)      # Measures this one references
    depended_by: Set[str] = field(default_factory=set)     # Measures that reference this one

    # Analysis metadata
    depth: int = 0              # Max depth in dependency tree (0 = leaf)
    is_base_measure: bool = True  # No dependencies
    is_root_measure: bool = True  # Nothing depends on this
    in_cycle: bool = False      # Part of circular dependency

    # Full reference details
    references: List[MeasureReference] = field(default_factory=list)


@dataclass
class DependencyCycle:
    """A detected circular dependency."""
    measures: List[str]         # Ordered list of measures in the cycle
    cycle_path: str             # Human-readable cycle path
    severity: str = "high"      # Always high - cycles are problematic

    def __post_init__(self):
        # Create readable path: A -> B -> C -> A
        self.cycle_path = " -> ".join(self.measures + [self.measures[0]])


@dataclass
class OptimizationOrder:
    """Recommended order for optimizing measures."""
    order: List[str]            # Measures in optimization order (base first)
    levels: Dict[int, List[str]]  # Measures grouped by depth level
    blocked: List[str]          # Measures blocked by cycles


@dataclass
class DependencyAnalysisResult:
    """Complete dependency analysis result."""
    # Graph data
    nodes: Dict[str, MeasureNode]

    # Statistics
    total_measures: int
    base_measures: int          # No dependencies (depth 0)
    intermediate_measures: int  # Both depends and depended
    root_measures: int          # Nothing depends on them
    max_depth: int

    # Cycles
    cycles: List[DependencyCycle]
    measures_in_cycles: Set[str]

    # Optimization guidance
    optimization_order: OptimizationOrder

    # Complexity metrics
    total_edges: int            # Total dependency relationships
    avg_dependencies: float     # Average outgoing deps per measure
    max_dependencies: int       # Max outgoing deps (most complex)
    most_depended_on: List[Tuple[str, int]]  # Top 10 most referenced


# =============================================================================
# DAX REFERENCE PARSER
# =============================================================================

class DAXReferenceParser:
    """Parses DAX expressions to extract measure references."""

    # Pattern for measure references: [Measure Name] or Table[Measure Name]
    # Excludes column references (which are typically in aggregations)
    MEASURE_REF_PATTERN = re.compile(
        r"""
        (?:                           # Optional table prefix
            '?([A-Za-z_][A-Za-z0-9_ ]*)'?  # Table name (group 1)
            \s*
        )?
        \[                            # Opening bracket
            ([A-Za-z_][A-Za-z0-9_ ]*)  # Measure/Column name (group 2)
        \]                            # Closing bracket
        """,
        re.VERBOSE | re.IGNORECASE
    )

    # Aggregation functions that take columns, not measures
    COLUMN_FUNCTIONS = {
        'SUM', 'AVERAGE', 'MIN', 'MAX', 'COUNT', 'COUNTROWS',
        'DISTINCTCOUNT', 'COUNTBLANK', 'COUNTA', 'COUNTX',
        'SUMX', 'AVERAGEX', 'MINX', 'MAXX', 'RANKX',
        'FIRSTNONBLANK', 'LASTNONBLANK',
        'VALUES', 'DISTINCT', 'ALL', 'ALLEXCEPT', 'ALLSELECTED',
        'EARLIER', 'EARLIEST', 'RELATED', 'RELATEDTABLE',
        'LOOKUPVALUE', 'SELECTEDVALUE', 'HASONEVALUE',
        'ISBLANK', 'ISERROR', 'ISEMPTY',
        'CONCATENATE', 'CONCATENATEX', 'FORMAT',
    }

    # Iterator functions where inner refs might be columns
    ITERATOR_FUNCTIONS = {'SUMX', 'AVERAGEX', 'MAXX', 'MINX', 'COUNTX',
                          'RANKX', 'CONCATENATEX', 'FILTER', 'ADDCOLUMNS',
                          'SELECTCOLUMNS', 'GENERATE', 'GENERATEALL'}

    def __init__(self, measure_names: Set[str], table_measure_map: Dict[str, Set[str]]):
        """
        Initialize parser with known measures.

        Args:
            measure_names: Set of all measure names in the model
            table_measure_map: Dict mapping table names to their measures
        """
        self.measure_names = {m.lower() for m in measure_names}
        self.table_measure_map = {
            t.lower(): {m.lower() for m in measures}
            for t, measures in table_measure_map.items()
        }
        # Flat lookup: measure_name -> table_name
        self.measure_to_table = {}
        for table, measures in table_measure_map.items():
            for m in measures:
                self.measure_to_table[m.lower()] = table

    def parse_expression(
        self,
        expression: str,
        source_measure: str,
        source_table: str
    ) -> List[MeasureReference]:
        """
        Parse a DAX expression and extract measure references.

        Args:
            expression: The DAX expression to parse
            source_measure: Name of the measure containing this expression
            source_table: Table containing the source measure

        Returns:
            List of MeasureReference objects
        """
        references = []

        # Remove comments first
        clean_expr = self._remove_comments(expression)

        # Find all bracket references
        for match in self.MEASURE_REF_PATTERN.finditer(clean_expr):
            table_name = match.group(1)
            ref_name = match.group(2)
            ref_name_lower = ref_name.lower()

            # Skip if this is definitely a column (has table prefix and not a known measure)
            if table_name:
                table_lower = table_name.lower()
                # Check if this table has this as a measure
                if table_lower in self.table_measure_map:
                    if ref_name_lower not in self.table_measure_map[table_lower]:
                        # Not a measure in this table, it's a column
                        continue
                else:
                    # Unknown table, probably a column reference
                    continue
            else:
                # No table prefix - check if it's a known measure
                if ref_name_lower not in self.measure_names:
                    continue

            # Skip self-references
            if ref_name_lower == source_measure.lower():
                continue

            # Determine context/type
            dep_type, context = self._get_reference_context(
                clean_expr, match.start(), match.end()
            )

            # Calculate approximate line number
            line_num = expression[:match.start()].count('\n') + 1

            references.append(MeasureReference(
                source_measure=source_measure,
                source_table=source_table,
                target_measure=ref_name,
                target_table=table_name or self.measure_to_table.get(ref_name_lower),
                dependency_type=dep_type,
                context=context,
                line_number=line_num
            ))

        return references

    def _remove_comments(self, expression: str) -> str:
        """Remove DAX comments from expression."""
        # Remove single-line comments
        result = re.sub(r'//.*$', '', expression, flags=re.MULTILINE)
        # Remove multi-line comments
        result = re.sub(r'/\*.*?\*/', '', result, flags=re.DOTALL)
        return result

    def _get_reference_context(
        self,
        expression: str,
        ref_start: int,
        ref_end: int
    ) -> Tuple[DependencyType, str]:
        """
        Determine the context of a measure reference.

        Returns:
            Tuple of (DependencyType, context_snippet)
        """
        # Get surrounding context (50 chars each side)
        ctx_start = max(0, ref_start - 50)
        ctx_end = min(len(expression), ref_end + 50)
        context = expression[ctx_start:ctx_end].strip()

        # Look at what precedes the reference
        prefix = expression[max(0, ref_start - 100):ref_start].upper()

        # Check for iterator context
        for func in self.ITERATOR_FUNCTIONS:
            if func in prefix and '(' in prefix[prefix.rfind(func):]:
                return DependencyType.ITERATOR, context

        # Check for CALCULATE context
        if 'CALCULATE' in prefix:
            # Count parens to see if we're inside CALCULATE
            open_parens = prefix.count('(') - prefix.count(')')
            if open_parens > 0:
                return DependencyType.CALCULATE, context

        # Check for conditional context
        for cond in ('IF', 'SWITCH', 'COALESCE'):
            if cond in prefix:
                return DependencyType.CONDITIONAL, context

        return DependencyType.DIRECT, context


# =============================================================================
# DEPENDENCY GRAPH BUILDER
# =============================================================================

class MeasureDependencyAnalyzer:
    """
    Analyzes measure dependencies to build a complete dependency graph.

    Usage:
        analyzer = MeasureDependencyAnalyzer()
        result = analyzer.analyze(measures)
    """

    def analyze(
        self,
        measures: List[Dict],  # List of measure dicts with name, table, expression
    ) -> DependencyAnalysisResult:
        """
        Analyze all measures and build the dependency graph.

        Args:
            measures: List of measure dictionaries with keys:
                     - name: Measure name
                     - table: Table containing the measure
                     - expression: DAX expression

        Returns:
            DependencyAnalysisResult with complete analysis
        """
        # Build lookup structures
        measure_names = {m['name'] for m in measures}
        table_measure_map: Dict[str, Set[str]] = {}
        for m in measures:
            table = m.get('table', '_Measures')
            if table not in table_measure_map:
                table_measure_map[table] = set()
            table_measure_map[table].add(m['name'])

        # Initialize parser
        parser = DAXReferenceParser(measure_names, table_measure_map)

        # Create nodes
        nodes: Dict[str, MeasureNode] = {}
        for m in measures:
            key = self._make_key(m['name'], m.get('table'))
            nodes[key] = MeasureNode(
                name=m['name'],
                table=m.get('table', '_Measures'),
                expression=m.get('expression', '')
            )

        # Parse all expressions and build edges
        total_edges = 0
        for m in measures:
            key = self._make_key(m['name'], m.get('table'))
            node = nodes[key]

            expression = m.get('expression', '')
            if not expression:
                continue

            refs = parser.parse_expression(
                expression,
                m['name'],
                m.get('table', '_Measures')
            )

            for ref in refs:
                target_key = self._make_key(ref.target_measure, ref.target_table)

                # Only add if target exists (it's a real measure, not a column)
                if target_key in nodes:
                    node.depends_on.add(target_key)
                    node.references.append(ref)
                    nodes[target_key].depended_by.add(key)
                    total_edges += 1

        # Detect cycles
        cycles = self._detect_cycles(nodes)
        measures_in_cycles = set()
        for cycle in cycles:
            measures_in_cycles.update(cycle.measures)

        # Mark nodes in cycles
        for key in measures_in_cycles:
            if key in nodes:
                nodes[key].in_cycle = True

        # Calculate depths (longest path from any leaf)
        self._calculate_depths(nodes)

        # Classify nodes
        base_count = 0
        root_count = 0
        intermediate_count = 0
        max_depth = 0

        for node in nodes.values():
            node.is_base_measure = len(node.depends_on) == 0
            node.is_root_measure = len(node.depended_by) == 0

            if node.is_base_measure:
                base_count += 1
            elif node.is_root_measure:
                root_count += 1
            else:
                intermediate_count += 1

            max_depth = max(max_depth, node.depth)

        # Build optimization order
        opt_order = self._build_optimization_order(nodes, measures_in_cycles)

        # Calculate complexity metrics
        dep_counts = [len(n.depends_on) for n in nodes.values()]
        avg_deps = sum(dep_counts) / len(dep_counts) if dep_counts else 0
        max_deps = max(dep_counts) if dep_counts else 0

        # Find most depended-on measures
        depended_counts = [(key, len(n.depended_by)) for key, n in nodes.items()]
        depended_counts.sort(key=lambda x: x[1], reverse=True)
        most_depended = depended_counts[:10]

        return DependencyAnalysisResult(
            nodes=nodes,
            total_measures=len(nodes),
            base_measures=base_count,
            intermediate_measures=intermediate_count,
            root_measures=root_count,
            max_depth=max_depth,
            cycles=cycles,
            measures_in_cycles=measures_in_cycles,
            optimization_order=opt_order,
            total_edges=total_edges,
            avg_dependencies=round(avg_deps, 2),
            max_dependencies=max_deps,
            most_depended_on=most_depended
        )

    def _make_key(self, name: str, table: Optional[str] = None) -> str:
        """Create a unique key for a measure."""
        # Use just the measure name as key (Power BI measures have unique names)
        return name.lower()

    def _detect_cycles(self, nodes: Dict[str, MeasureNode]) -> List[DependencyCycle]:
        """
        Detect circular dependencies using Tarjan's algorithm.

        Returns:
            List of DependencyCycle objects
        """
        cycles = []
        visited = set()
        rec_stack = set()
        path = []

        def dfs(node_key: str):
            visited.add(node_key)
            rec_stack.add(node_key)
            path.append(node_key)

            node = nodes.get(node_key)
            if node:
                for dep_key in node.depends_on:
                    if dep_key not in visited:
                        dfs(dep_key)
                    elif dep_key in rec_stack:
                        # Found a cycle
                        cycle_start = path.index(dep_key)
                        cycle_measures = path[cycle_start:]
                        cycles.append(DependencyCycle(
                            measures=[nodes[k].name for k in cycle_measures]
                        ))

            path.pop()
            rec_stack.remove(node_key)

        for key in nodes:
            if key not in visited:
                dfs(key)

        return cycles

    def _calculate_depths(self, nodes: Dict[str, MeasureNode]) -> None:
        """
        Calculate depth for each node (longest path from any leaf).
        Uses dynamic programming with memoization.
        """
        memo: Dict[str, int] = {}

        def get_depth(key: str, visiting: Set[str]) -> int:
            if key in memo:
                return memo[key]

            if key in visiting:
                # Cycle detected, return 0 to avoid infinite recursion
                return 0

            node = nodes.get(key)
            if not node or not node.depends_on:
                memo[key] = 0
                return 0

            visiting.add(key)
            max_dep_depth = 0
            for dep_key in node.depends_on:
                dep_depth = get_depth(dep_key, visiting)
                max_dep_depth = max(max_dep_depth, dep_depth)
            visiting.remove(key)

            depth = max_dep_depth + 1
            memo[key] = depth
            return depth

        for key in nodes:
            depth = get_depth(key, set())
            nodes[key].depth = depth

    def _build_optimization_order(
        self,
        nodes: Dict[str, MeasureNode],
        blocked: Set[str]
    ) -> OptimizationOrder:
        """
        Build recommended optimization order (topological sort).
        Base measures first, then progressively more dependent ones.
        """
        # Group by depth
        levels: Dict[int, List[str]] = {}
        for key, node in nodes.items():
            if key in blocked:
                continue
            if node.depth not in levels:
                levels[node.depth] = []
            levels[node.depth].append(node.name)

        # Build ordered list
        order = []
        for depth in sorted(levels.keys()):
            # Sort alphabetically within each level for consistency
            for name in sorted(levels[depth]):
                order.append(name)

        return OptimizationOrder(
            order=order,
            levels=levels,
            blocked=list(blocked)
        )

    def get_dependency_chain(
        self,
        result: DependencyAnalysisResult,
        measure_name: str
    ) -> List[str]:
        """
        Get the full dependency chain for a measure (all measures it depends on).
        """
        key = measure_name.lower()
        if key not in result.nodes:
            return []

        chain = []
        visited = set()

        def collect(k: str):
            if k in visited:
                return
            visited.add(k)
            node = result.nodes.get(k)
            if node:
                for dep in node.depends_on:
                    collect(dep)
                chain.append(node.name)

        collect(key)
        return chain

    def get_impact_analysis(
        self,
        result: DependencyAnalysisResult,
        measure_name: str
    ) -> List[str]:
        """
        Get all measures that would be impacted if this measure changes.
        """
        key = measure_name.lower()
        if key not in result.nodes:
            return []

        impacted = []
        visited = set()

        def collect(k: str):
            if k in visited:
                return
            visited.add(k)
            node = result.nodes.get(k)
            if node:
                for dep in node.depended_by:
                    impacted.append(result.nodes[dep].name)
                    collect(dep)

        collect(key)
        return impacted


# =============================================================================
# MERMAID DIAGRAM GENERATOR
# =============================================================================

class DependencyDiagramGenerator:
    """Generate Mermaid diagrams for measure dependencies."""

    def generate_full_graph(
        self,
        result: DependencyAnalysisResult,
        max_nodes: int = 50
    ) -> str:
        """
        Generate a full dependency graph diagram.

        Args:
            result: DependencyAnalysisResult from analyzer
            max_nodes: Maximum nodes to include (for readability)

        Returns:
            Mermaid diagram string
        """
        lines = ["graph TD"]

        # Select nodes to include
        nodes_to_include = list(result.nodes.keys())[:max_nodes]

        # Style definitions
        lines.append("    classDef base fill:#90EE90,stroke:#228B22")
        lines.append("    classDef root fill:#87CEEB,stroke:#4682B4")
        lines.append("    classDef cycle fill:#FFB6C1,stroke:#DC143C")
        lines.append("    classDef intermediate fill:#F0E68C,stroke:#DAA520")

        # Add nodes with styling
        for key in nodes_to_include:
            node = result.nodes[key]
            safe_name = self._safe_id(node.name)
            label = node.name[:30] + "..." if len(node.name) > 30 else node.name

            if node.in_cycle:
                lines.append(f'    {safe_name}["{label}"]:::cycle')
            elif node.is_base_measure:
                lines.append(f'    {safe_name}["{label}"]:::base')
            elif node.is_root_measure:
                lines.append(f'    {safe_name}["{label}"]:::root')
            else:
                lines.append(f'    {safe_name}["{label}"]:::intermediate')

        # Add edges
        for key in nodes_to_include:
            node = result.nodes[key]
            safe_source = self._safe_id(node.name)

            for dep_key in node.depends_on:
                if dep_key in nodes_to_include:
                    dep_node = result.nodes[dep_key]
                    safe_target = self._safe_id(dep_node.name)
                    lines.append(f"    {safe_source} --> {safe_target}")

        return "\n".join(lines)

    def generate_measure_focus(
        self,
        result: DependencyAnalysisResult,
        measure_name: str,
        depth: int = 2
    ) -> str:
        """
        Generate a focused diagram around a specific measure.

        Args:
            result: DependencyAnalysisResult
            measure_name: Measure to focus on
            depth: How many levels of dependencies to show

        Returns:
            Mermaid diagram string
        """
        key = measure_name.lower()
        if key not in result.nodes:
            return "graph TD\n    A[Measure not found]"

        lines = ["graph TD"]
        lines.append("    classDef focus fill:#FFD700,stroke:#FF8C00,stroke-width:3px")
        lines.append("    classDef depends fill:#90EE90,stroke:#228B22")
        lines.append("    classDef dependedby fill:#87CEEB,stroke:#4682B4")

        included = {key}

        # Collect dependencies (measures this one uses)
        def add_deps(k: str, current_depth: int):
            if current_depth > depth:
                return
            node = result.nodes.get(k)
            if node:
                for dep in node.depends_on:
                    included.add(dep)
                    if current_depth < depth:
                        add_deps(dep, current_depth + 1)

        # Collect dependents (measures that use this one)
        def add_dependents(k: str, current_depth: int):
            if current_depth > depth:
                return
            node = result.nodes.get(k)
            if node:
                for dep in node.depended_by:
                    included.add(dep)
                    if current_depth < depth:
                        add_dependents(dep, current_depth + 1)

        add_deps(key, 1)
        add_dependents(key, 1)

        # Add nodes
        focus_node = result.nodes[key]
        for k in included:
            node = result.nodes[k]
            safe_name = self._safe_id(node.name)
            label = node.name[:30] + "..." if len(node.name) > 30 else node.name

            if k == key:
                lines.append(f'    {safe_name}["{label}"]:::focus')
            elif k in focus_node.depends_on:
                lines.append(f'    {safe_name}["{label}"]:::depends')
            else:
                lines.append(f'    {safe_name}["{label}"]:::dependedby')

        # Add edges
        for k in included:
            node = result.nodes[k]
            safe_source = self._safe_id(node.name)

            for dep_key in node.depends_on:
                if dep_key in included:
                    dep_node = result.nodes[dep_key]
                    safe_target = self._safe_id(dep_node.name)
                    lines.append(f"    {safe_source} --> {safe_target}")

        return "\n".join(lines)

    def _safe_id(self, name: str) -> str:
        """Convert measure name to safe Mermaid ID."""
        # Replace special chars, keep alphanumeric and underscore
        safe = re.sub(r'[^A-Za-z0-9_]', '_', name)
        # Ensure starts with letter
        if safe and not safe[0].isalpha():
            safe = 'M_' + safe
        return safe


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_dependency_analyzer() -> MeasureDependencyAnalyzer:
    """Factory function to create a MeasureDependencyAnalyzer."""
    return MeasureDependencyAnalyzer()


def create_diagram_generator() -> DependencyDiagramGenerator:
    """Factory function to create a DependencyDiagramGenerator."""
    return DependencyDiagramGenerator()
