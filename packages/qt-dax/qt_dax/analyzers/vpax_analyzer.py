#!/usr/bin/env python3
"""
VPAX Analyzer - DAX Anti-Pattern Detection
==========================================
Production-grade VertiPaq analysis tool for Power BI model diagnostics.
Outputs LLM-ready structured data for automated optimization recommendations.

Author: Dialect Labs
Version: 1.0.0
"""

import json
import logging
import time
import zipfile
import re
import sys
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional, TYPE_CHECKING
from datetime import datetime
from enum import Enum

# Parser and dependency imports for DAX027/028
from ..parsers.dax_parser import analyze_dax, DAXMetadata
from .measure_dependencies import MeasureDependencyAnalyzer, DependencyAnalysisResult

# Module logger
logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS & THRESHOLDS
# =============================================================================

class Severity(Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class Category(Enum):
    # Model Structure
    MODEL_SIZE = "model_size"
    CARDINALITY = "cardinality"
    ENCODING = "encoding"
    RELATIONSHIP = "relationship"
    DATE_TABLE = "date_table"

    # DAX Patterns
    DAX_ANTI_PATTERN = "dax_anti_pattern"
    DAX_PERFORMANCE = "dax_performance"
    DAX_CORRECTNESS = "dax_correctness"
    DAX_MAINTAINABILITY = "dax_maintainability"

    # Architecture
    ARCHITECTURE = "architecture"


# Performance thresholds
THRESHOLDS = {
    "cardinality_warning": 100_000,
    "cardinality_critical": 1_000_000,
    "dictionary_ratio_warning": 0.80,
    "dictionary_ratio_critical": 0.95,
    "column_size_warning_mb": 50,
    "column_size_critical_mb": 200,
    "table_size_warning_mb": 100,
    "table_size_critical_mb": 500,
    "measure_complexity_warning": 300,  # chars
    "measure_complexity_critical": 800,
    "calculate_nesting_warning": 2,
    "calculate_nesting_critical": 4,
    "ri_violations_warning": 10,
    "ri_violations_critical": 100,
    "local_date_table_max": 1,  # Should be 0 ideally
}


# =============================================================================
# DAX ANTI-PATTERN DEFINITIONS
# =============================================================================

DAX_RULES = {
    # CRITICAL - Performance killers
    "DAX001": {
        "name": "FILTER_TABLE_ITERATOR",
        "description": "FILTER iterating entire table instead of column",
        "pattern": r"FILTER\s*\(\s*'?[A-Za-z][A-Za-z0-9_ ]*'?\s*,",
        "exclude_pattern": r"FILTER\s*\(\s*(ALL|VALUES|DISTINCT|CALCULATETABLE|SUMMARIZE|ADDCOLUMNS)",
        "severity": Severity.CRITICAL,
        "category": Category.DAX_ANTI_PATTERN,
        "penalty": 20,
        "recommendation": "Use CALCULATETABLE with filter arguments or filter on specific columns",
    },
    "DAX002": {
        "name": "SUMX_FILTER_COMBO",
        "description": "SUMX/AVERAGEX with FILTER creates row-by-row iteration",
        "pattern": r"(SUMX|AVERAGEX|MAXX|MINX|COUNTX)\s*\(\s*FILTER\s*\(",
        "severity": Severity.CRITICAL,
        "category": Category.DAX_ANTI_PATTERN,
        "penalty": 25,
        "recommendation": "Use CALCULATE with filter arguments, or CALCULATETABLE for the filtered set",
    },
    "DAX003": {
        "name": "DEEP_CALCULATE_NESTING",
        "description": "Excessive CALCULATE nesting causes exponential context transitions",
        "pattern": None,  # Detected by count
        "threshold": 4,
        "severity": Severity.CRITICAL,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 15,
        "recommendation": "Flatten CALCULATE structure using VAR and SWITCH patterns",
    },

    # HIGH - Correctness and performance issues
    "DAX004": {
        "name": "DIVISION_WITHOUT_DIVIDE",
        "description": "Division operator (/) without DIVIDE function risks divide-by-zero",
        "pattern": r"(?<![A-Za-z\/])\/(?![\/\*])",
        "exclude_pattern": r"\bDIVIDE\s*\(",
        "severity": Severity.HIGH,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 8,
        "recommendation": "Use DIVIDE(numerator, denominator, alternateResult) for safe division",
    },
    "DAX005": {
        "name": "CALCULATE_NESTING_WARNING",
        "description": "Multiple CALCULATE statements may indicate suboptimal pattern",
        "pattern": None,  # Detected by count
        "threshold": 2,
        "severity": Severity.HIGH,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 10,
        "recommendation": "Review if nested CALCULATE can be flattened",
    },
    "DAX006": {
        "name": "MISSING_VAR_COMPLEX_MEASURE",
        "description": "Complex measure without VAR causes repeated expression evaluation",
        "pattern": None,  # Detected by length + VAR absence
        "threshold": 300,
        "severity": Severity.HIGH,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 10,
        "recommendation": "Use VAR to store intermediate results and prevent re-evaluation",
    },
    "DAX007": {
        "name": "IF_INSTEAD_OF_SWITCH",
        "description": "Multiple nested IF statements should use SWITCH for clarity",
        "pattern": None,  # Detected by count
        "threshold": 3,
        "severity": Severity.MEDIUM,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 5,
        "recommendation": "Replace nested IF with SWITCH(TRUE(), condition1, result1, ...)",
    },

    # MEDIUM - Maintainability and style
    "DAX008": {
        "name": "HARDCODED_FILTER_VALUES",
        "description": "Hardcoded string literals in filters reduce maintainability",
        "pattern": r'=\s*"[A-Za-z][A-Za-z0-9\s\-_]+"',
        "threshold": 3,  # Per measure
        "severity": Severity.LOW,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 2,
        "recommendation": "Consider using a configuration table or parameter for filter values",
    },
    "DAX009": {
        "name": "COMMENTED_CODE",
        "description": "Commented-out code indicates technical debt",
        "pattern": r"(\/\*[\s\S]*?\*\/|\/\/.*$)",
        "severity": Severity.LOW,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 2,
        "recommendation": "Remove dead code; use source control for history",
    },
    "DAX010": {
        "name": "DISTINCTCOUNT_FUSION_BLOCKER",
        "description": "DISTINCTCOUNT prevents query fusion with other aggregations",
        "pattern": r"\bDISTINCTCOUNT\s*\(",
        "severity": Severity.INFO,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 1,
        "recommendation": "Consider if COUNT would suffice; be aware of fusion implications",
    },
    "DAX011": {
        "name": "USERELATIONSHIP_AGGREGATION_BLOCKER",
        "description": "USERELATIONSHIP prevents aggregation table hits in composite models",
        "pattern": r"\bUSERELATIONSHIP\s*\(",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 5,
        "recommendation": "Be aware this blocks aggregation tables in DirectQuery scenarios",
    },
    "DAX012": {
        "name": "SELECTEDVALUE_EXCESSIVE",
        "description": "Excessive SELECTEDVALUE usage may indicate architectural issues",
        "pattern": r"\bSELECTEDVALUE\s*\(",
        "threshold": 4,
        "severity": Severity.MEDIUM,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 5,
        "recommendation": "Consider using calculation groups or better model design",
    },
    "DAX013": {
        "name": "ISINSCOPE_MATRIX_LOGIC",
        "description": "ISINSCOPE for matrix-level logic is complex and hard to maintain",
        "pattern": r"\bISINSCOPE\s*\(",
        "severity": Severity.INFO,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 1,
        "recommendation": "Document matrix behavior; consider calculation groups",
    },
    "DAX014": {
        "name": "ALLEXCEPT_PATTERN",
        "description": "ALLEXCEPT may not behave as expected with expanded tables",
        "pattern": r"\bALLEXCEPT\s*\(",
        "severity": Severity.INFO,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 1,
        "recommendation": "Verify behavior with expanded tables; consider REMOVEFILTERS",
    },
    "DAX015": {
        "name": "CONTEXT_TRANSITION_IN_ITERATOR",
        "description": "Measure reference inside iterator causes context transition per row",
        "pattern": r"(SUMX|AVERAGEX|MAXX|MINX|COUNTX|FILTER)\s*\([^)]*\[[^\]]+\]\s*\)",
        "severity": Severity.INFO,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 1,
        "recommendation": "Be aware of context transition cost; use column references where possible",
    },

    # Time Intelligence Issues
    "DAX016": {
        "name": "TIME_INTELLIGENCE_WITHOUT_DATE_TABLE",
        "description": "Time intelligence functions require a proper date dimension table",
        "pattern": r"\b(TOTALYTD|TOTALQTD|TOTALMTD|SAMEPERIODLASTYEAR|DATEADD|PARALLELPERIOD|PREVIOUSYEAR|PREVIOUSQUARTER|PREVIOUSMONTH)\s*\(",
        "severity": Severity.HIGH,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 12,
        "recommendation": "Ensure a proper marked date table exists; avoid using auto date/time",
    },
    "DAX017": {
        "name": "DATESBETWEEN_PERFORMANCE",
        "description": "DATESBETWEEN can be slow; consider DATESINPERIOD for relative ranges",
        "pattern": r"\bDATESBETWEEN\s*\(",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 5,
        "recommendation": "Use DATESINPERIOD for relative date ranges; DATESBETWEEN for fixed ranges",
    },
    "DAX018": {
        "name": "CALENDAR_FUNCTION_IN_MEASURE",
        "description": "CALENDAR/CALENDARAUTO should be in calculated table, not measure",
        "pattern": r"\b(CALENDAR|CALENDARAUTO)\s*\(",
        "severity": Severity.HIGH,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 10,
        "recommendation": "Move CALENDAR to a calculated table definition",
    },

    # Correctness Issues
    "DAX019": {
        "name": "BLANK_NOT_HANDLED",
        "description": "Measure may return unexpected results when source is blank",
        "pattern": None,  # Detected by absence of ISBLANK/BLANK checks
        "severity": Severity.MEDIUM,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 5,
        "recommendation": "Consider using IF(ISBLANK(...)) or COALESCE for explicit blank handling",
    },
    "DAX020": {
        "name": "RELATED_IN_MEASURE",
        "description": "RELATED requires row context; may cause unexpected behavior in measures",
        "pattern": r"\bRELATED\s*\(",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 8,
        "recommendation": "RELATED only works in row context; use LOOKUPVALUE or restructure logic",
    },
    "DAX021": {
        "name": "LOOKUPVALUE_SLOW",
        "description": "LOOKUPVALUE is slow for large tables; consider relationship-based approach",
        "pattern": r"\bLOOKUPVALUE\s*\(",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 8,
        "recommendation": "Create proper relationships instead of LOOKUPVALUE for better performance",
    },

    # Maintainability Issues
    "DAX022": {
        "name": "EXCESSIVE_MEASURE_LENGTH",
        "description": "Very long measure is hard to maintain and debug",
        "pattern": None,  # Detected by length > 1000 chars
        "threshold": 1000,
        "severity": Severity.MEDIUM,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 5,
        "recommendation": "Break into smaller helper measures or use VAR statements",
    },
    "DAX023": {
        "name": "NO_MEASURE_DESCRIPTION",
        "description": "Measure lacks description; reduces discoverability and maintainability",
        "pattern": None,  # Detected by empty description field
        "severity": Severity.LOW,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 1,
        "recommendation": "Add meaningful descriptions to measures for documentation",
    },
    "DAX024": {
        "name": "MULTIPLE_CALCULATE_MODIFIERS",
        "description": "Multiple modifiers in CALCULATE can be confusing and error-prone",
        "pattern": r"CALCULATE\s*\([^)]+,(.*?,){3,}",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 5,
        "recommendation": "Split complex CALCULATE into multiple measures for clarity",
    },
    "DAX025": {
        "name": "RANK_TOPN_COMBINATION",
        "description": "TOPN with RANKX can be inefficient; consider simpler patterns",
        "pattern": r"\bTOPN\s*\([^)]*RANKX",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 8,
        "recommendation": "Review if both TOPN and RANKX are needed; often one suffices",
    },
    # Case study rules (DAX026-028) - derived from 150x speedup analysis
    "DAX026": {
        "name": "GROUPBY_SUMX_CONDITIONAL",
        "description": "GROUPBY + SUMX inside IF/SWITCH creates heavy Formula Engine iteration",
        "pattern": None,  # Complex detection - see _check_rule
        "severity": Severity.HIGH,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 15,
        "recommendation": "Use grain-first approach: pre-aggregate with ADDCOLUMNS + SUMMARIZE, then apply conditional logic",
    },
    "DAX027": {
        "name": "MEASURE_CHAIN_DEPTH",
        "description": "Deep measure reference chain (>5 levels) causes repeated table scans",
        "pattern": None,  # Count-based detection - see _check_rule
        "threshold": 5,
        "severity": Severity.MEDIUM,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 10,
        "recommendation": "Collapse measure chain into single orchestrator measure using VAR statements",
    },
    "DAX028": {
        "name": "SUM_OF_RATIOS_PATTERN",
        "description": "Division inside SUMX/AVERAGEX produces incorrect weighted averages and poor performance",
        "pattern": None,  # Complex detection - see _check_rule
        "severity": Severity.MEDIUM,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 12,
        "recommendation": "Use ratio-of-sums: DIVIDE(SUMX(..., weight * numerator), SUMX(..., weight * denominator))",
    },
    "DAXC001": {
        "name": "ROW_ITERATION_OWNERSHIP_CARBON",
        "description": "Row-by-row SUMX over assets with inline ownership+carbon calc; likely missing grain-first materialization",
        "pattern": None,  # Complex detection - see _check_rule
        "severity": Severity.HIGH,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 14,
        "confirmed": True,
        "recommendation": "Materialize CarbonByAsset + OwnershipByAsset and join once (NATURALINNERJOIN), then SUMX over the joined table",
    },
}

# Calculation group rules
CALC_GROUP_RULES = {
    "CG001": {
        "name": "COMPLEX_DAX_IN_CALCULATION_ITEMS",
        "description": "Calculation item contains complex DAX (nested CALCULATE or many iterators)",
        "severity": Severity.HIGH,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 15,
        "recommendation": "Simplify calculation item logic or move complexity into base measures",
    },
    "CG002": {
        "name": "OVERLAPPING_PRECEDENCE_VALUES",
        "description": "Calculation groups share the same precedence value",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 8,
        "recommendation": "Assign unique precedence values to avoid ambiguous evaluation order",
    },
    "CG003": {
        "name": "SELECTEDMEASURE_OVERHEAD",
        "description": "SELECTEDMEASURE() used with IF without ISSELECTEDMEASURE guard",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_PERFORMANCE,
        "penalty": 5,
        "recommendation": "Wrap conditional branches with ISSELECTEDMEASURE to limit evaluation",
    },
    "CG004": {
        "name": "UNUSED_CALCULATION_ITEMS",
        "description": "Calculation item appears unused or usage cannot be verified",
        "severity": Severity.LOW,
        "category": Category.DAX_MAINTAINABILITY,
        "penalty": 3,
        "recommendation": "Remove unused calculation items or document their purpose",
    },
    "CG005": {
        "name": "MISSING_ISSELECTEDMEASURE_GUARD",
        "description": "Hardcoded measure references without ISSELECTEDMEASURE guard",
        "severity": Severity.MEDIUM,
        "category": Category.DAX_CORRECTNESS,
        "penalty": 8,
        "recommendation": "Use ISSELECTEDMEASURE to guard hardcoded measure references",
    },
}

# Model structure rules
MODEL_RULES = {
    "MDL001": {
        "name": "AUTO_DATE_TIME_ENABLED",
        "description": "Auto date/time creates redundant date tables per date column",
        "severity": Severity.CRITICAL,
        "category": Category.DATE_TABLE,
        "penalty": 15,
        "recommendation": "Disable auto date/time; create single shared date dimension",
    },
    "MDL002": {
        "name": "REFERENTIAL_INTEGRITY_VIOLATION",
        "description": "Missing keys in relationship causes incorrect aggregations",
        "severity": Severity.CRITICAL,
        "category": Category.RELATIONSHIP,
        "penalty": 5,  # per 10 violations
        "recommendation": "Fix source data or add 'Unknown' dimension member",
    },
    "MDL003": {
        "name": "HIGH_CARDINALITY_COLUMN",
        "description": "Column with very high cardinality consumes excessive memory",
        "severity": Severity.HIGH,
        "category": Category.CARDINALITY,
        "penalty": 12,
        "recommendation": "Consider splitting column or removing if unused",
    },
    "MDL004": {
        "name": "INEFFICIENT_ENCODING",
        "description": "Integer column using HASH encoding instead of VALUE",
        "severity": Severity.MEDIUM,
        "category": Category.ENCODING,
        "penalty": 5,
        "recommendation": "Check data range; may benefit from encoding hint",
    },
    "MDL005": {
        "name": "HIGH_DICTIONARY_RATIO",
        "description": "Dictionary dominates column storage (high cardinality indicator)",
        "severity": Severity.MEDIUM,
        "category": Category.CARDINALITY,
        "penalty": 5,
        "recommendation": "Review if column is needed; consider computed column",
    },
    "MDL006": {
        "name": "BIDIRECTIONAL_RELATIONSHIP",
        "description": "Bi-directional relationships can cause ambiguity and performance issues",
        "severity": Severity.MEDIUM,
        "category": Category.RELATIONSHIP,
        "penalty": 10,
        "recommendation": "Use single direction unless specifically required",
    },

    # Snowflake and Star Schema Issues
    "MDL007": {
        "name": "SNOWFLAKE_DIMENSION",
        "description": "Snowflaked dimension tables increase join complexity and reduce performance",
        "severity": Severity.MEDIUM,
        "category": Category.ARCHITECTURE,
        "penalty": 8,
        "recommendation": "Flatten dimension hierarchy into single table where possible",
    },
    "MDL008": {
        "name": "MISSING_DIMENSION_KEY",
        "description": "Dimension table lacks a proper unique key column",
        "severity": Severity.HIGH,
        "category": Category.RELATIONSHIP,
        "penalty": 10,
        "recommendation": "Add or identify a unique key column for reliable relationships",
    },
    "MDL009": {
        "name": "CALCULATED_COLUMN_HIGH_CARDINALITY",
        "description": "Calculated column has high cardinality; consider as measure",
        "severity": Severity.MEDIUM,
        "category": Category.CARDINALITY,
        "penalty": 8,
        "recommendation": "Move high-cardinality calculated columns to measures if aggregation is needed",
    },

    # Unused Objects
    "MDL010": {
        "name": "UNUSED_COLUMN",
        "description": "Column is not used in any measure, relationship, or visual",
        "severity": Severity.LOW,
        "category": Category.MODEL_SIZE,
        "penalty": 2,
        "recommendation": "Remove or hide unused columns to reduce model size",
    },
    "MDL011": {
        "name": "UNUSED_TABLE",
        "description": "Table is not referenced by any relationship or measure",
        "severity": Severity.MEDIUM,
        "category": Category.MODEL_SIZE,
        "penalty": 8,
        "recommendation": "Remove unused tables or document their purpose",
    },
    "MDL012": {
        "name": "HIDDEN_USED_COLUMN",
        "description": "Hidden column is used in calculations but not visible to users",
        "severity": Severity.INFO,
        "category": Category.ARCHITECTURE,
        "penalty": 1,
        "recommendation": "Verify hiding is intentional; document hidden dependencies",
    },

    # Date Table Issues
    "MDL013": {
        "name": "MULTIPLE_DATE_TABLES",
        "description": "Multiple date tables can cause confusion and inefficiency",
        "severity": Severity.MEDIUM,
        "category": Category.DATE_TABLE,
        "penalty": 8,
        "recommendation": "Consolidate to a single master date dimension table",
    },
    "MDL014": {
        "name": "DATE_TABLE_MISSING_COLUMNS",
        "description": "Date table lacks standard fiscal or calendar columns",
        "severity": Severity.LOW,
        "category": Category.DATE_TABLE,
        "penalty": 3,
        "recommendation": "Add Year, Quarter, Month, Week columns for time intelligence",
    },

    # Large Model Issues
    "MDL015": {
        "name": "LARGE_TABLE_NO_AGGREGATION",
        "description": "Very large table without aggregation or summary tables",
        "severity": Severity.HIGH,
        "category": Category.MODEL_SIZE,
        "penalty": 12,
        "recommendation": "Consider aggregation tables or DirectQuery for large fact tables",
    },
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class VPAXIssue:
    """Single diagnostic issue found in the VPAX model."""
    rule_id: str
    rule_name: str
    severity: str
    category: str
    description: str
    recommendation: str
    object_type: str  # table, column, measure, relationship
    object_name: str
    table_name: Optional[str] = None
    details: Optional[dict] = None
    code_snippet: Optional[str] = None
    reference: Optional[str] = None


@dataclass
class MeasureAnalysis:
    """Detailed analysis of a single measure."""
    name: str
    table: str
    expression: str
    length: int
    issues: list = field(default_factory=list)
    metrics: dict = field(default_factory=dict)
    severity_score: int = 0


@dataclass
class TableAnalysis:
    """Analysis of a single table."""
    name: str
    row_count: int
    column_count: int
    size_bytes: int
    is_hidden: bool
    is_date_table: bool
    is_local_date_table: bool
    issues: list = field(default_factory=list)


@dataclass
class ColumnAnalysis:
    """Analysis of a single column."""
    name: str
    table: str
    data_type: str
    encoding: str
    cardinality: int
    total_size: int
    dictionary_size: int
    dictionary_ratio: float
    is_key: bool
    issues: list = field(default_factory=list)


@dataclass
class RelationshipAnalysis:
    """Analysis of a relationship."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    is_active: bool
    cross_filter: str
    missing_keys: int
    issues: list = field(default_factory=list)


@dataclass
class QualityGate:
    """Quality gate status based on Torque Score thresholds."""
    status: str  # pass, warn, fail, deny
    label: str   # Peak Torque, Power Band, Stall Zone, Redline

    @classmethod
    def from_score(cls, score: int) -> "QualityGate":
        """Determine quality gate from Torque Score."""
        if score >= 90:
            return cls(status="pass", label="Peak Torque")
        elif score >= 70:
            return cls(status="warn", label="Power Band")
        elif score >= 50:
            return cls(status="fail", label="Stall Zone")
        else:
            return cls(status="deny", label="Redline")


@dataclass
class ModelSummary:
    """High-level model summary."""
    file_name: str
    analysis_timestamp: str
    total_tables: int
    total_columns: int
    total_measures: int
    total_relationships: int
    total_size_bytes: int
    local_date_table_count: int
    local_date_table_size_bytes: int
    actual_data_size_bytes: int

    # Issue counts by severity
    critical_count: int = 0
    high_count: int = 0
    medium_count: int = 0
    low_count: int = 0
    info_count: int = 0

    # Torque Score (0-100) - THE PRIMARY METRIC
    # Use this score for all new features and display
    torque_score: int = 100
    total_penalty: int = 0
    quality_gate: Optional[QualityGate] = None
    tech_debt_hours: int = 0  # Estimated hours to fix all issues


@dataclass
class DiagnosticReport:
    """Complete diagnostic report."""
    summary: ModelSummary
    tables: list
    columns: list
    measures: list
    relationships: list
    all_issues: list

    # Grouped for LLM consumption
    critical_issues: list = field(default_factory=list)
    high_issues: list = field(default_factory=list)
    medium_issues: list = field(default_factory=list)
    low_issues: list = field(default_factory=list)
    info_issues: list = field(default_factory=list)

    # Top offenders
    worst_measures: list = field(default_factory=list)
    largest_tables: list = field(default_factory=list)
    highest_cardinality_columns: list = field(default_factory=list)


# =============================================================================
# VPAX PARSER
# =============================================================================

class VPAXParser:
    """Parse VPAX files exported from DAX Studio."""

    def __init__(self, vpax_path: str):
        self.vpax_path = Path(vpax_path)
        self.dax_model = None
        self.dax_vpa_view = None
        self.model_bim = None

    def parse(self) -> dict:
        """Extract and parse VPAX contents."""
        with zipfile.ZipFile(self.vpax_path, 'r') as zf:
            # Parse DaxVpaView.json (main analysis data)
            if 'DaxVpaView.json' in zf.namelist():
                with zf.open('DaxVpaView.json') as f:
                    content = f.read().decode('utf-8-sig')
                    self.dax_vpa_view = json.loads(content)

            # Parse DaxModel.json (additional model info)
            if 'DaxModel.json' in zf.namelist():
                with zf.open('DaxModel.json') as f:
                    content = f.read().decode('utf-8-sig')
                    self.dax_model = json.loads(content)

            # Parse Model.bim (full model definition)
            if 'Model.bim' in zf.namelist():
                with zf.open('Model.bim') as f:
                    content = f.read().decode('utf-8-sig')
                    self.model_bim = json.loads(content)

        return {
            'vpa_view': self.dax_vpa_view,
            'dax_model': self.dax_model,
            'model_bim': self.model_bim,
        }


# =============================================================================
# DAX ANALYZER
# =============================================================================

class DAXAnalyzer:
    """Analyze DAX measures for anti-patterns and issues."""

    def __init__(self, dependency_result: Optional[DependencyAnalysisResult] = None, confirmed_only: bool = True):
        self.rules = DAX_RULES
        self._dependency_result = dependency_result
        self._confirmed_only = confirmed_only
        logger.debug("Initialized DAXAnalyzer with %d rules", len(self.rules))

    def analyze_measure(self, name: str, table: str, expression: str) -> MeasureAnalysis:
        """Analyze a single measure for all issues."""
        if not expression:
            logger.debug("Empty expression for measure '%s'", name)
            return MeasureAnalysis(name=name, table=table, expression="", length=0)

        analysis = MeasureAnalysis(
            name=name,
            table=table,
            expression=expression,
            length=len(expression),
        )

        expr_upper = expression.upper()

        # Metrics
        analysis.metrics = {
            'calculate_count': len(re.findall(r'\bCALCULATE\s*\(', expr_upper)),
            'if_count': len(re.findall(r'\bIF\s*\(', expr_upper)),
            'selectedvalue_count': len(re.findall(r'\bSELECTEDVALUE\s*\(', expr_upper)),
            'sumx_count': len(re.findall(r'\bSUMX\s*\(', expr_upper)),
            'filter_count': len(re.findall(r'\bFILTER\s*\(', expr_upper)),
            'has_var': 'VAR ' in expr_upper or 'VAR\n' in expr_upper,
            'has_divide': 'DIVIDE' in expr_upper,
            'has_division': bool(re.search(r'(?<![A-Za-z\/])\/(?![\/\*])', expression)),
            'has_comments': '//' in expression or '/*' in expression,
            'hardcoded_strings': len(re.findall(r'=\s*"[A-Za-z][A-Za-z0-9\s\-_]+"', expression)),
        }

        # Apply rules
        for rule_id, rule in self.rules.items():
            if self._confirmed_only and not rule.get("confirmed", False):
                continue
            issue = self._check_rule(rule_id, rule, name, table, expression, analysis.metrics)
            if issue:
                analysis.issues.append(issue)
                analysis.severity_score += self._severity_weight(rule['severity'])

        return analysis

    def _has_division_in_iterator_body(self, expression: str) -> tuple[bool, dict]:
        """Check if SUMX/AVERAGEX has division in expression argument (2nd arg).

        Uses the DAX parser to properly identify division operators or DIVIDE
        function calls within the second argument of iterator functions, avoiding
        false positives from division in the first (table) argument.

        Returns:
            Tuple of (violated: bool, details: dict with 'iterator' and 'line' if violated)
        """
        try:
            metadata = analyze_dax(expression)
        except Exception:
            return False, {}

        for func in metadata.iterator_functions:
            if func.name not in ('SUMX', 'AVERAGEX'):
                continue
            if func.arg_count < 2 or len(func.args) < 2:
                continue
            # Check second arg tokens for division operator or DIVIDE function
            for token in func.args[1]:
                if token.type == 'OPERATOR' and '/' in token.value:
                    return True, {'iterator': func.name, 'line': func.line}
                if token.type == 'FUNC' and token.value.upper() == 'DIVIDE':
                    return True, {'iterator': func.name, 'line': func.line}
        return False, {}

    def _get_measure_depth(self, measure_name: str) -> int:
        """Get dependency chain depth from pre-built graph.

        Uses the MeasureDependencyAnalyzer's computed depth, which represents
        the longest path from this measure to any leaf measure (depth 0).

        Returns:
            The depth of the measure in the dependency graph, or 0 if not found
            or no dependency graph was provided.
        """
        if not self._dependency_result:
            return 0
        key = measure_name.lower()
        node = self._dependency_result.nodes.get(key)
        return node.depth if node else 0

    def _check_rule(self, rule_id: str, rule: dict, name: str, table: str,
                    expression: str, metrics: dict) -> Optional[VPAXIssue]:
        """Check if a rule is violated."""
        expr_upper = expression.upper()
        violated = False
        details = {}

        # Pattern-based rules
        if rule.get('pattern'):
            if re.search(rule['pattern'], expression, re.IGNORECASE | re.MULTILINE):
                # Check exclusion pattern
                if rule.get('exclude_pattern'):
                    if re.search(rule['exclude_pattern'], expr_upper):
                        return None
                    # For division rule, check if DIVIDE is present
                    if rule_id == 'DAX004' and metrics.get('has_divide'):
                        return None
                violated = True

        # Count-based rules
        if rule_id == 'DAX003':  # Deep CALCULATE nesting
            if metrics['calculate_count'] >= rule['threshold']:
                violated = True
                details['calculate_count'] = metrics['calculate_count']

        elif rule_id == 'DAX005':  # CALCULATE nesting warning
            if 2 <= metrics['calculate_count'] < 4:
                violated = True
                details['calculate_count'] = metrics['calculate_count']

        elif rule_id == 'DAX006':  # Missing VAR in complex measure
            if len(expression) >= rule['threshold'] and not metrics['has_var']:
                violated = True
                details['length'] = len(expression)

        elif rule_id == 'DAX007':  # Nested IF
            if metrics['if_count'] >= rule['threshold']:
                violated = True
                details['if_count'] = metrics['if_count']

        elif rule_id == 'DAX008':  # Hardcoded values
            if metrics['hardcoded_strings'] >= rule['threshold']:
                violated = True
                details['hardcoded_count'] = metrics['hardcoded_strings']

        elif rule_id == 'DAX012':  # Excessive SELECTEDVALUE
            if metrics['selectedvalue_count'] >= rule['threshold']:
                violated = True
                details['selectedvalue_count'] = metrics['selectedvalue_count']

        # Case study rules (DAX026-028)
        elif rule_id == 'DAX026':  # GROUPBY + SUMX inside IF/SWITCH
            # Detect GROUPBY + SUMX combination inside conditional branches
            has_groupby = bool(re.search(r'\bGROUPBY\s*\(', expr_upper))
            has_sumx = bool(re.search(r'\bSUMX\s*\(', expr_upper))
            has_conditional = bool(re.search(r'\b(IF|SWITCH)\s*\(', expr_upper))
            if has_groupby and has_sumx and has_conditional:
                violated = True
                details['pattern'] = 'GROUPBY+SUMX in conditional'

        elif rule_id == 'DAX027':  # Measure chain depth > 5 (model-wide graph)
            # Use pre-built dependency graph for accurate depth calculation
            depth = self._get_measure_depth(name)
            if depth > rule.get('threshold', 5):
                violated = True
                details['depth'] = depth
                details['threshold'] = rule.get('threshold', 5)

        elif rule_id == 'DAX028':  # Sum-of-ratios pattern (parser-based detection)
            # Use DAX parser to detect division in iterator body (2nd argument)
            violated, details = self._has_division_in_iterator_body(expression)

        elif rule_id == 'DAXC001':  # Grain-first materialization missing in scope-switch asset iteration
            has_asset_sumx = bool(re.search(r"\bSUMX\s*\(\s*'GS Asset'\s*,", expr_upper))
            has_groupby = bool(re.search(r"\bGROUPBY\s*\(", expr_upper))
            has_scope_switch = bool(re.search(r"\bSCOPE_TYPE_CODE\b", expr_upper)) or bool(re.search(r"\bSELECTEDVALUE\s*\(\s*'SCOPE EMISSION TYPES'", expr_upper))
            has_carbon = bool(re.search(r"\bCARBON_SCOPE_[12]_TONNES_CO2E\b", expr_upper)) or bool(re.search(r"\bABSOLUTE_GHG_SCOPE_3_", expr_upper))
            has_revenue = bool(re.search(r"\bREVENUE_", expr_upper))
            has_ownership = bool(re.search(r"\bMV_OWNERSHIP\b", expr_upper)) or bool(re.search(r"\bBENCHMARK_WEIGHT_EOD\b", expr_upper)) or bool(re.search(r"\bMARKET_CAP_BASE\b", expr_upper)) or bool(re.search(r"\bEVIC_BASE\b", expr_upper))
            has_join_materialization = bool(re.search(r"\bNATURALINNERJOIN\b", expr_upper)) or bool(re.search(r"\bADDCOLUMNS\s*\(.*\"@OWNERSHIP\"", expr_upper, re.DOTALL)) or bool(re.search(r"\bADDCOLUMNS\s*\(.*\"@CARBON\"", expr_upper, re.DOTALL))

            if (has_asset_sumx or has_groupby) and has_scope_switch and (has_carbon or has_revenue) and not has_join_materialization:
                violated = True
                details.update({
                    "has_asset_sumx": has_asset_sumx,
                    "has_groupby": has_groupby,
                    "has_scope_switch": True,
                    "has_carbon": has_carbon,
                    "has_revenue": has_revenue,
                    "has_ownership": has_ownership,
                    "has_join_materialization": False,
                })

        if violated:
            return VPAXIssue(
                rule_id=rule_id,
                rule_name=rule['name'],
                severity=rule['severity'].value,
                category=rule['category'].value,
                description=rule['description'],
                recommendation=rule['recommendation'],
                object_type='measure',
                object_name=name,
                table_name=table,
                details=details,
                code_snippet=expression[:500] + ('...' if len(expression) > 500 else ''),
                reference=rule.get('reference'),
            )

        return None

    def _severity_weight(self, severity: Severity) -> int:
        """Weight for severity scoring."""
        return {
            Severity.CRITICAL: 10,
            Severity.HIGH: 5,
            Severity.MEDIUM: 3,
            Severity.LOW: 1,
            Severity.INFO: 0,
        }.get(severity, 0)


# =============================================================================
# MODEL ANALYZER
# =============================================================================

class ModelAnalyzer:
    """Analyze model structure for issues."""

    def __init__(self, vpa_data: dict):
        self.tables = vpa_data.get('Tables', [])
        self.columns = vpa_data.get('Columns', [])
        self.measures = vpa_data.get('Measures', [])
        self.relationships = vpa_data.get('Relationships', [])

    def analyze_tables(self) -> list:
        """Analyze all tables."""
        analyses = []

        for table in self.tables:
            name = table.get('TableName', '')
            is_local = 'LocalDateTable' in name
            is_template = table.get('IsTemplateDateTable', False)

            # Get columns for this table
            table_columns = [c for c in self.columns if c.get('TableName') == name]
            total_size = sum(c.get('TotalSize', 0) for c in table_columns)

            analysis = TableAnalysis(
                name=name,
                row_count=table.get('RowsCount', 0),
                column_count=len(table_columns),
                size_bytes=total_size,
                is_hidden=table.get('IsHidden', False),
                is_date_table=is_template or is_local,
                is_local_date_table=is_local,
            )

            # Check for issues
            if is_local:
                analysis.issues.append(VPAXIssue(
                    rule_id='MDL001',
                    rule_name='AUTO_DATE_TIME_ENABLED',
                    severity=Severity.CRITICAL.value,
                    category=Category.DATE_TABLE.value,
                    description=f"Auto-generated date table consuming {total_size/1024:.1f} KB",
                    recommendation="Disable auto date/time; create shared date dimension",
                    object_type='table',
                    object_name=name,
                ))

            analyses.append(analysis)

        return analyses

    def analyze_columns(self) -> list:
        """Analyze all columns."""
        analyses = []

        for col in self.columns:
            name = col.get('ColumnName', '')
            table = col.get('TableName', '')

            # Skip row number columns
            if 'RowNumber' in name:
                continue

            cardinality = col.get('ColumnCardinality', 0) or 0
            total_size = col.get('TotalSize', 0) or 0
            dict_size = col.get('DictionarySize', 0) or 0
            dict_ratio = dict_size / total_size if total_size > 0 else 0

            analysis = ColumnAnalysis(
                name=name,
                table=table,
                data_type=col.get('DataType', ''),
                encoding=col.get('Encoding', ''),
                cardinality=int(cardinality),
                total_size=total_size,
                dictionary_size=dict_size,
                dictionary_ratio=dict_ratio,
                is_key=col.get('IsKey', False),
            )

            # High cardinality check
            if cardinality >= THRESHOLDS['cardinality_critical']:
                analysis.issues.append(VPAXIssue(
                    rule_id='MDL003',
                    rule_name='HIGH_CARDINALITY_COLUMN',
                    severity=Severity.CRITICAL.value,
                    category=Category.CARDINALITY.value,
                    description=f"Column has {cardinality:,} unique values",
                    recommendation="Consider splitting or removing if unused",
                    object_type='column',
                    object_name=name,
                    table_name=table,
                    details={'cardinality': cardinality},
                ))
            elif cardinality >= THRESHOLDS['cardinality_warning']:
                analysis.issues.append(VPAXIssue(
                    rule_id='MDL003',
                    rule_name='HIGH_CARDINALITY_COLUMN',
                    severity=Severity.HIGH.value,
                    category=Category.CARDINALITY.value,
                    description=f"Column has {cardinality:,} unique values",
                    recommendation="Review if this cardinality is necessary",
                    object_type='column',
                    object_name=name,
                    table_name=table,
                    details={'cardinality': cardinality},
                ))

            # Encoding check
            if col.get('Encoding') == 'HASH' and col.get('DataType') in ['Int64', 'Double', 'Decimal']:
                if cardinality < 1000:  # Low cardinality integer with HASH
                    analysis.issues.append(VPAXIssue(
                        rule_id='MDL004',
                        rule_name='INEFFICIENT_ENCODING',
                        severity=Severity.MEDIUM.value,
                        category=Category.ENCODING.value,
                        description=f"Integer column using HASH encoding (cardinality: {cardinality})",
                        recommendation="May benefit from VALUE encoding; check data range",
                        object_type='column',
                        object_name=name,
                        table_name=table,
                    ))

            # Dictionary ratio check
            if dict_ratio >= THRESHOLDS['dictionary_ratio_warning'] and total_size > 10000:
                analysis.issues.append(VPAXIssue(
                    rule_id='MDL005',
                    rule_name='HIGH_DICTIONARY_RATIO',
                    severity=Severity.MEDIUM.value,
                    category=Category.CARDINALITY.value,
                    description=f"Dictionary is {dict_ratio*100:.1f}% of column size",
                    recommendation="High cardinality indicator; review necessity",
                    object_type='column',
                    object_name=name,
                    table_name=table,
                    details={'dictionary_ratio': dict_ratio},
                ))

            analyses.append(analysis)

        return analyses

    def analyze_relationships(self) -> list:
        """Analyze all relationships."""
        analyses = []

        for rel in self.relationships:
            from_table = rel.get('FromTableName', '')
            to_table = rel.get('ToTableName', '')
            missing_keys = rel.get('MissingKeys', 0) or 0
            cross_filter = rel.get('CrossFilteringBehavior', 1)

            # Decode cross filter behavior
            cross_filter_str = {1: 'Single', 2: 'Both'}.get(cross_filter, 'Unknown')

            analysis = RelationshipAnalysis(
                from_table=from_table,
                from_column=rel.get('FromFullColumnName', '').split('[')[-1].rstrip(']') if '[' in rel.get('FromFullColumnName', '') else '',
                to_table=to_table,
                to_column=rel.get('ToFullColumnName', '').split('[')[-1].rstrip(']') if '[' in rel.get('ToFullColumnName', '') else '',
                is_active=rel.get('IsActive', True),
                cross_filter=cross_filter_str,
                missing_keys=missing_keys,
            )

            # RI violation check
            if missing_keys > 0:
                severity = Severity.CRITICAL if missing_keys >= THRESHOLDS['ri_violations_critical'] else Severity.HIGH
                analysis.issues.append(VPAXIssue(
                    rule_id='MDL002',
                    rule_name='REFERENTIAL_INTEGRITY_VIOLATION',
                    severity=severity.value,
                    category=Category.RELATIONSHIP.value,
                    description=f"{missing_keys:,} missing keys in relationship",
                    recommendation="Fix source data or add 'Unknown' dimension member",
                    object_type='relationship',
                    object_name=f"{from_table} -> {to_table}",
                    details={'missing_keys': missing_keys},
                ))

            # Bi-directional check
            if cross_filter == 2:
                analysis.issues.append(VPAXIssue(
                    rule_id='MDL006',
                    rule_name='BIDIRECTIONAL_RELATIONSHIP',
                    severity=Severity.MEDIUM.value,
                    category=Category.RELATIONSHIP.value,
                    description="Bi-directional cross-filtering enabled",
                    recommendation="Use single direction unless specifically required",
                    object_type='relationship',
                    object_name=f"{from_table} <-> {to_table}",
                ))

            analyses.append(analysis)

        return analyses


# =============================================================================
# REPORT GENERATOR
# =============================================================================

class ReportGenerator:
    """Generate comprehensive diagnostic reports."""

    def __init__(self, vpax_path: str, confirmed_only: bool = True):
        self.vpax_path = Path(vpax_path)
        logger.info("Initializing ReportGenerator for %s", vpax_path)
        self.parser = VPAXParser(vpax_path)
        self.dax_analyzer = DAXAnalyzer(confirmed_only=confirmed_only)
        self._confirmed_only = confirmed_only

    def _analyze_calculation_groups(self, vpa_view: dict) -> list:
        """Analyze calculation groups and items for CG rules."""
        calculation_groups = vpa_view.get("CalculationGroups") or vpa_view.get("CalculationGroup") or []
        if isinstance(calculation_groups, dict):
            calculation_groups = [calculation_groups]

        issues = []
        precedence_map = {}
        complex_functions = [
            "SUMX",
            "AVERAGEX",
            "MAXX",
            "MINX",
            "COUNTX",
            "FILTER",
            "TOPN",
            "RANKX",
            "ADDCOLUMNS",
            "SUMMARIZE",
            "GROUPBY",
            "CONCATENATEX",
        ]

        for group in calculation_groups:
            group_name = (
                group.get("CalculationGroupName")
                or group.get("Name")
                or group.get("TableName")
                or "Unknown Calculation Group"
            )
            precedence = group.get("Precedence")
            if precedence is None:
                precedence = group.get("CalculationGroupPrecedence")

            if precedence is not None:
                precedence_map.setdefault(precedence, []).append(group_name)

            items = group.get("CalculationItems") or group.get("CalculationItem") or []
            if isinstance(items, dict):
                items = [items]

            for item in items:
                item_name = (
                    item.get("CalculationItemName")
                    or item.get("Name")
                    or item.get("ItemName")
                    or "Unknown Calculation Item"
                )
                expression = (
                    item.get("Expression")
                    or item.get("CalculationItemExpression")
                    or item.get("ExpressionText")
                    or ""
                )
                expr_upper = expression.upper()

                calculate_count = len(re.findall(r"\bCALCULATE\s*\(", expr_upper))
                complex_count = sum(
                    len(re.findall(rf"\b{func}\s*\(", expr_upper))
                    for func in complex_functions
                )
                if calculate_count >= 2 or complex_count > 3:
                    issues.append(VPAXIssue(
                        rule_id="CG001",
                        rule_name=CALC_GROUP_RULES["CG001"]["name"],
                        severity=CALC_GROUP_RULES["CG001"]["severity"].value,
                        category=CALC_GROUP_RULES["CG001"]["category"].value,
                        description=CALC_GROUP_RULES["CG001"]["description"],
                        recommendation=CALC_GROUP_RULES["CG001"]["recommendation"],
                        object_type="calculation_item",
                        object_name=f"{group_name}.{item_name}",
                        table_name=group_name,
                        details={
                            "calculate_count": calculate_count,
                            "complex_function_count": complex_count,
                        },
                        code_snippet=expression[:500] + ("..." if len(expression) > 500 else ""),
                    ))

                has_selectedmeasure = bool(re.search(r"\bSELECTEDMEASURE\s*\(", expr_upper))
                has_if = bool(re.search(r"\bIF\s*\(", expr_upper))
                has_guard = bool(re.search(r"\bISSELECTEDMEASURE\s*\(", expr_upper))
                if has_selectedmeasure and has_if and not has_guard:
                    issues.append(VPAXIssue(
                        rule_id="CG003",
                        rule_name=CALC_GROUP_RULES["CG003"]["name"],
                        severity=CALC_GROUP_RULES["CG003"]["severity"].value,
                        category=CALC_GROUP_RULES["CG003"]["category"].value,
                        description=CALC_GROUP_RULES["CG003"]["description"],
                        recommendation=CALC_GROUP_RULES["CG003"]["recommendation"],
                        object_type="calculation_item",
                        object_name=f"{group_name}.{item_name}",
                        table_name=group_name,
                        code_snippet=expression[:500] + ("..." if len(expression) > 500 else ""),
                    ))

                expr_no_table = re.sub(r"'[^']+'\s*\[[^\]]+\]", "", expression)
                expr_no_funcs = re.sub(
                    r"\b(SELECTEDMEASURE|ISSELECTEDMEASURE|SELECTEDMEASURENAME)\s*\(\s*\)",
                    "",
                    expr_no_table,
                    flags=re.IGNORECASE,
                )
                unqualified_refs = re.findall(r"\[[^\]]+\]", expr_no_funcs)
                if unqualified_refs and not has_guard:
                    issues.append(VPAXIssue(
                        rule_id="CG005",
                        rule_name=CALC_GROUP_RULES["CG005"]["name"],
                        severity=CALC_GROUP_RULES["CG005"]["severity"].value,
                        category=CALC_GROUP_RULES["CG005"]["category"].value,
                        description=CALC_GROUP_RULES["CG005"]["description"],
                        recommendation=CALC_GROUP_RULES["CG005"]["recommendation"],
                        object_type="calculation_item",
                        object_name=f"{group_name}.{item_name}",
                        table_name=group_name,
                        details={"measure_refs": unqualified_refs},
                        code_snippet=expression[:500] + ("..." if len(expression) > 500 else ""),
                    ))

                usage_flag = item.get("IsUsed")
                if usage_flag is None:
                    usage_flag = item.get("IsReferenced")
                if usage_flag is None:
                    usage_flag = item.get("IsInUse")

                if usage_flag is False or usage_flag is None:
                    usage_status = "unused" if usage_flag is False else "potentially_unused"
                    issues.append(VPAXIssue(
                        rule_id="CG004",
                        rule_name=CALC_GROUP_RULES["CG004"]["name"],
                        severity=CALC_GROUP_RULES["CG004"]["severity"].value,
                        category=CALC_GROUP_RULES["CG004"]["category"].value,
                        description=CALC_GROUP_RULES["CG004"]["description"],
                        recommendation=CALC_GROUP_RULES["CG004"]["recommendation"],
                        object_type="calculation_item",
                        object_name=f"{group_name}.{item_name}",
                        table_name=group_name,
                        details={"usage_status": usage_status},
                        code_snippet=expression[:500] + ("..." if len(expression) > 500 else ""),
                    ))

        for precedence, group_names in precedence_map.items():
            if len(group_names) > 1:
                for group_name in group_names:
                    issues.append(VPAXIssue(
                        rule_id="CG002",
                        rule_name=CALC_GROUP_RULES["CG002"]["name"],
                        severity=CALC_GROUP_RULES["CG002"]["severity"].value,
                        category=CALC_GROUP_RULES["CG002"]["category"].value,
                        description=CALC_GROUP_RULES["CG002"]["description"],
                        recommendation=CALC_GROUP_RULES["CG002"]["recommendation"],
                        object_type="calculation_group",
                        object_name=group_name,
                        details={
                            "precedence": precedence,
                            "overlapping_groups": group_names,
                        },
                    ))

        return issues

    def generate(self) -> DiagnosticReport:
        """Generate complete diagnostic report."""
        logger.info("Starting VPAX analysis for %s", self.vpax_path)
        start_time = time.time()

        # Parse VPAX
        logger.debug("Parsing VPAX file...")
        data = self.parser.parse()
        vpa_view = data['vpa_view']
        parse_duration = time.time() - start_time
        logger.info("VPAX parsed in %.2fs", parse_duration)

        # Initialize analyzers
        model_analyzer = ModelAnalyzer(vpa_view)

        # Analyze components
        logger.debug("Analyzing tables...")
        table_analyses = model_analyzer.analyze_tables()
        logger.debug("Analyzing columns...")
        column_analyses = model_analyzer.analyze_columns()
        logger.debug("Analyzing relationships...")
        relationship_analyses = model_analyzer.analyze_relationships()

        # Build dependency graph for all measures (needed for DAX027)
        measures_for_graph = [
            {'name': m.get('MeasureName', ''),
             'table': m.get('TableName', ''),
             'expression': m.get('MeasureExpression', '')}
            for m in vpa_view.get('Measures', [])
        ]
        dep_analyzer = MeasureDependencyAnalyzer()
        dependency_result = dep_analyzer.analyze(measures_for_graph)
        logger.debug(
            "Built dependency graph: %d measures, max_depth=%d",
            dependency_result.total_measures,
            dependency_result.max_depth
        )

        # Create DAXAnalyzer with dependency result for accurate DAX027 detection
        self.dax_analyzer = DAXAnalyzer(dependency_result=dependency_result, confirmed_only=self._confirmed_only)

        # Analyze measures
        logger.info("Analyzing %d measures...", len(vpa_view.get('Measures', [])))
        measure_analyses = []
        for m in vpa_view.get('Measures', []):
            analysis = self.dax_analyzer.analyze_measure(
                name=m.get('MeasureName', ''),
                table=m.get('TableName', ''),
                expression=m.get('MeasureExpression', ''),
            )
            measure_analyses.append(analysis)

        calc_group_issues = self._analyze_calculation_groups(vpa_view)

        # Collect all issues
        all_issues = []
        for t in table_analyses:
            all_issues.extend(t.issues)
        for c in column_analyses:
            all_issues.extend(c.issues)
        for r in relationship_analyses:
            all_issues.extend(r.issues)
        for m in measure_analyses:
            all_issues.extend(m.issues)
        all_issues.extend(calc_group_issues)

        # Calculate summary statistics
        local_date_tables = [t for t in table_analyses if t.is_local_date_table]
        local_date_size = sum(t.size_bytes for t in local_date_tables)
        total_size = sum(c.total_size for c in column_analyses)
        actual_data_size = total_size - local_date_size

        # Count issues by severity
        severity_counts = {s.value: 0 for s in Severity}
        for issue in all_issues:
            severity_counts[issue.severity] += 1

        # Calculate Torque Score using penalty matrix
        # Aggregate penalties by rule (capped per rule to prevent runaway scores)
        penalty_by_rule = {}
        for issue in all_issues:
            rule_id = issue.rule_id
            if rule_id not in penalty_by_rule:
                penalty_by_rule[rule_id] = 0

            # Get penalty from rule definition
            rule_def = DAX_RULES.get(rule_id) or MODEL_RULES.get(rule_id) or CALC_GROUP_RULES.get(rule_id)
            penalty = rule_def.get("penalty", 5) if rule_def else 5

            # Cap penalty per rule at 40 to prevent single issue dominating
            penalty_by_rule[rule_id] = min(40, penalty_by_rule[rule_id] + penalty)

        total_penalty = sum(penalty_by_rule.values())
        torque_score = max(0, 100 - total_penalty)
        quality_gate = QualityGate.from_score(torque_score)

        # Calculate tech debt hours based on severity
        # Critical: ~2h, High: ~1h, Medium: ~0.5h, Low: ~0.25h
        # Use float math then round to preserve precision
        tech_debt_hours_raw = (
            severity_counts['critical'] * 2.0 +
            severity_counts['high'] * 1.0 +
            severity_counts['medium'] * 0.5 +
            severity_counts['low'] * 0.25
        )
        tech_debt_hours = round(tech_debt_hours_raw)

        summary = ModelSummary(
            file_name=self.vpax_path.name,
            analysis_timestamp=datetime.now().isoformat(),
            total_tables=len([t for t in table_analyses if not t.is_local_date_table]),
            total_columns=len(column_analyses),
            total_measures=len(measure_analyses),
            total_relationships=len(relationship_analyses),
            total_size_bytes=total_size,
            local_date_table_count=len(local_date_tables),
            local_date_table_size_bytes=local_date_size,
            actual_data_size_bytes=actual_data_size,
            critical_count=severity_counts['critical'],
            high_count=severity_counts['high'],
            medium_count=severity_counts['medium'],
            low_count=severity_counts['low'],
            info_count=severity_counts['info'],
            torque_score=torque_score,
            total_penalty=total_penalty,
            quality_gate=quality_gate,
            tech_debt_hours=tech_debt_hours,
        )

        # Group issues by severity
        report = DiagnosticReport(
            summary=summary,
            tables=[asdict(t) for t in table_analyses],
            columns=[asdict(c) for c in column_analyses if c.issues],  # Only problematic columns
            measures=[asdict(m) for m in measure_analyses],
            relationships=[asdict(r) for r in relationship_analyses],
            all_issues=[asdict(i) for i in all_issues],
            critical_issues=[asdict(i) for i in all_issues if i.severity == 'critical'],
            high_issues=[asdict(i) for i in all_issues if i.severity == 'high'],
            medium_issues=[asdict(i) for i in all_issues if i.severity == 'medium'],
            low_issues=[asdict(i) for i in all_issues if i.severity == 'low'],
            info_issues=[asdict(i) for i in all_issues if i.severity == 'info'],
            worst_measures=[asdict(m) for m in sorted(measure_analyses, key=lambda x: x.severity_score, reverse=True)[:20]],
            largest_tables=[asdict(t) for t in sorted(table_analyses, key=lambda x: x.size_bytes, reverse=True)[:20]],
            highest_cardinality_columns=[asdict(c) for c in sorted(column_analyses, key=lambda x: x.cardinality, reverse=True)[:20]],
        )

        total_duration = time.time() - start_time
        logger.info(
            "VPAX analysis complete: score=%d, issues=%d, tables=%d, measures=%d, duration=%.2fs",
            torque_score, len(all_issues), len(table_analyses), len(measure_analyses), total_duration
        )

        return report

    def to_json(self, report: DiagnosticReport) -> str:
        """Convert report to JSON."""
        return json.dumps(asdict(report), indent=2, default=str)

    def to_llm_prompt(self, report: DiagnosticReport) -> str:
        """Generate LLM-ready prompt with structured data."""
        summary = report.summary

        prompt = f"""# Power BI Model Diagnostic Analysis

## Model: {summary.file_name}
## Analyzed: {summary.analysis_timestamp}

---

## TORQUE SCORE

| Metric | Score | Status |
|--------|-------|--------|
| **Torque Score** | **{summary.torque_score}/100** | **{"CRITICAL" if summary.torque_score < 50 else "WARNING" if summary.torque_score < 80 else "GOOD"}** |

---

## MODEL STATISTICS

- Tables: {summary.total_tables} (excluding {summary.local_date_table_count} auto-generated date tables)
- Columns: {summary.total_columns}
- Measures: {summary.total_measures}
- Relationships: {summary.total_relationships}
- Total Size: {summary.total_size_bytes / (1024*1024):.2f} MB
- Actual Data: {summary.actual_data_size_bytes / (1024*1024):.2f} MB
- Wasted (LocalDateTables): {summary.local_date_table_size_bytes / (1024*1024):.2f} MB ({summary.local_date_table_size_bytes / summary.total_size_bytes * 100 if summary.total_size_bytes > 0 else 0:.1f}%)

---

## ISSUE COUNTS BY SEVERITY

| Severity | Count |
|----------|-------|
| Critical | {summary.critical_count} |
| High | {summary.high_count} |
| Medium | {summary.medium_count} |
| Low | {summary.low_count} |
| Info | {summary.info_count} |

---

## CRITICAL ISSUES

"""
        for issue in report.critical_issues[:20]:
            prompt += f"""
### [{issue['rule_id']}] {issue['rule_name']}
- **Object:** {issue['object_type']}: `{issue['object_name']}`
- **Description:** {issue['description']}
- **Recommendation:** {issue['recommendation']}
"""
            if issue.get('code_snippet'):
                prompt += f"- **Code:** `{issue['code_snippet'][:200]}...`\n"
            if issue.get('reference'):
                prompt += f"- **Reference:** {issue['reference']}\n"

        prompt += """
---

## HIGH PRIORITY ISSUES

"""
        for issue in report.high_issues[:15]:
            prompt += f"""
### [{issue['rule_id']}] {issue['rule_name']}
- **Object:** {issue['object_type']}: `{issue['object_name']}`
- **Description:** {issue['description']}
- **Recommendation:** {issue['recommendation']}
"""

        prompt += """
---

## TOP 10 WORST MEASURES (by severity score)

"""
        for i, m in enumerate(report.worst_measures[:10], 1):
            if m['severity_score'] > 0:
                prompt += f"""
### #{i}: `{m['name']}` (Score: {m['severity_score']})
- **Table:** {m['table']}
- **Length:** {m['length']} chars
- **Issues:** {len(m['issues'])}
"""
                for issue in m['issues'][:3]:
                    prompt += f"  - {issue['rule_name']}: {issue['description']}\n"

        prompt += """
---

## RECOMMENDED ACTIONS

Based on this analysis, prioritize:

1. **CRITICAL issues first** - These are performance killers or correctness problems
2. **Storage optimization** - Address auto-generated date tables if present
3. **DAX refactoring** - Fix anti-patterns in worst measures
4. **Data quality** - Resolve referential integrity violations

---

*Analysis performed using DAX anti-pattern detection*
"""
        return prompt

    def to_markdown(self, report: DiagnosticReport) -> str:
        """Generate detailed Markdown report."""
        return self.to_llm_prompt(report)  # Same format works for both


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python vpax_analyzer.py <path_to_vpax_file> [--json|--markdown|--llm]")
        print("\nOutputs:")
        print("  --json      Full JSON diagnostic data")
        print("  --markdown  Markdown report (default)")
        print("  --llm       LLM-optimized prompt format")
        sys.exit(1)

    vpax_path = sys.argv[1]
    output_format = sys.argv[2] if len(sys.argv) > 2 else '--markdown'

    if not Path(vpax_path).exists():
        print(f"Error: File not found: {vpax_path}")
        sys.exit(1)

    # Generate report
    generator = ReportGenerator(vpax_path)
    report = generator.generate()

    # Output in requested format
    if output_format == '--json':
        print(generator.to_json(report))
    elif output_format == '--llm':
        print(generator.to_llm_prompt(report))
    else:
        print(generator.to_markdown(report))


if __name__ == '__main__':
    main()
