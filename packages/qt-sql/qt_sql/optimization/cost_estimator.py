"""Cost Estimator for SQL Optimization Savings.

Estimates annualized savings based on detected opportunities and database-specific
cost models. Returns savings bands (low/mid/high) rather than exact figures.

Usage:
    from qt_sql.optimization.cost_estimator import estimate_savings, CostModel

    # With execution time from EXPLAIN ANALYZE
    savings = estimate_savings(
        opportunities=detected_opps,
        execution_time_ms=150,
        daily_runs=100,
        cost_model=CostModel.DUCKDB_LOCAL
    )

    # Without execution time (uses heuristics)
    savings = estimate_savings(
        opportunities=detected_opps,
        query_complexity=query_structure,
        daily_runs=100,
        cost_model=CostModel.SNOWFLAKE_MEDIUM
    )
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CostModel(str, Enum):
    """Database cost models with different pricing assumptions."""

    # DuckDB - use Snowflake Medium as proxy (realistic cloud DW cost)
    DUCKDB = "duckdb"

    # PostgreSQL - use Snowflake Small as proxy
    POSTGRES = "postgres"

    # Snowflake tiers
    SNOWFLAKE_XSMALL = "snowflake_xsmall"  # ~$2/hr
    SNOWFLAKE_SMALL = "snowflake_small"    # ~$4/hr
    SNOWFLAKE_MEDIUM = "snowflake_medium"  # ~$8/hr
    SNOWFLAKE_LARGE = "snowflake_large"    # ~$16/hr

    # Other clouds
    BIGQUERY_ON_DEMAND = "bigquery_on_demand"  # $5/TB scanned
    REDSHIFT_DC2 = "redshift_dc2"  # ~$0.25/hr per node

    # Generic cloud estimate
    GENERIC = "generic"


# Cost per second in USD for each model
# Based on Snowflake pricing: $3/credit, credits/hr varies by warehouse size
COST_PER_SECOND: dict[CostModel, float] = {
    # DuckDB - use Snowflake Medium pricing as realistic cloud DW proxy
    CostModel.DUCKDB: 0.0067,        # $24/hr / 3600 (Snowflake Medium)

    # PostgreSQL - use Snowflake Small pricing
    CostModel.POSTGRES: 0.0033,      # $12/hr / 3600 (Snowflake Small)

    # Snowflake credits ($3/credit)
    CostModel.SNOWFLAKE_XSMALL: 0.0017,   # $6/hr / 3600 (1 credit/hr)
    CostModel.SNOWFLAKE_SMALL: 0.0033,    # $12/hr / 3600 (2 credits/hr)
    CostModel.SNOWFLAKE_MEDIUM: 0.0067,   # $24/hr / 3600 (4 credits/hr)
    CostModel.SNOWFLAKE_LARGE: 0.0133,    # $48/hr / 3600 (8 credits/hr)

    # BigQuery - assume 10GB scanned per query, $5/TB
    CostModel.BIGQUERY_ON_DEMAND: 0.00014,  # $0.05 per query / 360s avg

    # Redshift - 2 node dc2.large cluster
    CostModel.REDSHIFT_DC2: 0.00028,  # $1/hr / 3600

    # Generic cloud - Snowflake Small equivalent
    CostModel.GENERIC: 0.0033,  # $12/hr / 3600
}


# Map dialect strings to cost models
DIALECT_TO_COST_MODEL: dict[str, CostModel] = {
    "duckdb": CostModel.DUCKDB,
    "postgres": CostModel.POSTGRES,
    "postgresql": CostModel.POSTGRES,
    "snowflake": CostModel.SNOWFLAKE_MEDIUM,
    "bigquery": CostModel.BIGQUERY_ON_DEMAND,
    "redshift": CostModel.REDSHIFT_DC2,
    "generic": CostModel.GENERIC,
    "tsql": CostModel.GENERIC,
    "mysql": CostModel.GENERIC,
}


def get_cost_model_for_dialect(dialect: str) -> CostModel:
    """Get appropriate cost model for a SQL dialect."""
    return DIALECT_TO_COST_MODEL.get(dialect.lower(), CostModel.GENERIC)


# Production scale factor - queries in prod operate on much larger data than dev/test
PRODUCTION_DATA_SCALE = 100

# Typical daily query frequency for production workloads
PRODUCTION_DAILY_RUNS = 24


# Opportunity weights - higher = more impactful
# Scale: 1-10, where 10 = transformative improvement
OPPORTUNITY_WEIGHTS: dict[str, int] = {
    # High-value (proven significant improvements)
    "QT-OPT-001": 8,   # or_to_union - major for OR-heavy queries
    "QT-OPT-002": 9,   # correlated_to_cte - O(n²) to O(n)
    "QT-OPT-003": 7,   # date_cte_isolate - partition pruning
    "QT-OPT-004": 8,   # push_pred - filter before aggregate
    "QT-OPT-005": 7,   # consolidate_scans - reduce I/O

    # Standard (moderate improvements)
    "QT-OPT-006": 6,   # multi_push_pred
    "QT-OPT-007": 5,   # materialize_cte
    "QT-OPT-008": 5,   # flatten_subq
    "QT-OPT-009": 4,   # reorder_join
    "QT-OPT-010": 3,   # inline_cte
    "QT-OPT-011": 2,   # remove_redundant
}

# Improvement factor ranges based on weight
# Weight -> (low_factor, mid_factor, high_factor)
# Factor represents potential time reduction (0.3 = 30% faster)
WEIGHT_TO_IMPROVEMENT: dict[int, tuple[float, float, float]] = {
    10: (0.50, 0.65, 0.80),  # 50-80% faster
    9:  (0.40, 0.55, 0.70),  # 40-70% faster
    8:  (0.30, 0.45, 0.60),  # 30-60% faster
    7:  (0.25, 0.35, 0.50),  # 25-50% faster
    6:  (0.20, 0.30, 0.40),  # 20-40% faster
    5:  (0.15, 0.25, 0.35),  # 15-35% faster
    4:  (0.10, 0.20, 0.30),  # 10-30% faster
    3:  (0.08, 0.15, 0.25),  # 8-25% faster
    2:  (0.05, 0.10, 0.20),  # 5-20% faster
    1:  (0.02, 0.05, 0.10),  # 2-10% faster
}


@dataclass
class SavingsEstimate:
    """Estimated annual savings from optimization."""

    # Savings band in USD
    low: float
    mid: float
    high: float

    # Display string
    band_display: str  # e.g., "$500 - $2,000"

    # Component breakdown
    total_weight: int
    opportunity_count: int
    daily_cost_estimate: float
    annual_cost_estimate: float

    # Improvement factors used
    improvement_low: float
    improvement_high: float

    def to_dict(self) -> dict:
        return {
            "low": round(self.low, 2),
            "mid": round(self.mid, 2),
            "high": round(self.high, 2),
            "band_display": self.band_display,
            "total_weight": self.total_weight,
            "opportunity_count": self.opportunity_count,
            "daily_cost": round(self.daily_cost_estimate, 4),
            "annual_cost": round(self.annual_cost_estimate, 2),
        }


def estimate_savings(
    opportunities: list,
    execution_time_ms: Optional[float] = None,
    query_complexity: Optional[dict] = None,
    daily_runs: int = PRODUCTION_DAILY_RUNS,
    cost_model: CostModel = CostModel.DUCKDB,
    dialect: Optional[str] = None,
) -> SavingsEstimate:
    """Estimate annualized savings from detected opportunities.

    Args:
        opportunities: List of detected opportunities (need .pattern_id attribute)
        execution_time_ms: Actual execution time if available (from EXPLAIN ANALYZE)
        query_complexity: Query structure dict (cte_count, join_count, etc.) for heuristics
        daily_runs: How many times this query runs per day
        cost_model: Database cost model to use
        dialect: SQL dialect - if provided, overrides cost_model with dialect-appropriate model

    Returns:
        SavingsEstimate with low/mid/high bands
    """
    # Use dialect to determine cost model if provided
    if dialect:
        cost_model = get_cost_model_for_dialect(dialect)

    if not opportunities:
        return SavingsEstimate(
            low=0, mid=0, high=0,
            band_display="$0",
            total_weight=0,
            opportunity_count=0,
            daily_cost_estimate=0,
            annual_cost_estimate=0,
            improvement_low=0,
            improvement_high=0,
        )

    # Calculate total weight from opportunities
    total_weight = 0
    for opp in opportunities:
        pattern_id = getattr(opp, 'pattern_id', None) or opp.get('pattern_id', '')
        weight = OPPORTUNITY_WEIGHTS.get(pattern_id, 3)
        total_weight += weight

    # Cap total weight at 10 (diminishing returns)
    effective_weight = min(total_weight, 10)

    # Get improvement factors for this weight
    low_factor, mid_factor, high_factor = WEIGHT_TO_IMPROVEMENT.get(
        effective_weight,
        WEIGHT_TO_IMPROVEMENT[5]  # default to middle
    )

    # Estimate execution time if not provided
    if execution_time_ms is None:
        execution_time_ms = _estimate_execution_time(query_complexity)

    # Apply production data scale - queries in prod run on 100x more data
    execution_time_ms = execution_time_ms * PRODUCTION_DATA_SCALE

    # Calculate costs using Snowflake-equivalent pricing
    cost_per_sec = COST_PER_SECOND.get(cost_model, COST_PER_SECOND[CostModel.GENERIC])
    execution_time_sec = execution_time_ms / 1000

    # Current costs
    cost_per_run = execution_time_sec * cost_per_sec
    daily_cost = cost_per_run * daily_runs
    annual_cost = daily_cost * 365

    # Savings = current - optimized
    savings_low = annual_cost * low_factor
    savings_mid = annual_cost * mid_factor
    savings_high = annual_cost * high_factor

    # Format display band
    band_display = _format_savings_band(savings_low, savings_high)

    return SavingsEstimate(
        low=savings_low,
        mid=savings_mid,
        high=savings_high,
        band_display=band_display,
        total_weight=total_weight,
        opportunity_count=len(opportunities),
        daily_cost_estimate=daily_cost,
        annual_cost_estimate=annual_cost,
        improvement_low=low_factor,
        improvement_high=high_factor,
    )


def _estimate_execution_time(query_complexity: Optional[dict]) -> float:
    """Estimate execution time from query complexity when actual time unavailable.

    Returns estimated milliseconds.
    """
    if not query_complexity:
        return 500  # Default: 500ms for unknown queries

    # Base time
    base_ms = 100

    # Add complexity factors
    cte_count = query_complexity.get('cte_count', 0)
    join_count = query_complexity.get('join_count', 0)
    subquery_count = query_complexity.get('subquery_count', 0)
    table_count = query_complexity.get('table_count', 0)

    # Heuristic: more complex = longer
    complexity_ms = (
        cte_count * 50 +
        join_count * 100 +
        subquery_count * 150 +
        table_count * 30
    )

    return base_ms + complexity_ms


def _format_savings_band(low: float, high: float) -> str:
    """Format savings as a readable band string."""
    if high < 1:
        return "< $1"
    elif high < 10:
        return f"${int(low)} - ${int(high)}"
    elif high < 100:
        return f"${int(low/10)*10} - ${int(high/10)*10}"
    elif high < 1000:
        return f"${int(low/100)*100} - ${int(high/100)*100}"
    elif high < 10000:
        low_k = low / 1000
        high_k = high / 1000
        return f"${low_k:.1f}K - ${high_k:.1f}K"
    else:
        low_k = int(low / 1000)
        high_k = int(high / 1000)
        return f"${low_k}K - ${high_k}K"


def get_opportunity_weight(pattern_id: str) -> int:
    """Get the weight score for an opportunity pattern."""
    return OPPORTUNITY_WEIGHTS.get(pattern_id, 3)


def get_weight_description(weight: int) -> str:
    """Get human-readable description for a weight score."""
    if weight >= 9:
        return "Critical"
    elif weight >= 7:
        return "High"
    elif weight >= 5:
        return "Medium"
    elif weight >= 3:
        return "Low"
    else:
        return "Minor"
