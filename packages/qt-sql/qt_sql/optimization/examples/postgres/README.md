# PostgreSQL Gold Examples

Database-specific verified optimizations for PostgreSQL.

**DO NOT MIX** with DuckDB examples in parent directory.

## Verified Examples

| ID | Name | Speedup | Dataset |
|----|------|---------|---------|
| early_filter_decorrelate | Early Filter + Decorrelate | 1.13x | DSB SF10 Q1 |

## Key Learnings

PostgreSQL optimizer is different from DuckDB:
- Window functions (AVG OVER PARTITION) don't help as much
- Multi-scan rewrites cause regressions
- Early filter pushdown + decorrelate is the winning pattern

## Usage

These examples should only be used when `db_type == "postgres"`.
