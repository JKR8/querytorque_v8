"""Workload Mode — fleet-level optimization of 100+ queries.

Wraps the single-query pipeline with:
- Triage scoring (pain × frequency × tractability)
- Fleet-level pattern detection (shared scans, config, statistics)
- Tiered optimization (Tier 1 fleet → Tier 2 light → Tier 3 deep)
- Iterative downsizing loop
- Scorecard with business case
"""
