"""Tests for config_boost EXPLAIN parsing rules.

Run:
  cd <repo-root>
  PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m pytest packages/qt-sql/qt_sql/tests/test_config_boost.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Bootstrap paths
PROJECT_ROOT = Path(__file__).resolve().parents[4]
for p in ["packages/qt-shared", "packages/qt-sql", "."]:
    full = str(PROJECT_ROOT / p)
    if full not in sys.path:
        sys.path.insert(0, full)

from qt_sql.config_boost import (
    propose_config_from_explain,
    _rule_work_mem,
    _rule_nestloop,
    _rule_parallelism,
    _rule_jit,
    _rule_random_page_cost,
    _rule_join_collapse,
    _round_to_power_of_2,
)

# =========================================================================
# Test fixtures — synthetic EXPLAIN text patterns
# =========================================================================

HASH_SPILL_EXPLAIN = """\
Hash Join (cost=1000..5000 rows=100)
  Hash Cond: (a.id = b.id)
  ->  Seq Scan on orders (cost=0..500 rows=10000)
  ->  Hash (cost=500..500 rows=500)
        Buckets: 1024  Batches: 8  Memory Usage: 512kB
        Peak Memory: 8192kB
"""

HASH_HIGH_MEMORY_NO_SPILL = """\
Hash Join (cost=1000..5000 rows=100)
  ->  Hash (cost=500..500 rows=500)
        Buckets: 4096  Batches: 1  Memory Usage: 3072kB
        Peak Memory: 3072kB
"""

HASH_LOW_MEMORY = """\
Hash Join (cost=100..500 rows=50)
  ->  Hash (cost=50..50 rows=50)
        Buckets: 256  Batches: 1  Memory Usage: 256kB
        Peak Memory: 1024kB
"""

NESTLOOP_HIGH_ROWS = """\
Nested Loop (cost=0..10000 rows=50000)
  Join Filter: (a.date > b.date)
  ->  Seq Scan on table_a (cost=0..100 rows=5000)
  ->  Index Scan on table_b (cost=0..2 rows=10)
"""

NESTLOOP_LOW_ROWS = """\
Nested Loop (cost=0..100 rows=500)
  ->  Index Scan on dim_table (cost=0..5 rows=1)
  ->  Index Scan on fact_table (cost=0..10 rows=500)
"""

NO_PARALLEL_LARGE_SCAN = """\
Seq Scan on store_sales (cost=0..1000000 rows=2880404)
  Filter: (ss_sold_date_sk >= 2451180)
"""

ALREADY_PARALLEL = """\
Gather (workers=4)
  ->  Parallel Seq Scan on store_sales (cost=0..500000 rows=1000000)
        Filter: (ss_sold_date_sk >= 2451180)
"""

SMALL_SEQ_SCAN = """\
Seq Scan on customer (cost=0..100 rows=50000)
  Filter: (c_birth_year = 1970)
"""

JIT_SHORT_QUERY = """\
Aggregate (cost=100..100 rows=1)
  ->  Seq Scan on table (cost=0..100 rows=100)
Planning Time: 0.5 ms
JIT:
  Functions: 5
  Time: 15.234 ms
Execution Time: 120.5 ms
"""

JIT_LONG_QUERY = """\
Aggregate (cost=100000..100000 rows=1)
  ->  Hash Join (cost=50000..80000 rows=500000)
JIT:
  Functions: 25
  Time: 150.0 ms
Execution Time: 5000.0 ms
"""

NO_JIT_QUERY = """\
Aggregate (cost=100..100 rows=1)
  ->  Seq Scan on table (cost=0..100 rows=100)
Execution Time: 50.0 ms
"""

FACT_SEQ_SCAN = """\
Hash Join (cost=5000..100000 rows=10000)
  ->  Seq Scan on catalog_sales (cost=0..50000 rows=1441548)
  ->  Hash (cost=100..100 rows=100)
"""

DIMENSION_SEQ_SCAN = """\
Seq Scan on customer (cost=0..500 rows=50000)
  Filter: (c_customer_sk = 12345)
"""

MANY_JOINS = """\
Hash Join (cost=...)
  ->  Hash Join (cost=...)
        ->  Merge Join (cost=...)
              ->  Nested Loop Left Join (cost=...)
                    ->  Hash Right Join (cost=...)
                          ->  Hash Semi Join (cost=...)
                                ->  Hash Anti Join (cost=...)
"""

FEW_JOINS = """\
Hash Join (cost=100..500 rows=100)
  ->  Merge Join (cost=50..200 rows=50)
"""


# =========================================================================
# Tests
# =========================================================================


class TestRuleWorkMem:
    """Test Rule 1: Hash memory + spill detection."""

    def test_disk_spill_detected(self):
        proposals = {}
        _rule_work_mem(HASH_SPILL_EXPLAIN, current_mb=4, proposals=proposals)
        assert "work_mem" in proposals
        assert "8 batches" in proposals["work_mem"]["reason"]
        assert proposals["work_mem"]["value"] == "256MB"

    def test_high_memory_no_spill(self):
        proposals = {}
        _rule_work_mem(HASH_HIGH_MEMORY_NO_SPILL, current_mb=4, proposals=proposals)
        assert "work_mem" in proposals
        # 3MB peak > 50% of 4MB → fire. 4x 3MB = 12MB → round to 256MB (min)
        assert proposals["work_mem"]["value"] == "256MB"

    def test_no_fire_low_memory(self):
        proposals = {}
        _rule_work_mem(HASH_LOW_MEMORY, current_mb=4, proposals=proposals)
        # 1MB peak < 50% of 4MB = 2MB → should NOT fire
        assert "work_mem" not in proposals

    def test_no_memory_patterns(self):
        proposals = {}
        _rule_work_mem("Seq Scan on table (rows=100)", current_mb=4, proposals=proposals)
        assert "work_mem" not in proposals


class TestRuleNestloop:
    """Test Rule 2: Nested Loop with high row estimates."""

    def test_high_row_count(self):
        proposals = {}
        _rule_nestloop(NESTLOOP_HIGH_ROWS, proposals=proposals)
        assert "enable_nestloop" in proposals
        assert proposals["enable_nestloop"]["value"] == "off"
        assert "50,000" in proposals["enable_nestloop"]["reason"]

    def test_low_row_count(self):
        proposals = {}
        _rule_nestloop(NESTLOOP_LOW_ROWS, proposals=proposals)
        assert "enable_nestloop" not in proposals

    def test_no_nestloop(self):
        proposals = {}
        _rule_nestloop("Hash Join (rows=50000)", proposals=proposals)
        assert "enable_nestloop" not in proposals


class TestRuleParallelism:
    """Test Rule 3: No parallel nodes despite large seq scans."""

    def test_no_parallel_large_scan(self):
        proposals = {}
        _rule_parallelism(NO_PARALLEL_LARGE_SCAN, proposals=proposals)
        assert "max_parallel_workers_per_gather" in proposals
        assert proposals["max_parallel_workers_per_gather"]["value"] == "4"
        assert "store_sales" in proposals["max_parallel_workers_per_gather"]["reason"]

    def test_already_parallel(self):
        proposals = {}
        _rule_parallelism(ALREADY_PARALLEL, proposals=proposals)
        assert "max_parallel_workers_per_gather" not in proposals

    def test_small_seq_scan(self):
        proposals = {}
        _rule_parallelism(SMALL_SEQ_SCAN, proposals=proposals)
        assert "max_parallel_workers_per_gather" not in proposals


class TestRuleJit:
    """Test Rule 4: JIT on short queries."""

    def test_jit_short_query(self):
        proposals = {}
        _rule_jit(JIT_SHORT_QUERY, proposals=proposals)
        assert "jit" in proposals
        assert proposals["jit"]["value"] == "off"
        assert "120" in proposals["jit"]["reason"]

    def test_jit_long_query(self):
        proposals = {}
        _rule_jit(JIT_LONG_QUERY, proposals=proposals)
        assert "jit" not in proposals  # >500ms, JIT likely helps

    def test_no_jit(self):
        proposals = {}
        _rule_jit(NO_JIT_QUERY, proposals=proposals)
        assert "jit" not in proposals


class TestRuleRandomPageCost:
    """Test Rule 5: Seq scans on fact tables."""

    def test_fact_table_seq_scan(self):
        proposals = {}
        _rule_random_page_cost(FACT_SEQ_SCAN, proposals=proposals)
        assert "random_page_cost" in proposals
        assert proposals["random_page_cost"]["value"] == "1.1"
        assert "catalog_sales" in proposals["random_page_cost"]["reason"]

    def test_dimension_table_scan(self):
        proposals = {}
        _rule_random_page_cost(DIMENSION_SEQ_SCAN, proposals=proposals)
        assert "random_page_cost" not in proposals

    def test_fact_table_small_scan(self):
        explain = "Seq Scan on store_sales (rows=500)"
        proposals = {}
        _rule_random_page_cost(explain, proposals=proposals)
        assert "random_page_cost" not in proposals  # <100K rows


class TestRuleJoinCollapse:
    """Test Rule 6: Many joins increase join_collapse_limit."""

    def test_many_joins(self):
        proposals = {}
        _rule_join_collapse(MANY_JOINS, proposals=proposals)
        assert "join_collapse_limit" in proposals
        assert proposals["join_collapse_limit"]["value"] == "12"

    def test_few_joins(self):
        proposals = {}
        _rule_join_collapse(FEW_JOINS, proposals=proposals)
        assert "join_collapse_limit" not in proposals


class TestRoundToPower2:
    """Test the _round_to_power_of_2 helper."""

    def test_exact_power(self):
        assert _round_to_power_of_2(256) == 256
        assert _round_to_power_of_2(512) == 512

    def test_round_up(self):
        assert _round_to_power_of_2(300) == 512
        assert _round_to_power_of_2(129) == 256

    def test_small(self):
        assert _round_to_power_of_2(1) == 1
        assert _round_to_power_of_2(0) == 1

    def test_large(self):
        assert _round_to_power_of_2(2048) == 2048
        assert _round_to_power_of_2(1025) == 2048


class TestProposeConfig:
    """Test the top-level propose_config_from_explain()."""

    def test_multiple_rules_fire(self):
        explain = f"""
{HASH_SPILL_EXPLAIN}
{NESTLOOP_HIGH_ROWS}
{JIT_SHORT_QUERY}
"""
        proposals = propose_config_from_explain(explain, current_work_mem_mb=4)
        assert "work_mem" in proposals
        assert "enable_nestloop" in proposals
        assert "jit" in proposals
        assert len(proposals) >= 3

    def test_no_rules_match(self):
        explain = "Seq Scan on small_table (rows=10)"
        proposals = propose_config_from_explain(explain, current_work_mem_mb=4)
        assert proposals == {}

    def test_empty_input(self):
        assert propose_config_from_explain("", current_work_mem_mb=4) == {}
        assert propose_config_from_explain(None, current_work_mem_mb=4) == {}

    def test_all_proposals_have_required_keys(self):
        explain = f"{HASH_SPILL_EXPLAIN}\n{NO_PARALLEL_LARGE_SCAN}"
        proposals = propose_config_from_explain(explain, current_work_mem_mb=4)
        for param, info in proposals.items():
            assert "value" in info, f"{param} missing 'value'"
            assert "rule" in info, f"{param} missing 'rule'"
            assert "reason" in info, f"{param} missing 'reason'"
