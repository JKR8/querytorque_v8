"""Pytest configuration and fixtures for qt-sql tests."""

import pytest
from typing import Optional

# Import detector and related classes
from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector, SQLAnalysisResult
from qt_sql.analyzers.ast_detector.base import ASTDetector, ASTContext
from qt_sql.analyzers.ast_detector.registry import get_all_rules, get_rule_by_id


# =============================================================================
# DETECTOR FIXTURES
# =============================================================================

@pytest.fixture
def detector() -> SQLAntiPatternDetector:
    """Create a detector with generic dialect."""
    return SQLAntiPatternDetector(dialect="generic")


@pytest.fixture
def snowflake_detector() -> SQLAntiPatternDetector:
    """Create a detector with Snowflake dialect."""
    return SQLAntiPatternDetector(dialect="snowflake")


@pytest.fixture
def postgres_detector() -> SQLAntiPatternDetector:
    """Create a detector with PostgreSQL dialect."""
    return SQLAntiPatternDetector(dialect="postgres")


@pytest.fixture
def duckdb_detector() -> SQLAntiPatternDetector:
    """Create a detector with DuckDB dialect."""
    return SQLAntiPatternDetector(dialect="duckdb")


@pytest.fixture
def ast_detector() -> ASTDetector:
    """Create a raw AST detector for low-level testing."""
    return ASTDetector(dialect="generic")


# =============================================================================
# SAMPLE SQL FIXTURES
# =============================================================================

@pytest.fixture
def sample_clean_sql() -> str:
    """SQL that should score 100 - no issues."""
    return """
    SELECT
        u.id,
        u.name,
        u.email,
        u.created_at
    FROM users u
    WHERE u.active = true
    ORDER BY u.created_at DESC
    LIMIT 100
    """


@pytest.fixture
def sample_select_star_sql() -> str:
    """SQL with SELECT * issue."""
    return "SELECT * FROM users WHERE active = true"


@pytest.fixture
def sample_cartesian_join_sql() -> str:
    """SQL with Cartesian join issue."""
    return """
    SELECT u.name, o.order_id
    FROM users u, orders o
    WHERE u.active = true
    """


@pytest.fixture
def sample_function_on_column_sql() -> str:
    """SQL with function on indexed column issue."""
    return """
    SELECT id, name
    FROM users
    WHERE UPPER(email) = 'TEST@EXAMPLE.COM'
    """


@pytest.fixture
def sample_correlated_subquery_sql() -> str:
    """SQL with correlated subquery in WHERE."""
    return """
    SELECT u.id, u.name
    FROM users u
    WHERE EXISTS (
        SELECT 1 FROM orders o
        WHERE o.user_id = u.id
        AND o.total > 100
    )
    """


@pytest.fixture
def sample_multiple_issues_sql() -> str:
    """SQL with multiple issues for integration testing."""
    return """
    SELECT *
    FROM users u, orders o
    WHERE UPPER(u.email) = 'test@example.com'
    AND u.id = o.user_id
    ORDER BY 1
    """


@pytest.fixture
def sample_cte_sql() -> str:
    """SQL with CTEs for structure testing."""
    return """
    WITH active_users AS (
        SELECT id, name, email
        FROM users
        WHERE active = true
    ),
    user_orders AS (
        SELECT user_id, COUNT(*) as order_count
        FROM orders
        GROUP BY user_id
    )
    SELECT
        au.id,
        au.name,
        COALESCE(uo.order_count, 0) as orders
    FROM active_users au
    LEFT JOIN user_orders uo ON au.id = uo.user_id
    ORDER BY orders DESC
    LIMIT 10
    """


@pytest.fixture
def sample_complex_cte_sql() -> str:
    """Complex CTE SQL for testing multi-reference detection."""
    return """
    WITH base_data AS (
        SELECT id, name, category
        FROM products
        WHERE active = true
    ),
    category_stats AS (
        SELECT category, COUNT(*) as cnt
        FROM base_data
        GROUP BY category
    ),
    final_join AS (
        SELECT b.*, c.cnt
        FROM base_data b
        JOIN category_stats c ON b.category = c.category
    )
    SELECT * FROM final_join
    """


@pytest.fixture
def sample_window_function_sql() -> str:
    """SQL with window functions."""
    return """
    SELECT
        id,
        name,
        salary,
        ROW_NUMBER() OVER (ORDER BY salary DESC) as rank,
        SUM(salary) OVER (PARTITION BY department_id) as dept_total
    FROM employees
    """


@pytest.fixture
def sample_union_sql() -> str:
    """SQL with UNION (without ALL)."""
    return """
    SELECT id, name FROM users WHERE role = 'admin'
    UNION
    SELECT id, name FROM users WHERE role = 'manager'
    """


@pytest.fixture
def sample_deeply_nested_sql() -> str:
    """SQL with deeply nested subqueries."""
    return """
    SELECT id FROM users
    WHERE id IN (
        SELECT user_id FROM orders
        WHERE product_id IN (
            SELECT id FROM products
            WHERE category_id IN (
                SELECT id FROM categories WHERE active = true
            )
        )
    )
    """


# =============================================================================
# DUCKDB FIXTURES
# =============================================================================

@pytest.fixture
def duckdb_connection():
    """Create an in-memory DuckDB connection with sample tables."""
    try:
        import duckdb
        conn = duckdb.connect(":memory:")

        # Create sample tables
        conn.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                email VARCHAR,
                active BOOLEAN,
                created_at TIMESTAMP
            );
        """)

        conn.execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER,
                product_id INTEGER,
                amount DECIMAL(10, 2),
                order_date DATE
            );
        """)

        conn.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                category VARCHAR,
                price DECIMAL(10, 2),
                active BOOLEAN
            );
        """)

        # Insert sample data
        conn.execute("""
            INSERT INTO users VALUES
            (1, 'Alice', 'alice@example.com', true, '2024-01-01'),
            (2, 'Bob', 'bob@example.com', true, '2024-01-02'),
            (3, 'Charlie', 'charlie@example.com', false, '2024-01-03');
        """)

        conn.execute("""
            INSERT INTO products VALUES
            (1, 'Widget', 'Electronics', 29.99, true),
            (2, 'Gadget', 'Electronics', 49.99, true),
            (3, 'Thing', 'Home', 19.99, false);
        """)

        conn.execute("""
            INSERT INTO orders VALUES
            (1, 1, 1, 29.99, '2024-01-15'),
            (2, 1, 2, 49.99, '2024-01-16'),
            (3, 2, 1, 29.99, '2024-01-17');
        """)

        yield conn
        conn.close()
    except ImportError:
        pytest.skip("DuckDB not installed")


@pytest.fixture
def duckdb_executor(duckdb_connection):
    """Create a DuckDB executor wrapper."""
    try:
        from qt_sql.execution.duckdb_executor import DuckDBExecutor

        class PreconnectedExecutor:
            """Wrapper that uses existing connection."""
            def __init__(self, conn):
                self.conn = conn

            def execute(self, sql: str):
                return self.conn.execute(sql).fetchall()

            def explain(self, sql: str, analyze: bool = False):
                prefix = "EXPLAIN ANALYZE" if analyze else "EXPLAIN"
                return self.conn.execute(f"{prefix} {sql}").fetchall()

        return PreconnectedExecutor(duckdb_connection)
    except ImportError:
        pytest.skip("DuckDB executor not available")


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def analyze_and_find_rule(detector: SQLAntiPatternDetector, sql: str, rule_id: str) -> bool:
    """Analyze SQL and check if a specific rule was triggered."""
    result = detector.analyze(sql)
    return any(issue.rule_id == rule_id for issue in result.issues)


def get_rule_ids(result: SQLAnalysisResult) -> list[str]:
    """Extract rule IDs from analysis result."""
    return [issue.rule_id for issue in result.issues]


# =============================================================================
# MARKERS
# =============================================================================

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "duckdb: marks tests that require DuckDB"
    )
