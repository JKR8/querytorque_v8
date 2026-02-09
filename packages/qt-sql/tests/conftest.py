"""Pytest configuration and fixtures for qt-sql tests."""

import pytest


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
