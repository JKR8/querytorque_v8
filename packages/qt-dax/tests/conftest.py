"""Pytest configuration and fixtures for qt-dax tests."""

import pytest
import tempfile
import json
import zipfile
from pathlib import Path
from typing import Optional


# =============================================================================
# DAX EXPRESSION FIXTURES
# =============================================================================

@pytest.fixture
def sample_simple_dax() -> str:
    """Simple DAX measure."""
    return "SUM('Sales'[Amount])"


@pytest.fixture
def sample_calculate_dax() -> str:
    """DAX with CALCULATE."""
    return """
    CALCULATE(
        SUM('Sales'[Amount]),
        'Product'[Category] = "Electronics"
    )
    """


@pytest.fixture
def sample_complex_dax() -> str:
    """Complex DAX with variables."""
    return """
    VAR TotalSales = SUM('Sales'[Amount])
    VAR TotalCost = SUM('Sales'[Cost])
    VAR Profit = TotalSales - TotalCost
    RETURN
        DIVIDE(Profit, TotalSales, 0)
    """


@pytest.fixture
def sample_nested_calculate_dax() -> str:
    """DAX with nested CALCULATE (anti-pattern)."""
    return """
    CALCULATE(
        CALCULATE(
            CALCULATE(
                SUM('Sales'[Amount]),
                'Date'[Year] = 2024
            ),
            'Product'[Category] = "Electronics"
        ),
        'Customer'[Region] = "North"
    )
    """


@pytest.fixture
def sample_sumx_filter_dax() -> str:
    """DAX with SUMX + FILTER (anti-pattern)."""
    return """
    SUMX(
        FILTER('Sales', 'Sales'[Amount] > 100),
        'Sales'[Amount] * 'Sales'[Quantity]
    )
    """


@pytest.fixture
def sample_division_without_divide_dax() -> str:
    """DAX with division operator (anti-pattern)."""
    return "[Total Sales] / [Total Quantity]"


@pytest.fixture
def sample_iterator_dax() -> str:
    """DAX with iterator functions."""
    return """
    SUMX(
        'Sales',
        'Sales'[Price] * 'Sales'[Quantity]
    )
    """


@pytest.fixture
def sample_time_intelligence_dax() -> str:
    """DAX with time intelligence functions."""
    return """
    VAR CurrentSales = SUM('Sales'[Amount])
    VAR PriorYearSales = CALCULATE(
        SUM('Sales'[Amount]),
        SAMEPERIODLASTYEAR('Date'[Date])
    )
    RETURN
        DIVIDE(CurrentSales - PriorYearSales, PriorYearSales, BLANK())
    """


@pytest.fixture
def sample_deeply_nested_if_dax() -> str:
    """DAX with deeply nested IF (should use SWITCH)."""
    return """
    IF(
        [Metric] = "Sales",
        [Total Sales],
        IF(
            [Metric] = "Cost",
            [Total Cost],
            IF(
                [Metric] = "Profit",
                [Total Profit],
                IF(
                    [Metric] = "Margin",
                    [Profit Margin],
                    BLANK()
                )
            )
        )
    )
    """


@pytest.fixture
def sample_with_comments_dax() -> str:
    """DAX with various comment styles."""
    return """
    // Single line comment
    VAR TotalSales = SUM('Sales'[Amount])
    /* Block comment
       spanning multiple lines */
    -- SQL style comment
    RETURN TotalSales
    """


# =============================================================================
# VPAX FIXTURES
# =============================================================================

@pytest.fixture
def sample_vpax_data() -> dict:
    """Sample VPAX data structure (DaxVpaView.json content)."""
    return {
        "Tables": [
            {
                "TableName": "Sales",
                "RowsCount": 100000,
                "IsHidden": False,
                "IsTemplateDateTable": False,
            },
            {
                "TableName": "Date",
                "RowsCount": 3650,
                "IsHidden": False,
                "IsTemplateDateTable": True,
            },
            {
                "TableName": "LocalDateTable_12345",
                "RowsCount": 365,
                "IsHidden": True,
                "IsTemplateDateTable": False,
            },
        ],
        "Columns": [
            {
                "TableName": "Sales",
                "ColumnName": "Amount",
                "DataType": "Decimal",
                "Encoding": "VALUE",
                "ColumnCardinality": 5000,
                "TotalSize": 50000,
                "DictionarySize": 10000,
                "IsKey": False,
            },
            {
                "TableName": "Sales",
                "ColumnName": "ProductID",
                "DataType": "Int64",
                "Encoding": "VALUE",
                "ColumnCardinality": 100,
                "TotalSize": 10000,
                "DictionarySize": 1000,
                "IsKey": False,
            },
            {
                "TableName": "Sales",
                "ColumnName": "HighCardColumn",
                "DataType": "String",
                "Encoding": "HASH",
                "ColumnCardinality": 1500000,  # Very high cardinality
                "TotalSize": 200000,
                "DictionarySize": 190000,
                "IsKey": False,
            },
        ],
        "Measures": [
            {
                "TableName": "Sales",
                "MeasureName": "Total Sales",
                "MeasureExpression": "SUM('Sales'[Amount])",
            },
            {
                "TableName": "Sales",
                "MeasureName": "Bad Division",
                "MeasureExpression": "[Total Sales] / [Total Quantity]",
            },
            {
                "TableName": "Sales",
                "MeasureName": "Nested Calculate",
                "MeasureExpression": """
                CALCULATE(
                    CALCULATE(
                        SUM('Sales'[Amount]),
                        'Date'[Year] = 2024
                    ),
                    'Product'[Category] = "Electronics"
                )
                """,
            },
        ],
        "Relationships": [
            {
                "FromTableName": "Sales",
                "FromFullColumnName": "'Sales'[ProductID]",
                "ToTableName": "Product",
                "ToFullColumnName": "'Product'[ID]",
                "IsActive": True,
                "CrossFilteringBehavior": 1,  # Single
                "MissingKeys": 0,
            },
            {
                "FromTableName": "Sales",
                "FromFullColumnName": "'Sales'[DateKey]",
                "ToTableName": "Date",
                "ToFullColumnName": "'Date'[DateKey]",
                "IsActive": True,
                "CrossFilteringBehavior": 2,  # Both (bi-directional)
                "MissingKeys": 15,  # RI violation
            },
        ],
    }


@pytest.fixture
def sample_vpax_file(sample_vpax_data, tmp_path) -> Path:
    """Create a temporary VPAX file for testing."""
    vpax_path = tmp_path / "test_model.vpax"

    with zipfile.ZipFile(vpax_path, "w") as zf:
        # Add DaxVpaView.json
        zf.writestr("DaxVpaView.json", json.dumps(sample_vpax_data))

        # Add minimal DaxModel.json
        dax_model = {"ModelName": "TestModel"}
        zf.writestr("DaxModel.json", json.dumps(dax_model))

    return vpax_path


@pytest.fixture
def sample_clean_vpax_data() -> dict:
    """VPAX data for a clean model (should score high)."""
    return {
        "Tables": [
            {
                "TableName": "Sales",
                "RowsCount": 10000,
                "IsHidden": False,
                "IsTemplateDateTable": False,
            },
            {
                "TableName": "Date",
                "RowsCount": 1826,
                "IsHidden": False,
                "IsTemplateDateTable": True,
            },
        ],
        "Columns": [
            {
                "TableName": "Sales",
                "ColumnName": "Amount",
                "DataType": "Decimal",
                "Encoding": "VALUE",
                "ColumnCardinality": 1000,
                "TotalSize": 50000,
                "DictionarySize": 10000,
                "IsKey": False,
            },
        ],
        "Measures": [
            {
                "TableName": "Sales",
                "MeasureName": "Total Sales",
                "MeasureExpression": "SUM('Sales'[Amount])",
            },
            {
                "TableName": "Sales",
                "MeasureName": "YoY Growth",
                "MeasureExpression": """
                VAR CurrentYear = SUM('Sales'[Amount])
                VAR PriorYear = CALCULATE(SUM('Sales'[Amount]), SAMEPERIODLASTYEAR('Date'[Date]))
                RETURN DIVIDE(CurrentYear - PriorYear, PriorYear, BLANK())
                """,
            },
        ],
        "Relationships": [
            {
                "FromTableName": "Sales",
                "FromFullColumnName": "'Sales'[DateKey]",
                "ToTableName": "Date",
                "ToFullColumnName": "'Date'[DateKey]",
                "IsActive": True,
                "CrossFilteringBehavior": 1,
                "MissingKeys": 0,
            },
        ],
    }


@pytest.fixture
def sample_clean_vpax_file(sample_clean_vpax_data, tmp_path) -> Path:
    """Create a clean VPAX file (should score high)."""
    vpax_path = tmp_path / "clean_model.vpax"

    with zipfile.ZipFile(vpax_path, "w") as zf:
        zf.writestr("DaxVpaView.json", json.dumps(sample_clean_vpax_data))
        zf.writestr("DaxModel.json", json.dumps({"ModelName": "CleanModel"}))

    return vpax_path


# =============================================================================
# ANALYZER FIXTURES
# =============================================================================

@pytest.fixture
def dax_analyzer():
    """Create a DAXAnalyzer instance."""
    from qt_dax.analyzers.vpax_analyzer import DAXAnalyzer
    return DAXAnalyzer()


@pytest.fixture
def vpax_parser(sample_vpax_file):
    """Create a VPAXParser instance."""
    from qt_dax.analyzers.vpax_analyzer import VPAXParser
    return VPAXParser(str(sample_vpax_file))


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_vpax_with_measures(measures: list[dict], tmp_path: Path) -> Path:
    """Helper to create VPAX with specific measures."""
    vpax_data = {
        "Tables": [{"TableName": "Sales", "RowsCount": 1000, "IsHidden": False}],
        "Columns": [],
        "Measures": [
            {
                "TableName": "Sales",
                "MeasureName": m.get("name", f"Measure{i}"),
                "MeasureExpression": m.get("expression", "SUM('Sales'[Amount])"),
            }
            for i, m in enumerate(measures)
        ],
        "Relationships": [],
    }

    vpax_path = tmp_path / "custom_model.vpax"
    with zipfile.ZipFile(vpax_path, "w") as zf:
        zf.writestr("DaxVpaView.json", json.dumps(vpax_data))
        zf.writestr("DaxModel.json", json.dumps({"ModelName": "CustomModel"}))

    return vpax_path


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
        "markers", "windows: marks tests that require Windows"
    )
