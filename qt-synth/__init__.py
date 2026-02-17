"""
Synthetic Data Validation Tool

Validate SQL queries by:
1. Extracting schema via SQLGlot AST
2. Creating DuckDB tables
3. Generating synthetic data  
4. Executing queries
"""

from .validator import SyntheticValidator, SchemaExtractor, SyntheticDataGenerator

__all__ = ['SyntheticValidator', 'SchemaExtractor', 'SyntheticDataGenerator']
