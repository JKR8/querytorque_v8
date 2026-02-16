# Synthetic Data Validation Tool

Automatically validates SQL queries by extracting schema, creating tables in DuckDB, generating synthetic data, and executing queries.

## Quick Start

```bash
# Validate a SQL file (target ~1000 output rows)
python3 validator.py your_query.sql

# Specify target row range (default: target to target*5)
python3 validator.py your_query.sql --target-rows 1000 --min-rows 800 --max-rows 5000

# Save results to JSON
python3 validator.py your_query.sql --target-rows 1000 --output results.json
```

## How It Works

1. **Schema Extraction** (SQLGlot AST): Parses SQL to identify all tables and columns
2. **Schema Creation**: Creates corresponding tables in DuckDB (in-memory)
3. **Data Generation**: Generates synthetic data based on column name patterns
4. **Query Execution**: Runs the query and reports results

## Column Type Inference

Types are inferred from column names:

| Pattern | Type |
|---------|------|
| `_sk`, `_id`, `_key` | INTEGER |
| `date`, `_dt` | DATE |
| `amt`, `amount`, `qty`, `price`, `sales` | DECIMAL(18,2) |
| `name`, `category`, `state` | VARCHAR |

## API Usage

```python
from validator import SyntheticValidator

validator = SyntheticValidator()
result = validator.validate('query.sql', target_rows=1000)

print(f"Success: {result['success']}")
print(f"Rows returned: {result['actual_rows']}")
```

## Limitations

- **GROUP BY queries**: Output rows limited by distinct combinations
- **WHERE clauses**: Restrictive filters reduce output rows
- **Column types**: Inferred from names (not schema)
- **Foreign keys**: Best-effort matching

## Example Queries

### Simple SELECT with LIMIT
```sql
SELECT customer_id, customer_name 
FROM customers 
LIMIT 1000
-- Returns: 1000 rows
```

### Join with Aggregation
```sql
SELECT c.customer_id, SUM(o.amount)
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id
-- Returns: Limited by distinct customer_id count
```

### Restrictive WHERE
```sql
SELECT * FROM customers WHERE state = 'CA'
-- Returns: Limited by matching rows (~10% of data)
```
