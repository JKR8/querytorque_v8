# qt-sql

SQL optimization and analysis product for QueryTorque.

## Features

- Static SQL analysis with 119 anti-pattern detection rules
- DuckDB query execution and validation
- Optional Calcite optimizer integration
- LLM-powered query optimization

## CLI

```bash
qt-sql audit <file.sql>
qt-sql optimize <file.sql>
qt-sql validate <orig.sql> <opt.sql>
```
