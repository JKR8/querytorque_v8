"""Convert DSB-76 queries from PostgreSQL to MySQL 8.0 dialect.

Only conversion needed: date interval arithmetic.
  PG:    cast('2002-03-06' as date) + interval '90 day'
  MySQL: DATE_ADD(CAST('2002-03-06' AS DATE), INTERVAL 90 DAY)

  PG:    cast('2002-02-20' as date) - interval '30 day'
  MySQL: DATE_SUB(CAST('2002-02-20' AS DATE), INTERVAL 30 DAY)

Also handles:
  PG:    ('2002-02-20' - interval '30 day')   (without explicit CAST)
  MySQL: DATE_SUB('2002-02-20', INTERVAL 30 DAY)
"""
import re
import shutil
from pathlib import Path

SRC = Path(__file__).parent.parent / "packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/queries"
DST = Path(__file__).parent.parent / "packages/qt-sql/qt_sql/benchmarks/mysql_dsb_76/queries"


def convert_interval(sql: str) -> str:
    """Convert PostgreSQL interval arithmetic to MySQL DATE_ADD/DATE_SUB."""
    # Pattern 1: cast('...' as date) + interval 'N day'
    # Handles optional whitespace and case variations
    sql = re.sub(
        r"cast\s*\(\s*'([^']+)'\s+as\s+date\s*\)\s*\+\s*interval\s+'(\d+)\s+day'",
        r"DATE_ADD(CAST('\1' AS DATE), INTERVAL \2 DAY)",
        sql,
        flags=re.IGNORECASE,
    )
    # Pattern 2: cast('...' as date) - interval 'N day'
    sql = re.sub(
        r"cast\s*\(\s*'([^']+)'\s+as\s+date\s*\)\s*-\s*interval\s+'(\d+)\s+day'",
        r"DATE_SUB(CAST('\1' AS DATE), INTERVAL \2 DAY)",
        sql,
        flags=re.IGNORECASE,
    )
    # Pattern 3: ('...' + interval 'N day') — bare string + interval (in parens)
    sql = re.sub(
        r"\(\s*'(\d{4}-\d{2}-\d{2})'\s*\+\s*interval\s+'(\d+)\s+day'\s*\)",
        r"DATE_ADD('\1', INTERVAL \2 DAY)",
        sql,
        flags=re.IGNORECASE,
    )
    # Pattern 4: ('...' - interval 'N day') — bare string - interval (in parens)
    sql = re.sub(
        r"\(\s*'(\d{4}-\d{2}-\d{2})'\s*\-\s*interval\s+'(\d+)\s+day'\s*\)",
        r"DATE_SUB('\1', INTERVAL \2 DAY)",
        sql,
        flags=re.IGNORECASE,
    )
    # Pattern 5: CAST(...) + INTERVAL 'N month' (less common)
    sql = re.sub(
        r"cast\s*\(\s*'([^']+)'\s+as\s+date\s*\)\s*\+\s*interval\s+'(\d+)\s+month'",
        r"DATE_ADD(CAST('\1' AS DATE), INTERVAL \2 MONTH)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = re.sub(
        r"cast\s*\(\s*'([^']+)'\s+as\s+date\s*\)\s*-\s*interval\s+'(\d+)\s+month'",
        r"DATE_SUB(CAST('\1' AS DATE), INTERVAL \2 MONTH)",
        sql,
        flags=re.IGNORECASE,
    )
    return sql


def main():
    DST.mkdir(parents=True, exist_ok=True)

    converted = 0
    unchanged = 0

    for src_file in sorted(SRC.glob("*.sql")):
        original = src_file.read_text()
        converted_sql = convert_interval(original)

        dst_file = DST / src_file.name
        dst_file.write_text(converted_sql)

        if original != converted_sql:
            converted += 1
            print(f"  CONVERTED: {src_file.name}")
        else:
            unchanged += 1

    total = converted + unchanged
    print(f"\nDone: {total} queries ({converted} converted, {unchanged} unchanged)")


if __name__ == "__main__":
    main()
