"""Dump the exact prompt DSPy sends to the model for inspection."""
import json
from pathlib import Path
import dspy
import sqlglot

MODEL = "ollama/qwen2.5-coder:7b"
OUT = Path(__file__).parent / "dspy_prompt_dump.txt"

def normalize_sql(sql: str) -> str:
    try:
        parsed = sqlglot.parse(sql)
        parts = []
        for stmt in parsed:
            parts.append(stmt.sql(dialect="duckdb", normalize=True, pretty=True, comments=False).lower())
        return "\n".join(parts)
    except Exception:
        return sql.lower().strip()

# -- Signature (same as dspy_equiv.py) --
class SQLEquivalence(dspy.Signature):
    """Determine if two SQL queries are semantically equivalent.
    Two queries are equivalent if and only if they return identical rows and
    columns on EVERY possible database state.

    SAFE transformations that preserve equivalence:
    - Extracting filters into CTEs or derived tables
    - Converting comma-joins (FROM a, b WHERE a.id = b.id) to explicit INNER JOINs
    - Reordering INNER JOIN clauses
    - Renaming table/column aliases
    - Splitting a single query into multiple CTEs joined back together

    UNSAFE transformations that BREAK equivalence:
    - Moving a WHERE predicate on a LEFT/RIGHT JOIN table into the ON clause
      (WHERE filters AFTER join removing NULLs; ON filters DURING join preserving NULLs)
    - Converting a correlated subquery to an uncorrelated CTE/derived table
      (correlated computes per outer row; uncorrelated computes once globally)
    - Changing INNER JOIN to LEFT JOIN or vice versa
    - Adding or removing DISTINCT
    - Changing GROUP BY columns or aggregate function scope
    - Dropping a filter predicate during CTE extraction
    - Changing AND to OR or vice versa in WHERE clauses"""

    query_a: str = dspy.InputField(desc="First SQL query (normalized, lowercase)")
    query_b: str = dspy.InputField(desc="Second SQL query (normalized, lowercase)")
    is_equivalent: str = dspy.OutputField(
        desc="Exactly TRUE or FALSE. TRUE means the queries return identical "
             "results on any database. FALSE means there exists some database "
             "where they return different results."
    )

class SQLEquivChecker(dspy.Module):
    def __init__(self):
        self.judge = dspy.ChainOfThought(SQLEquivalence)
    def forward(self, query_a, query_b):
        return self.judge(query_a=query_a, query_b=query_b)

# -- Sample queries (one short equivalent pair) --
SAMPLE_A = """select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
  from date_dim, store_sales, item, customer, customer_address, store
  where d_date_sk = ss_sold_date_sk
    and ss_item_sk = i_item_sk
    and ss_customer_sk = c_customer_sk
    and c_current_addr_sk = ca_address_sk
    and ss_store_sk = s_store_sk
    and i_category = 'Jewelry'
    and d_year = 2002
    and d_moy = 4
    and substring(ca_zip,1,5) <> substring(s_zip,1,5)
    and ca_state = 'IL'
    and c_birth_month = 1
    and ss_wholesale_cost between 35 and 55
 group by i_brand, i_brand_id, i_manufact_id, i_manufact
 order by ext_price desc, i_brand, i_brand_id, i_manufact_id, i_manufact
 limit 100;"""

SAMPLE_B = """WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2002 AND d_moy = 4)
SELECT i_brand_id AS brand_id, i_brand AS brand, i_manufact_id, i_manufact,
  SUM(ss_ext_sales_price) AS ext_price
FROM store_sales
JOIN filtered_dates ON d_date_sk = ss_sold_date_sk
JOIN item ON ss_item_sk = i_item_sk
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN customer_address ON c_current_addr_sk = ca_address_sk
JOIN store ON ss_store_sk = s_store_sk
WHERE i_category = 'Jewelry' AND c_birth_month = 1
  AND substring(ca_zip,1,5) <> substring(s_zip,1,5) AND ca_state = 'IL'
  AND ss_wholesale_cost BETWEEN 35 AND 55
GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
LIMIT 100;"""

def main():
    lm = dspy.LM(MODEL, api_base="http://localhost:11434", temperature=0.3, max_tokens=512)
    dspy.configure(lm=lm)

    checker = SQLEquivChecker()
    norm_a = normalize_sql(SAMPLE_A)
    norm_b = normalize_sql(SAMPLE_B)

    # Make one call so DSPy builds the prompt
    pred = checker(query_a=norm_a, query_b=norm_b)

    # Extract the actual prompt from LM history
    history = lm.history
    output = []
    output.append("=" * 80)
    output.append("DSPy PROMPT DUMP — what actually gets sent to the model")
    output.append("=" * 80)

    if history:
        last_call = history[-1]
        output.append(f"\n--- LM Call Metadata ---")
        output.append(f"Model: {MODEL}")
        output.append(f"Temperature: 0.3")
        output.append(f"Max tokens: 512")

        messages = last_call.get("messages", [])
        output.append(f"Messages count: {len(messages)}")

        for i, msg in enumerate(messages):
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            output.append(f"\n{'=' * 80}")
            output.append(f"MESSAGE {i+1} — role: {role}")
            output.append(f"{'=' * 80}")
            output.append(content)

        output.append(f"\n{'=' * 80}")
        output.append(f"MODEL RESPONSE")
        output.append(f"{'=' * 80}")

        response = last_call.get("response", {})
        if hasattr(response, "choices"):
            for choice in response.choices:
                output.append(choice.message.content)
        else:
            output.append(str(response))

        output.append(f"\n{'=' * 80}")
        output.append(f"PARSED OUTPUT")
        output.append(f"{'=' * 80}")
        output.append(f"is_equivalent: {pred.is_equivalent}")
        if hasattr(pred, "reasoning"):
            output.append(f"reasoning: {pred.reasoning}")
    else:
        output.append("No history found!")

    text = "\n".join(output)
    with open(OUT, "w") as f:
        f.write(text)
    print(text)
    print(f"\nSaved to: {OUT}")

if __name__ == "__main__":
    main()
