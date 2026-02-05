# Failure Analysis

## Failure Distribution


Total failures: 47/99 (47.5%)

### By Type


#### ERROR: 3 queries
- Q30: Original query execution failed: Binder Error: Referenced co...
- Q44: Original query execution failed: Binder Error: column profit...
- Q67: T; i; m; e; o; u; t;  ; (; 3; 0; 0; s; )

#### FAILS_VALIDATION: 9 queries
- Q2: Value mismatch detected between original and optimized resul...
- Q7: Value mismatch detected between original and optimized resul...
- Q16: Value mismatch detected between original and optimized resul...
- Q26: Value mismatch detected between original and optimized resul...
- Q35: Value mismatch detected between original and optimized resul...
- ... and 4 more

#### REGRESSION: 35 queries
- Q3: 
- Q9: 
- Q11: 
- Q14: 
- Q21: 
- ... and 30 more


## Regression Queries (Made Slower)

These queries show speedup < 1.0x, meaning our optimization made them slower:

**Count**: 35 queries

- Q3: 0.98x speedup (tried )
- Q9: 0.42x speedup (tried )
- Q11: 0.98x speedup (tried )
- Q14: 0.95x speedup (tried )
- Q21: 0.99x speedup (tried date_cte_isolate)
- Q22: 0.98x speedup (tried date_cte_isolate)
- Q24: 0.87x speedup (tried pushdown)
- Q25: 0.98x speedup (tried date_cte_isolate)
- Q29: 0.95x speedup (tried date_cte_isolate)
- Q32: 0.27x speedup (tried decorrelate)
- ... and 25 more


## Validation Failures

These queries failed result validation (wrong answer):

**Count**: 9 queries

- Q2: pushdown
- Q7: date_cte_isolate
- Q16: semantic_rewrite
- Q26: or_to_union
- Q35: date_cte_isolate
- Q51: date_cte_isolate
- Q59: pushdown
- Q65: date_cte_isolate
- Q81: decorrelate
