# Payload Comparison Summary

## Final Benchmark Results

| Version | Speedup | Method |
|---------|---------|--------|
| Original | 1.00x | baseline |
| DeepSeek-chat (EXISTS) | 2.11x | minimal prompt |
| **DeepSeek-reasoner (IN)** | **2.64x** | minimal prompt |
| Gemini | 2.61x | v8 prompt |
| Manual | 2.56x | hand-tuned |

## Payload Size Comparison

| Payload | Estimated Tokens | Result |
|---------|------------------|--------|
| V7 (full YAML) | ~2,500 tokens | Did not produce optimal query |
| V8 current | ~800 tokens | Gemini worked, DeepSeek failed |
| **Minimal** | **~200 tokens** | **Both worked** |

## The Winning Minimal Prompt

```
Optimize this SQL. Reduce rows early.

Row counts:
- store_returns: 29M rows
- store WHERE s_state='SD': 41 rows
- date_dim WHERE d_year=2000: 366 rows

<query>

Return optimized SQL only.
```

## Key Insight

**Show filter selectivity, not just table size.**

- V7/V8: `store: 402 rows` (useless - LLM doesn't know the filter reduces it)
- Minimal: `store WHERE s_state='SD': 41 rows` (LLM sees the reduction potential)

## What V7 Payload Included (unnecessary)

- Engine metadata (version, threads, memory)
- Full schema with all columns
- Constraints (permitted/forbidden transformations)
- Recommended actions catalog
- Output format requirements
- Verification harness requirements

None of this helped. The LLM just needed:
1. Row counts with filter selectivity
2. The query
3. "Reduce rows early"
