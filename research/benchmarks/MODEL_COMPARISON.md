# Model Comparison - SQL Optimization

Quick comparison of different LLM providers on TPC-DS optimization task.

## Q1 Single-Query Test

| Model | Provider | Speedup | LLM Time | Valid | Notes |
|-------|----------|---------|----------|-------|-------|
| DeepSeek V3 | Direct | 1.35x | ~10-15s | ✓ | Baseline from full run |
| Kimi K2.5 | OpenRouter | 1.45x | 54.1s | ✓ | Best speedup |
| Claude Sonnet 4 | OpenRouter | 1.19x | 28.3s | ✓ | Fastest LLM response |

**Test conditions:**
- Database: DuckDB SF100 sampled (1%)
- Query: TPC-DS Q1 (store returns aggregation)
- Pipeline: DSPy ChainOfThought
- Validation: Row-level checksum comparison

## Full Benchmark Runs

| Model | Date | Optimized | Avg Speedup | Wins (≥1.2x) | Validated | Link |
|-------|------|-----------|-------------|--------------|-----------|------|
| DeepSeek V3 | 2026-02-01 | 82/99 | 1.14x | 20 | 72/82 | [Results](deepseek/2026-02-01.md) |
| Kimi K2.5 | - | - | - | - | - | Pending |
| Claude Sonnet 4 | - | - | - | - | - | Pending |

## Cost Comparison (Estimated)

| Model | Input $/1M | Output $/1M | Est. Cost/Query | Est. Full Run |
|-------|------------|-------------|-----------------|---------------|
| DeepSeek V3 | $0.14 | $0.28 | ~$0.01 | ~$1 |
| Kimi K2.5 | $0.125 | $0.55 | ~$0.02 | ~$2 |
| Claude Sonnet 4 | $3.00 | $15.00 | ~$0.15 | ~$15 |

*Costs via OpenRouter, may vary*

## Observations

1. **Kimi K2.5** produced the best Q1 optimization (1.45x) but slowest LLM time
2. **Claude Sonnet 4** fastest response but lowest speedup
3. **DeepSeek V3** good balance of speed and quality
4. All models passed semantic validation on Q1

---

*Updated: 2026-02-02*
