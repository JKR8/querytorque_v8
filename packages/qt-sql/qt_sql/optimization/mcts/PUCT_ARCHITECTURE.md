# Hybrid MCTS SQL Optimizer (Spec-Accurate)

This MCTS implementation follows the **Mini-HEP + Trimmed-Mean** spec.

## Key Points
- **Action Space:** deterministic sqlglot transforms (no LLM rewrites)
- **Policy:** deterministic priors (no LLM)
- **Search:** MCTS with PUCT and FPU for unvisited actions
- **Reward:** speedup from trimmed runtime (3 runs, drop first/warm-up)
- **Timeout:** 2x baseline → reward = 0.4

## ASCII Architecture
```
┌────────────────────────────────────────────────────────────────────────────┐
│                              MCTS + PUCT Tree                               │
│                                                                            │
│ Root: baseline latency (trimmed mean)                                      │
│ Node: sql, N, W, P, children                                               │
│                                                                            │
│ Select: traverse by PUCT                                                   │
│   PUCT = Q + C_puct * P * sqrt(sum N) / (1 + N)                            │
│   Q = W/N, if N=0 -> FPU                                                   │
│                                                                            │
│ Expand:                                                                    │
│   - apply all sqlglot rules (no-op → discard)                              │
│   - compute deterministic priors over valid moves                           │
│   - initialize child nodes with priors                                     │
│                                                                            │
│ Simulate:                                                                  │
│   - execute top-prior child                                                │
│   - trimmed-mean latency → speedup reward                                  │
│                                                                            │
│ Backprop: update N, W up the path                                          │
└────────────────────────────────────────────────────────────────────────────┘
```
