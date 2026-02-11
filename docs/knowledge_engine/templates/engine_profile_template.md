## {Engine} Optimizer Profile ({version}, {N} {benchmark} queries SF{scale})

{briefing_note — 1-2 sentences of field intelligence context}

### Strengths — DO NOT fight these

| ID | Summary | Note |
|----|---------|------|
| {SCREAMING_SNAKE_ID} | {1 sentence} | {tactical field note with evidence} |
| | | |

### Gaps — Hunt for these

#### [{priority}] {GAP_ID}
**What**: {1 sentence — what the optimizer fails to do}
**Why**: {1 sentence — internal mechanism causing the failure}
**Hunt**: {1 sentence — what the worker should try}

Won: {Q_ID} {speedup}x ({technique}) · {Q_ID} {speedup}x ({technique})
Lost: {Q_ID} {speedup}x ({why it failed})

Rules:
- {actionable, condition-scoped field note}
- {diagnostic: what EXPLAIN signal confirms the opportunity}
- {safety: what NOT to do, with evidence}

---

(repeat gap block for each gap, ordered by priority HIGH → MEDIUM → LOW)

---

### SET LOCAL Config Intel (PostgreSQL only)

Config is ADDITIVE to SQL rewrite, not a substitute.

| Rule | Trigger (EXPLAIN signal) | Config | Evidence | Risk |
|------|--------------------------|--------|----------|------|
| {ID} | {what to look for} | {SET LOCAL ...} | {Q_ID}: {speedup}x | LOW / MEDIUM / HIGH / CRITICAL |
| | | | | |

Key findings:
- {1 sentence takeaway}
- {1 sentence takeaway}

### Scale Warning

{engine-specific scale sensitivity warning}

---

**Profile version**: {YYYY.MM.DD-vN}
**Last validated**: {YYYY-MM-DD}
**Analysis sessions**: {AS-XXX-NNN, AS-XXX-NNN}
