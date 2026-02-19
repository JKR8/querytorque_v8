# QueryTorque SQL Extension — Build Plan

## Dependency Graph

```
Phase 0: Backend Prep ──────────────────────┐
  (Python — validator Qwen, API cleanup)     │
                                             │
Phase 1: Extension MVP ◄────────────────────┘
  (TypeScript — scaffold, beam flow, results)
         │
         ├──→ Phase 2: Free Tier Value
         │     (TS — patterns, config recs, Torque Score)
         │
         └──→ Phase 3: Growth
               (Billing, CLI, CI/CD, more DBs)
                    │
                    └──→ Phase 4: Enterprise
                          (Fleet Control, dbt, batch, gainshare)
```

**Rule:** Each phase produces a shippable artifact. No phase exceeds 3 weeks of focused work.

---

## Phase 0: Backend Prep (Python Only)

**Duration:** 1-2 weeks
**Goal:** The beam pipeline gains a validator LLM, and the API is clean enough for the extension to call.
**Ships:** Updated Python API server that the extension will talk to.

### What exists today

- `/api/sql/optimize` endpoint — takes SQL + DSN, returns `OptimizeResponse` with status, speedup, optimised SQL, per-worker results
- `Pipeline.from_dsn()` — creates ephemeral pipeline from just a connection string
- `BeamSession` — full 8-worker fan-out with 4-gate validation
- Typed response models matching the web client

### What we build

#### 0.1 — Validator LLM Integration

**Files to create:**
```
packages/qt-sql/qt_sql/validation/llm_validator.py    (~250 lines)
```

**Files to modify:**
```
packages/qt-sql/qt_sql/sessions/beam_session.py       (add validator call between Gate 1 and benchmark)
packages/qt-sql/qt_sql/schemas.py                     (add ValidatorVerdict dataclass)
packages/qt-shared/qt_shared/config/settings.py       (add QT_VALIDATOR_PROVIDER, QT_VALIDATOR_MODEL)
```

**Implementation:**

```python
# llm_validator.py — core class

@dataclass
class CandidateVerdict:
    candidate_id: str
    verdict: str           # EQUIVALENT | NOT_EQUIVALENT | UNCERTAIN
    confidence: float      # 0.0-1.0
    issues: list[str]      # Empty if EQUIVALENT
    column_lineage: dict   # col_name → {source, transform, match}
    reasoning: str         # LLM explanation

class LLMSemanticValidator:
    """Validates candidate SQL rewrites for semantic equivalence using an LLM.

    Sits between Gate 1 (parse check) and benchmark execution.
    Receives all candidates that passed parsing, returns per-candidate verdicts.
    Candidates with NOT_EQUIVALENT are rejected before benchmarking.
    Candidates with UNCERTAIN proceed to benchmark (let execution decide).
    """

    def __init__(self, llm_client: LLMClient, schema_info: dict):
        self.llm = llm_client
        self.schema = schema_info

    def validate_candidates(
        self,
        original_sql: str,
        candidates: list[AppliedPatch],
        explain_text: str | None = None,
    ) -> dict[str, CandidateVerdict]:
        """Validate all candidates in a single LLM call.

        Returns: {patch_id: CandidateVerdict}
        """
        prompt = self._build_prompt(original_sql, candidates, explain_text)
        response = self.llm.analyze(prompt)
        return self._parse_response(response, candidates)
```

**Integration point in beam_session.py:**

```python
# In _run_beam_iteration(), after Gate 1 parse check, before benchmark:

# --- NEW: Validator LLM gate ---
if self.config.get("validator_enabled", True):
    validator_client = create_llm_client(
        provider=settings.validator_provider or "openrouter",
        model=settings.validator_model or "qwen/qwen-2.5-72b-instruct",
    )
    if validator_client:
        validator = LLMSemanticValidator(validator_client, self._schema_info)
        verdicts = validator.validate_candidates(
            original_sql=self.original_sql,
            candidates=[p for p in patches if p.semantic_passed],  # Gate 1 survivors
            explain_text=self._original_explain,
        )
        for patch in patches:
            if patch.patch_id in verdicts:
                v = verdicts[patch.patch_id]
                if v.verdict == "NOT_EQUIVALENT":
                    patch.semantic_passed = False
                    patch.apply_error = f"Validator: {'; '.join(v.issues)}"
                    patch.status = "FAIL"
                patch.validator_verdict = v  # Store for UI display
# --- END validator gate ---
```

**Validator prompt:** See PRD_COMBINED_v2.md Section 6.2 for the full prompt template with column lineage rubric.

**Config:**
```env
QT_VALIDATOR_PROVIDER=openrouter       # or deepseek, openai, etc.
QT_VALIDATOR_MODEL=qwen/qwen-2.5-72b-instruct
QT_VALIDATOR_ENABLED=true              # can disable
```

#### 0.2 — API Cleanup

**Files to modify:**
```
packages/qt-sql/api/main.py           (strip SaaS route imports, keep /api/sql/optimize + /health)
```

**What to do:**
1. Make SaaS route imports conditional (behind `if settings.saas_mode:`)
2. Ensure `/api/sql/optimize` works standalone without Redis, Celery, Auth0
3. Add validator verdict to `OptimizeResponse`:
   ```python
   class WorkerResultResponse(BaseModel):
       # ... existing fields ...
       validator_verdict: Optional[str] = None     # EQUIVALENT | NOT_EQUIVALENT | UNCERTAIN
       validator_reasoning: Optional[str] = None   # Why
       validator_issues: list[str] = []             # What's wrong
   ```
4. Add `/api/v1/health` that returns version + capabilities

#### 0.3 — Standalone Server Script

**File to create:**
```
packages/qt-sql/serve.py               (~30 lines)
```

Simple script to start the API server without SaaS dependencies:
```python
#!/usr/bin/env python3
"""Start QueryTorque API server for VS Code extension."""
import uvicorn
uvicorn.run("api.main:app", host="127.0.0.1", port=8002, log_level="info")
```

The extension will either:
- (a) Auto-start this server as a child process, or
- (b) Connect to a user-started server, or
- (c) Connect to hosted `api.querytorque.com` (future)

### Phase 0 Definition of Done

- [ ] `LLMSemanticValidator` class implemented with column lineage prompt
- [ ] Validator integrated into beam_session.py between Gate 1 and benchmark
- [ ] Validator verdicts included in `WorkerResultResponse`
- [ ] `/api/sql/optimize` works without Redis/Celery/Auth0
- [ ] `python serve.py` starts the API server on port 8002
- [ ] Test: run a beam via curl, see validator verdicts in response

---

## Phase 1: Extension MVP (TypeScript)

**Duration:** 2-3 weeks
**Goal:** Install extension, connect PG, select SQL, run beam, see results, apply to file.
**Ships:** VS Code Marketplace listing (v0.1.0)
**Depends on:** Phase 0

### 1.1 — Extension Scaffold

**Files to create:**
```
extensions/sql/
├── package.json                    # Extension manifest
├── tsconfig.json                   # TypeScript config
├── webpack.config.js               # Bundler (or esbuild.config.js)
├── .vscodeignore                   # Exclude dev files from .vsix
├── src/
│   ├── extension.ts                # Entry point (activate/deactivate)
│   ├── constants.ts                # Extension IDs, default config
│   └── types.ts                    # Shared type definitions
├── resources/
│   └── icon.png                    # Extension icon
└── README.md                       # Marketplace description
```

**package.json key sections:**
```json
{
  "name": "querytorque-sql",
  "displayName": "QueryTorque for SQL",
  "description": "AI-powered SQL optimization. Detect anti-patterns, rewrite queries, validate speedups.",
  "version": "0.1.0",
  "engines": { "vscode": "^1.85.0" },
  "categories": ["Linters", "Programming Languages", "Other"],
  "activationEvents": [
    "onLanguage:sql",
    "workspaceContains:**/*.sql",
    "workspaceContains:dbt_project.yml",
    "workspaceContains:.querytorque.yml"
  ],
  "contributes": {
    "commands": [
      { "command": "querytorque.runBeam", "title": "QueryTorque: Optimise Query at Cursor" },
      { "command": "querytorque.connectDatabase", "title": "QueryTorque: Connect to Database" },
      { "command": "querytorque.explainQuery", "title": "QueryTorque: Explain Query at Cursor" },
      { "command": "querytorque.showConfigRecs", "title": "QueryTorque: Show Config Recommendations" }
    ],
    "configuration": {
      "title": "QueryTorque",
      "properties": {
        "querytorque.apiUrl": {
          "type": "string",
          "default": "http://127.0.0.1:8002",
          "description": "QueryTorque API server URL"
        },
        "querytorque.connections": {
          "type": "array",
          "description": "Database connections"
        }
      }
    },
    "viewsContainers": {
      "activitybar": [
        { "id": "querytorque", "title": "QueryTorque", "icon": "resources/icon.png" }
      ]
    }
  }
}
```

### 1.2 — Database Connection (PG Only for MVP)

**Files to create:**
```
extensions/sql/src/database/
├── connector.ts                    # DatabaseConnector interface
├── postgres.ts                     # PostgreSQL implementation (node-postgres)
└── connectionManager.ts            # Connection lifecycle, SecretStorage for credentials
```

**Scope for MVP:**
- Single PG connection via `node-postgres`
- Store credentials in VS Code SecretStorage (encrypted)
- "Test Connection" command
- `EXPLAIN ANALYZE` execution
- `SELECT` execution with LIMIT safety
- Schema introspection (tables, columns, types)

**UX:** Settings UI or command palette → "Connect to Database" → input form for host/port/db/user/password → test → save.

### 1.3 — Beam Request Flow

**Files to create:**
```
extensions/sql/src/api/
├── client.ts                       # REST client (adapt from web/src/api/client.ts)
└── types.ts                        # Request/response types (copy from web client)
```

**Flow:**
1. User selects SQL in editor (or cursor is inside a SQL block)
2. User runs `QueryTorque: Optimise Query at Cursor` (Ctrl+Shift+Q or command palette)
3. Extension extracts SQL from selection or detects query boundaries
4. Extension calls `POST /api/sql/optimize` with SQL + DSN
5. Extension shows progress panel with spinner ("Analyst reasoning...", "8 workers generating...", "Validator checking...", "Benchmarking...")
6. On completion, show results panel

### 1.4 — Results Panel (WebView)

**Files to create:**
```
extensions/sql/src/panels/
├── beamResultsPanel.ts             # WebView panel provider
└── beamResultsView/
    ├── index.html                  # Panel HTML shell
    ├── main.ts                     # Panel script
    └── styles.css                  # Panel styling
```

**What it shows:**
- Status badge: WIN / IMPROVED / NEUTRAL / REGRESSION / ERROR
- Speedup: "2.45x faster (4,200ms → 1,714ms)"
- Diff view: original vs optimised SQL (VS Code diff editor API)
- Transforms applied: ["decorrelate", "early_filter"]
- Validator reasoning (expandable per candidate)
- All 8 worker results (expandable list)
- Buttons: **[Apply to File]** [View Full Diff] [Reject]

### 1.5 — Write-Back

**Files to create:**
```
extensions/sql/src/commands/
├── runBeam.ts                      # Main beam command handler
├── applyFix.ts                     # Write optimised SQL back to file
├── connectDatabase.ts              # Connection command
└── explainQuery.ts                 # EXPLAIN at cursor
```

**Apply to file:** Uses VS Code `WorkspaceEdit` API to replace the selected SQL range with the optimised version. Shows undo notification ("Applied optimised SQL. [Undo]").

### 1.6 — SQL Extraction

**Files to create:**
```
extensions/sql/src/sql/
├── extractor.ts                    # Extract SQL at cursor position
└── boundaries.ts                   # Detect query boundaries (semicolons, statement gaps)
```

**Strategy for MVP:**
- In `.sql` files: detect statement boundaries by semicolons
- Selection takes priority: if user has selected text, use that
- Future: inline SQL detection in Python/TS/Java files

### Phase 1 Definition of Done

- [ ] Extension installs from .vsix or marketplace
- [ ] User can connect to PostgreSQL database via command palette
- [ ] User can place cursor on SQL in .sql file and run "Optimise Query"
- [ ] Extension calls API, shows progress, displays results panel
- [ ] Results show diff, speedup, worker details, validator reasoning
- [ ] "Apply to File" writes optimised SQL back
- [ ] End-to-end: install → connect → optimise → apply takes < 2 minutes

---

## Phase 2: Free Tier Value (TypeScript)

**Duration:** 2-3 weeks
**Goal:** Extension is useful without running a single beam. Patterns + config recommendations.
**Ships:** v0.2.0
**Depends on:** Phase 1 (extension scaffold)

### 2.1 — SQL Parser Integration

**Files to create:**
```
extensions/sql/src/analysis/
├── parser.ts                       # SQL parser wrapper (node-sql-parser or sql-parser-cst)
├── ast.ts                          # AST utility functions
└── features.ts                     # Feature extraction (port from Python tag_index.py)
```

**Decision needed:** `node-sql-parser` (broad dialect support) vs `sql-parser-cst` (better error recovery) vs `libpg-query` (highest PG fidelity, WASM build).

**Feature extraction (port from Python):**
- Table names + repeat counts
- Join types (INNER, LEFT, CROSS)
- Subquery detection (correlated vs independent)
- OR branch analysis (same-column vs cross-column)
- CTE detection (filtered vs unfiltered)
- Aggregation functions
- Star schema detection

### 2.2 — Anti-Pattern Library

**Files to create:**
```
extensions/sql/src/analysis/
├── patterns/
│   ├── index.ts                    # Pattern registry + loader
│   ├── types.ts                    # Pattern definition interface
│   ├── joins.ts                    # JN-001 through JN-006
│   ├── queryStructure.ts           # QS-001 through QS-008
│   ├── cloudCost.ts                # CC-001 through CC-006
│   └── orm.ts                      # ORM-001 through ORM-005
└── detector.ts                     # Main detection engine: SQL → Pattern matches
```

**Pattern definition format:**
```typescript
interface Pattern {
  id: string;                       // "JN-001"
  category: string;                 // "join", "structure", "cloud", "orm"
  name: string;                     // "Cartesian Join"
  severity: "critical" | "warning" | "info";
  description: string;              // Plain-language explanation
  detection: (ast: AST, plan?: QueryPlan) => PatternMatch | null;
  beamOpportunity: string | null;   // "Decorrelation typically yields 2-10x"
  databases: string[];              // ["postgresql", "mysql", "duckdb", ...]
}
```

### 2.3 — Config Recommendations Engine

**Files to create:**
```
extensions/sql/src/analysis/
├── configRecommender.ts            # EXPLAIN → SET LOCAL recommendations
└── pgRules.ts                      # 6 PostgreSQL rules (port from config_boost.py)
```

**The 6 rules (ported from Python):**
1. work_mem sizing (hash spill detection)
2. Disable nested loops (high-row nested loops)
3. Enable parallelism (large sequential scans)
4. Disable JIT (short queries with JIT overhead)
5. Favour index scans (SSD hint)
6. Increase join_collapse_limit (high join count)

**Output format:**
```typescript
interface ConfigRecommendation {
  parameter: string;                // "work_mem"
  currentValue: string;             // "4MB"
  recommendedValue: string;         // "256MB"
  setLocalStatement: string;        // "SET LOCAL work_mem = '256MB';"
  reasoning: string;                // "Hash spill detected (Batches=4)..."
  confidence: "high" | "medium";
  safeToTest: boolean;              // Always true (SET LOCAL is transaction-scoped)
}
```

### 2.4 — Torque Score

**Files to create:**
```
extensions/sql/src/analysis/
└── scoring.ts                      # Torque Score calculation
```

Score = (Severity × 0.2) + (Cost × 0.4) + (ScanEfficiency × 0.2) + (Frequency × 0.2)

### 2.5 — Editor Diagnostics

**Files to modify/create:**
```
extensions/sql/src/providers/
├── diagnosticProvider.ts           # Squiggly underlines on anti-patterns
├── codeLensProvider.ts             # "Torque: 72 | $34/mo | 3 issues" above queries
├── hoverProvider.ts                # Hover on underline → pattern explanation
└── codeActionProvider.ts           # Lightbulb → "Run Beam" / "Show Config Recs"
```

### 2.6 — Database Health Panel

**Files to create:**
```
extensions/sql/src/views/
├── healthPanel.ts                  # Activity bar tree view
├── issueTree.ts                    # Issue list grouped by priority
└── costExplorer.ts                 # Top expensive queries view
```

### Phase 2 Definition of Done

- [ ] Opening a .sql file shows squiggly underlines on detected patterns
- [ ] Hover on underline shows: pattern name, explanation, beam opportunity hint
- [ ] CodeLens shows Torque Score above each query
- [ ] "Show Config Recommendations" runs EXPLAIN and shows SET LOCAL advice
- [ ] Database Health panel in activity bar shows issue tree
- [ ] All detection is offline (no API calls, no beam credits consumed)
- [ ] Config recommendations are copy-pasteable SQL statements

---

## Phase 3: Growth

**Duration:** 3-4 weeks
**Goal:** Billing, more databases, CLI, CI/CD.
**Ships:** v0.3.0 (Pro tier goes live)
**Depends on:** Phase 2

### 3.1 — Billing (Stripe)

**Files to create:**
```
extensions/sql/src/billing/
├── licenseManager.ts               # License key validation, credit tracking
├── stripeClient.ts                 # Stripe checkout session creation
└── creditTracker.ts                # Free beam counter (3/month), Pro unlimited
```

**Flow:** Extension stores a licence key (from Stripe checkout) in SecretStorage. On each beam request, check credits:
- Free: 3 beams/month (tracked locally + server-side)
- Pro ($49/mo): 50 beams/month, unused credits roll over 1 month (max 100 banked)
- Team ($199/seat/mo): Unlimited beams, no credit tracking needed

### 3.2 — Additional Database Connectors

**Files to create:**
```
extensions/sql/src/database/
├── duckdb.ts                       # DuckDB connector (duckdb-node)
├── snowflake.ts                    # Snowflake connector (snowflake-sdk)
└── mysql.ts                        # MySQL connector (mysql2)
```

Each implements the `DatabaseConnector` interface from Phase 1.

### 3.3 — CLI Package

**Files to create:**
```
packages/cli/
├── package.json                    # @querytorque/cli
├── src/
│   ├── index.ts                    # CLI entry point
│   ├── commands/
│   │   ├── scan.ts                 # Static analysis (uses same pattern library)
│   │   ├── beam.ts                 # Run beam on a file
│   │   └── score.ts                # Just the Torque Score
│   └── reporters/
│       ├── json.ts                 # JSON output
│       ├── sarif.ts                # SARIF for GitHub Code Scanning
│       └── html.ts                 # HTML report
└── tsconfig.json
```

**Shared code:** The pattern library and scoring engine from the extension are extracted into a shared package (`@querytorque/analysis`) used by both extension and CLI.

### 3.4 — GitHub Action

**Files to create:**
```
actions/scan-action/
├── action.yml                      # GitHub Action definition
├── Dockerfile                      # Runs CLI in container
└── entrypoint.sh                   # Calls @querytorque/cli scan
```

### 3.5 — Multi-Connection UX

Support multiple simultaneous database connections. Connection picker in status bar. Per-file connection association (via `.querytorque.yml` or manual assignment).

### Phase 3 Definition of Done

- [ ] Pro tier live: Stripe checkout, licence key, credit enforcement
- [ ] Free → Pro conversion prompt on 4th beam attempt
- [ ] DuckDB, Snowflake, MySQL connectors working
- [ ] CLI: `npx @querytorque/cli scan ./src/ --db postgresql`
- [ ] GitHub Action: PR comments with Torque Score + issues
- [ ] SARIF output for GitHub Code Scanning integration

---

## Phase 4: Enterprise

**Duration:** 4-6 weeks
**Goal:** Fleet Control dashboard, batch processing, dbt, team tier.
**Ships:** v1.0.0 + Fleet Control web app
**Depends on:** Phase 3

### 4.1 — Fleet Control Dashboard (React Web App)

**Files to create:**
```
apps/fleet-control/
├── package.json
├── src/
│   ├── pages/
│   │   ├── Dashboard.tsx           # Org-wide Torque Score, cost trending
│   │   ├── CostLeaderboard.tsx     # Top queries by cost
│   │   ├── TeamHealth.tsx          # Per-team scores
│   │   ├── SavingsTracker.tsx      # Gainshare calculator
│   │   └── BatchProcessor.tsx      # Upload SQL, bulk beam
│   ├── api/
│   │   └── fleetClient.ts          # Fleet Control API client
│   └── components/
│       └── ...
└── tsconfig.json
```

### 4.2 — Fleet Control API (Python)

**Files to create/modify:**
```
packages/qt-sql/api/routes/fleet_v2.py     # New fleet endpoints (strip old SaaS bloat)
```

Endpoints:
- `POST /api/v1/fleet/ingest` — Receive telemetry from extensions
- `GET /api/v1/fleet/dashboard` — Aggregate scores, costs, savings
- `POST /api/v1/fleet/batch` — Submit batch beam job
- `GET /api/v1/fleet/batch/{id}` — Batch status + results

### 4.3 — Extension Telemetry (Opt-In)

**Files to create:**
```
extensions/sql/src/telemetry/
├── reporter.ts                     # Aggregate metrics (never raw SQL)
└── consent.ts                      # Opt-in UI, Team tier only
```

### 4.4 — dbt Integration

**Files to create:**
```
extensions/sql/src/integrations/
├── dbt/
│   ├── detector.ts                 # Detect dbt_project.yml
│   ├── modelParser.ts              # Parse models, resolve refs
│   ├── compiler.ts                 # Call dbt compile or builtin resolution
│   └── materialisationAnalyser.ts  # Flag table vs view mismatches
```

### 4.5 — Batch Processing

Extension or Fleet Control can upload a SQL file set (or pg_stat_statements export) and queue beam optimisations for all queries. Results presented as a report with aggregate savings estimate.

### 4.6 — Team Tier ($199/seat/mo)

- Seat management (invite team members, assign roles)
- Shared `.querytorque.yml` config across org
- Unlimited beams per seat (no credit tracking)
- Fleet Control telemetry opt-in consent flow

### 4.7 — Gainshare Engine

The core Fleet Control revenue mechanism:

1. **Baseline capture:** Original query timing (5x trimmed mean) + frequency (pg_stat_statements / query history)
2. **Savings verification:** Periodic re-benchmarks (weekly) confirm deployed rewrites still hold
3. **Calculation:** `saved_seconds × monthly_frequency × $/compute-second`
4. **Invoice:** `total_verified_savings × gainshare_rate` (10–15%)
5. **Audit trail:** All benchmarks checksummed, customer can verify every number

**Files to create:**
```
apps/fleet-control/src/
├── services/
│   ├── savingsVerifier.ts          # Periodic re-benchmark scheduler
│   ├── gainshareCalculator.ts      # Savings × rate = invoice
│   └── auditTrail.ts              # Checksummed benchmark history
```

### Phase 4 Definition of Done

- [ ] Fleet Control dashboard live at fleet.querytorque.com
- [ ] Org-wide Torque Score trending
- [ ] Batch beam processing: upload 50 queries, get optimisation report
- [ ] Gainshare engine: verified savings calculation with audit trail
- [ ] Savings waterfall: "We saved you $4,200/month. Your gainshare (15%): $630/month."
- [ ] dbt models scanned and analysed
- [ ] Team tier: seat management, unlimited beams, shared config
- [ ] PDF savings report export (for procurement justification)

---

## Phase Summary

| Phase | Duration | Ships | Revenue | Key Deliverable |
|-------|----------|-------|---------|----------------|
| **0: Backend Prep** | 1-2 weeks | Python API update | — | Validator Qwen in beam pipeline, clean API endpoint |
| **1: Extension MVP** | 2-3 weeks | v0.1.0 on Marketplace | Free (3 beams/mo) | Connect PG → Run Beam → See 2.45x → Apply to file |
| **2: Free Tier Value** | 2-3 weeks | v0.2.0 | Free (the hook) | Patterns + config recs + Torque Score (no beam needed) |
| **3: Growth** | 3-4 weeks | v0.3.0 + CLI + Action | Pro $49/mo, Team $199/seat | Billing, multi-DB, CLI, GitHub Action |
| **4: Enterprise** | 4-6 weeks | v1.0.0 + Fleet Control | Fleet $2,500+/mo or 10-15% gainshare | Dashboard, batch, dbt, gainshare engine |

**Total:** 12-18 weeks to full product suite.
**First usable artifact:** 3-5 weeks (end of Phase 1).
**First revenue:** Week 12 (Pro tier live).
**First enterprise deal:** Week 18+ (Fleet Control + gainshare).

---

## Revenue Model by Phase

```
Phase 0-1:  $0          — Building the weapon
Phase 2:    $0          — Building the hook (free tier)
Phase 3:    Pro $49/mo × N users + Team $199/seat × M seats
Phase 4:    Fleet Control $2,500+/mo OR 10-15% gainshare per org

Target at 18 months post-launch:
  1,000 Pro users × $49      = $49K MRR
  50 Team seats × $199        = $10K MRR
  5 Fleet Control orgs × $5K  = $25K MRR
  2 Gainshare orgs × $15K     = $30K MRR
  ─────────────────────────────
  Total:                        ~$114K MRR
```

---

## Critical Path

```
Week 1-2:   Phase 0 (validator + API cleanup)
Week 3-5:   Phase 1 (extension MVP — THE demo)
Week 5:     SHIP v0.1.0 to Marketplace ← first users (3 free beams)
Week 6-8:   Phase 2 (free tier — THE hook)
Week 8:     SHIP v0.2.0 ← patterns + config recs live
Week 9-12:  Phase 3 (billing — THE revenue)
Week 12:    SHIP v0.3.0 ← Pro $49/mo + Team $199/seat live
Week 13-18: Phase 4 (enterprise — THE big deal)
Week 18:    SHIP v1.0.0 + Fleet Control ← gainshare live
```

---

## Immediate Next Steps (This Week)

1. **Decision: SQL parser** — `node-sql-parser` vs `sql-parser-cst` vs `libpg-query` WASM
2. **Decision: API hosting** — Local Python server vs hosted api.querytorque.com vs both
3. **Decision: Validator model** — Qwen-72B vs Qwen-32B vs DeepSeek-V3 (benchmark on 10 TPC-DS queries)
4. **Start Phase 0.1** — Implement `LLMSemanticValidator` class
5. **Start Phase 1.1** — Scaffold VS Code extension (`yo code` generator)
