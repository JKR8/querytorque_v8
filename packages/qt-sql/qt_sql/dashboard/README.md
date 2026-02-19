# Fleet C2 Dashboard

Single interactive dashboard for query optimization triage, execution, and results.

## Entry Point

```bash
qt dashboard <benchmark>          # e.g. qt dashboard databricks_tpch
qt dashboard <benchmark> --port 9000
```

Opens `http://127.0.0.1:8765` (default) in your browser.

## Architecture

```
qt dashboard <benchmark>
    |
    v
dashboard_cmd.py::serve_dashboard()
    |-- _collect_fleet_data()      # AST detection, EXPLAIN timing, bucket scoring
    |-- _fleet_queries_to_c2()     # Adapter: field mapping + SQL loading
    |-- FleetWSServer(initial_data) # FastAPI + uvicorn
         |-- GET /          -> fleet_c2.html (data injected via __FLEET_DATA__)
         |-- GET /help      -> fleet_c2_help.html
         |-- WS  /ws        -> live event stream
```

## Components

| Component | File | Role |
|-----------|------|------|
| Frontend | `fleet_c2.html` | Single-page app with 4 tabs |
| WebSocket server | `fleet/ws_server.py` | FastAPI, serves HTML + WS events |
| Data collector | `dashboard_cmd.py` | AST detection, EXPLAIN timing, fleet runs |
| Event bus | `fleet/event_bus.py` | Thread-safe event queue for live updates |

## Tabs

1. **Triage** -- Query analysis: runtimes, cost buckets, transform matches, structural flags (read-only intelligence)
2. **Execution** -- Approve & Deploy, live progress monitoring, pause/resume
3. **Editor** -- SQL editor with Beam/Strike execution, per-query optimization
4. **Results** -- Worker results, speedups, iteration history

## Standalone Mode

When launched via `qt dashboard`, the server runs in standalone mode:
- `EventBus` is empty (no active orchestrator pushing events)
- `triage_gate` is unset (no execution flow to gate)
- Dashboard renders `initial_data` on load and handles idle state gracefully
- WebSocket connects but no live events flow until an editor session is started

## Config Modal

The Config button (gear icon) opens runtime configuration:
- **Engine**: Database engine type
- **DSN**: Database connection string
- **Source mode**: local benchmark dir
- **Connection test**: Verify DB/LLM connectivity

## Demo Mode

If no data is injected (standalone HTML), the dashboard falls back to built-in mock data (20 TPC-DS queries) for offline exploration.

## Archived

`_forensic_static.html` -- Previous read-only dashboard with forensic visualizations (Opportunity Matrix, Pattern Coverage donut, Q-Error charts). Kept for reference; unique charts may be ported into fleet_c2.html later.
