"""Live terminal dashboard for fleet mode using rich.live."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from rich.live import Live
from rich.table import Table
from rich.text import Text


STATUS_STYLES = {
    "PENDING": "dim",
    "RUNNING": "yellow",
    "WIN": "bold green",
    "IMPROVED": "green",
    "NEUTRAL": "white",
    "REGRESSION": "bold red",
    "ERROR": "red",
    "SKIP": "dim",
    "FAIL": "red",
}


@dataclass
class DashRow:
    """State of a single query in the dashboard."""

    query_id: str
    bucket: str = ""
    phase: str = ""
    status: str = "PENDING"
    speedup: Optional[float] = None
    detail: str = ""


class FleetDashboard:
    """Live Rich dashboard for fleet mode execution."""

    def __init__(self) -> None:
        self.rows: Dict[str, DashRow] = {}
        self._live: Optional[Live] = None

    def start(self) -> None:
        self._live = Live(self._build_table(), refresh_per_second=2)
        self._live.start()

    def stop(self) -> None:
        if self._live:
            self._live.stop()
            self._live = None

    def init_query(self, query_id: str, bucket: str, detail: str = "") -> None:
        """Register a query before execution starts."""
        status = "SKIP" if bucket == "SKIP" else "PENDING"
        phase = "skipped" if bucket == "SKIP" else "pending"
        self.rows[query_id] = DashRow(
            query_id=query_id,
            bucket=bucket,
            phase=phase,
            status=status,
            detail=detail,
        )
        self._refresh()

    def set_query_status(
        self,
        query_id: str,
        status: str,
        phase: str = "",
        speedup: Optional[float] = None,
        detail: str = "",
    ) -> None:
        """Update a query's status in the dashboard."""
        row = self.rows.get(query_id)
        if not row:
            row = DashRow(query_id=query_id)
            self.rows[query_id] = row

        row.status = status
        if phase:
            row.phase = phase
        if speedup is not None:
            row.speedup = speedup
        if detail:
            row.detail = detail

        self._refresh()

    def _refresh(self) -> None:
        if self._live:
            self._live.update(self._build_table())

    def _build_table(self) -> Table:
        table = Table(title="Fleet Dashboard", expand=False)
        table.add_column("Query", style="cyan", min_width=20)
        table.add_column("Bucket", min_width=8)
        table.add_column("Phase", min_width=12)
        table.add_column("Status", min_width=10)
        table.add_column("Speedup", justify="right", min_width=8)
        table.add_column("Detail", max_width=40)

        # Sort: RUNNING first, then by bucket priority (HIGH > MEDIUM > LOW > SKIP)
        bucket_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2, "SKIP": 3}
        status_order = {"RUNNING": 0}

        for row in sorted(
            self.rows.values(),
            key=lambda r: (
                status_order.get(r.status, 1),
                bucket_order.get(r.bucket, 4),
                r.query_id,
            ),
        ):
            style = STATUS_STYLES.get(row.status, "")
            speedup_str = f"{row.speedup:.2f}x" if row.speedup is not None else "-"

            table.add_row(
                row.query_id,
                Text(row.bucket, style="bold" if row.bucket == "HIGH" else ""),
                row.phase,
                Text(row.status, style=style),
                speedup_str,
                row.detail[:40] if row.detail else "",
            )

        # Summary footer
        total = len(self.rows)
        done = sum(
            1 for r in self.rows.values()
            if r.status not in ("PENDING", "RUNNING", "SKIP")
        )
        running = sum(1 for r in self.rows.values() if r.status == "RUNNING")
        wins = sum(1 for r in self.rows.values() if r.status == "WIN")
        skipped = sum(1 for r in self.rows.values() if r.status == "SKIP")

        table.caption = (
            f"Done: {done}/{total - skipped} | "
            f"Running: {running} | "
            f"Wins: {wins} | "
            f"Skipped: {skipped}"
        )

        return table
