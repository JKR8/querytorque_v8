"""Celery tasks for QueryTorque async job processing.

Tasks:
- optimize_query: Run BeamSession pipeline on a single SQL query
- optimize_batch: Run optimization on multiple SQL files
- fleet_survey: Survey a customer database for slow queries
- process_github_pr: Extract SQL from PR and optimize
"""

import asyncio
import logging
import uuid
from datetime import datetime
from typing import Optional

import httpx

from .celery_app import celery_app

logger = logging.getLogger(__name__)


def _update_job_status(
    job_id: str,
    *,
    status: Optional[str] = None,
    progress: Optional[int] = None,
    result_json: Optional[dict] = None,
    error: Optional[str] = None,
    best_speedup: Optional[float] = None,
    best_sql: Optional[str] = None,
    outcome: Optional[str] = None,
    llm_tokens_prompt: Optional[int] = None,
    llm_tokens_completion: Optional[int] = None,
    completed: bool = False,
) -> None:
    """Update AnalysisJob in DB. Runs async in sync context."""
    async def _update():
        from qt_shared.database.connection import get_session_context
        from qt_shared.database.models import AnalysisJob
        from sqlalchemy import select

        async with get_session_context() as session:
            stmt = select(AnalysisJob).where(AnalysisJob.id == uuid.UUID(job_id))
            result = await session.execute(stmt)
            job = result.scalar_one_or_none()
            if not job:
                logger.error("Job %s not found", job_id)
                return

            if status:
                job.status = status
            if progress is not None:
                job.progress = progress
            if result_json is not None:
                job.result_json = result_json
            if error is not None:
                job.error = error
            if best_speedup is not None:
                job.best_speedup = best_speedup
            if best_sql is not None:
                job.best_sql = best_sql
            if outcome is not None:
                job.outcome = outcome
            if llm_tokens_prompt is not None:
                job.llm_tokens_prompt = llm_tokens_prompt
            if llm_tokens_completion is not None:
                job.llm_tokens_completion = llm_tokens_completion
            if completed:
                job.completed_at = datetime.utcnow()
            await session.commit()

    asyncio.run(_update())


def _is_private_ip(addr) -> bool:
    """Check if an ipaddress object points to a non-public network."""
    return addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved


def _validate_callback_url(url: str) -> bool:
    """Reject callback URLs that target internal networks (SSRF prevention).

    Resolves the hostname via DNS and checks every resolved address against
    private/loopback/link-local/reserved ranges. This defeats alternate IP
    notations (127.1, 2130706433, 0x7f000001) and DNS rebinding to RFC-1918.
    """
    from urllib.parse import urlparse
    import ipaddress
    import socket

    parsed = urlparse(url)
    if parsed.scheme not in ("https",):
        return False
    hostname = parsed.hostname or ""
    if not hostname:
        return False

    # Block well-known internal hostnames before DNS
    _blocked_hosts = {"localhost", "metadata.google.internal"}
    if hostname.lower() in _blocked_hosts:
        return False

    # Resolve hostname and check ALL returned addresses
    try:
        infos = socket.getaddrinfo(hostname, parsed.port or 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False  # unresolvable → reject

    if not infos:
        return False

    for family, _type, _proto, _canonname, sockaddr in infos:
        ip_str = sockaddr[0]
        try:
            addr = ipaddress.ip_address(ip_str)
        except ValueError:
            return False  # unparseable resolved IP → reject
        if _is_private_ip(addr):
            return False

    return True


def _send_callback(callback_url: str, payload: dict) -> None:
    """POST results to customer webhook callback URL."""
    if not _validate_callback_url(callback_url):
        logger.warning("Callback URL rejected (SSRF check): %s", callback_url)
        return
    try:
        resp = httpx.post(callback_url, json=payload, timeout=30)
        logger.info("Callback to %s: %s", callback_url, resp.status_code)
    except Exception as e:
        logger.warning("Callback to %s failed: %s", callback_url, e)


@celery_app.task(bind=True, name="qt_sql.tasks.optimize_query")
def optimize_query(
    self,
    job_id: str,
    sql: str,
    dsn: str,
    engine: str = "postgresql",
    max_iterations: int = 3,
    target_speedup: float = 1.10,
    org_id: Optional[str] = None,
    callback_url: Optional[str] = None,
) -> dict:
    """Run BeamSession optimization pipeline on a single SQL query.

    Args:
        job_id: AnalysisJob UUID
        sql: SQL query to optimize
        dsn: Decrypted database DSN
        engine: Database engine (postgresql, duckdb, snowflake)
        max_iterations: Max optimization iterations
        target_speedup: Target speedup ratio
        org_id: Organization UUID for metered billing
        callback_url: Optional URL to POST results to
    """
    _update_job_status(job_id, status="processing", progress=10)

    try:
        from .pipeline import Pipeline

        pipeline = Pipeline.from_dsn(dsn=dsn, engine=engine)

        query_id = f"job_{job_id[:8]}"

        _update_job_status(job_id, progress=30)

        result = pipeline.run_optimization_session(
            query_id=query_id,
            sql=sql,
            max_iterations=max_iterations,
            target_speedup=target_speedup,
        )

        result_payload = {
            "status": result.status,
            "speedup": result.best_speedup,
            "optimized_sql": result.best_sql,
            "transforms": getattr(result, "best_transforms", []),
            "n_iterations": getattr(result, "n_iterations", 0),
        }

        _update_job_status(
            job_id,
            status="completed",
            progress=100,
            result_json=result_payload,
            best_speedup=result.best_speedup,
            best_sql=result.best_sql,
            outcome=result.status,
            completed=True,
        )

        if callback_url:
            _send_callback(callback_url, {
                "job_id": job_id,
                "status": "completed",
                **result_payload,
            })

        return result_payload

    except Exception as exc:
        logger.exception("optimize_query failed for job %s", job_id)
        _update_job_status(
            job_id,
            status="failed",
            error=str(exc),
            completed=True,
        )
        if callback_url:
            _send_callback(callback_url, {
                "job_id": job_id,
                "status": "failed",
                "error": str(exc),
            })
        raise


@celery_app.task(bind=True, name="qt_sql.tasks.optimize_batch")
def optimize_batch(self, job_ids: list[str], **kwargs) -> dict:
    """Run optimization on multiple jobs sequentially.

    Creates individual optimize_query subtasks for each job.
    """
    results = {}
    for job_id in job_ids:
        try:
            result = optimize_query.delay(job_id, **kwargs)
            results[job_id] = {"celery_task_id": result.id}
        except Exception as e:
            results[job_id] = {"error": str(e)}
    return results


@celery_app.task(bind=True, name="qt_sql.tasks.fleet_survey")
def fleet_survey(
    self,
    survey_id: str,
    dsn: str,
    engine: str = "postgresql",
    org_id: Optional[str] = None,
) -> dict:
    """Survey a customer database for slow queries.

    Connects to the target database, discovers slow queries from
    pg_stat_statements (PostgreSQL) or query logs, and returns
    triage results.
    """
    try:
        from .fleet.orchestrator import FleetOrchestrator

        orchestrator = FleetOrchestrator()
        triage = orchestrator.survey_from_dsn(dsn=dsn, engine=engine)

        triage_data = [
            {
                "query_id": t.query_id,
                "sql": t.sql,
                "bucket": t.bucket,
                "priority_score": t.priority_score,
                "max_iterations": t.max_iterations,
                "runtime_ms": t.survey.runtime_ms if t.survey else None,
            }
            for t in triage
        ]

        # Update survey record
        async def _update():
            from qt_shared.database.connection import get_session_context
            from qt_shared.database.models import FleetSurvey
            from sqlalchemy import select

            async with get_session_context() as session:
                stmt = select(FleetSurvey).where(FleetSurvey.id == uuid.UUID(survey_id))
                result = await session.execute(stmt)
                survey = result.scalar_one_or_none()
                if survey:
                    survey.status = "completed"
                    survey.triage_json = {"queries": triage_data}
                    await session.commit()

        asyncio.run(_update())

        return {"survey_id": survey_id, "query_count": len(triage_data), "queries": triage_data}

    except Exception as exc:
        logger.exception("fleet_survey failed for survey %s", survey_id)
        raise


@celery_app.task(bind=True, name="qt_sql.tasks.process_github_pr")
def process_github_pr(
    self,
    installation_id: int,
    repo_full_name: str,
    pr_number: int,
    org_id: str,
) -> dict:
    """Extract SQL from a GitHub PR and optimize each file.

    1. Fetch PR diff
    2. Extract .sql files from diff
    3. Create AnalysisJob per SQL file
    4. Run optimization
    5. Post PR review comment with results
    """
    try:
        from .github.sql_extractor import extract_sql_from_diff
        from .github.comment_builder import build_review_comment
        from .github.api_client import GitHubAppClient

        client = GitHubAppClient(installation_id=installation_id)
        diff = client.get_pr_diff(repo_full_name, pr_number)
        sql_files = extract_sql_from_diff(diff)

        if not sql_files:
            return {"pr": pr_number, "sql_files": 0, "comment": "No SQL files found"}

        results = []
        for sql_file in sql_files:
            try:
                from .pipeline import Pipeline
                pipeline = Pipeline.from_dsn(dsn=":memory:", engine="duckdb")
                result = pipeline.run_query(
                    query_id=f"pr_{pr_number}_{sql_file['path']}",
                    sql=sql_file["sql"],
                )
                results.append({
                    "path": sql_file["path"],
                    "status": result.status,
                    "speedup": result.speedup,
                    "optimized_sql": result.optimized_sql,
                    "transforms": result.transforms_applied,
                })
            except Exception as e:
                results.append({
                    "path": sql_file["path"],
                    "status": "ERROR",
                    "error": str(e),
                })

        comment_body = build_review_comment(results)
        client.post_pr_comment(repo_full_name, pr_number, comment_body)

        return {
            "pr": pr_number,
            "sql_files": len(sql_files),
            "results": results,
        }

    except Exception as exc:
        logger.exception("process_github_pr failed for PR #%s", pr_number)
        raise
