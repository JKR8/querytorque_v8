"""GitHub App webhook handler (Tier C).

POST /api/v1/github/webhook — Handle GitHub webhook events (PR opened/updated)
"""

import hashlib
import hmac
import logging

from fastapi import APIRouter, HTTPException, Request

from qt_shared.config import get_settings

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/github", tags=["GitHub"])


def _verify_webhook_signature(body: bytes, signature: str, secret: str) -> bool:
    """Verify GitHub webhook signature (HMAC-SHA256)."""
    if not signature or not secret:
        return False
    expected = "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


@router.post("/webhook")
async def github_webhook(request: Request):
    """Handle GitHub App webhook events.

    Processes pull_request.opened and pull_request.synchronize events.
    Extracts SQL from PR diffs and dispatches optimization tasks.
    """
    settings = get_settings()
    if not settings.github_webhook_secret:
        raise HTTPException(status_code=501, detail="GitHub webhooks not configured")

    body = await request.body()
    signature = request.headers.get("X-Hub-Signature-256", "")

    if not _verify_webhook_signature(body, signature, settings.github_webhook_secret):
        raise HTTPException(status_code=400, detail="Invalid webhook signature")

    import json
    payload = json.loads(body)
    event_type = request.headers.get("X-GitHub-Event", "")

    if event_type == "pull_request":
        action = payload.get("action", "")
        if action in ("opened", "synchronize"):
            pr = payload.get("pull_request", {})
            repo = payload.get("repository", {})
            installation = payload.get("installation", {})

            pr_number = pr.get("number")
            repo_full_name = repo.get("full_name")
            installation_id = installation.get("id")

            if not all([pr_number, repo_full_name, installation_id]):
                return {"status": "skipped", "reason": "Missing required fields"}

            # Look up org for this installation
            from qt_shared.database.connection import get_session_context
            from qt_shared.database.models import GitHubInstallation
            from sqlalchemy import select
            import asyncio

            async def _get_org_id():
                async with get_session_context() as session:
                    stmt = select(GitHubInstallation).where(
                        GitHubInstallation.installation_id == installation_id
                    )
                    result = await session.execute(stmt)
                    inst = result.scalar_one_or_none()
                    return str(inst.org_id) if inst else None

            org_id = await _get_org_id()
            if not org_id:
                logger.warning("No org found for GitHub installation %s", installation_id)
                return {"status": "skipped", "reason": "Unknown installation"}

            # Dispatch async task
            from qt_sql.tasks import process_github_pr
            task = process_github_pr.delay(
                installation_id=installation_id,
                repo_full_name=repo_full_name,
                pr_number=pr_number,
                org_id=org_id,
            )

            logger.info(
                "Dispatched PR optimization: %s#%s (task=%s)",
                repo_full_name, pr_number, task.id,
            )
            return {"status": "processing", "task_id": task.id}

    elif event_type == "installation":
        action = payload.get("action", "")
        logger.info("GitHub App installation event: %s", action)
        return {"status": "acknowledged"}

    return {"status": "ignored", "event": event_type}
