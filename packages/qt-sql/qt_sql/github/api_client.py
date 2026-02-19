"""GitHub App API client for QueryTorque.

Handles:
- Installation access token management
- Fetching PR diffs
- Posting PR review comments
"""

import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"


class GitHubAppClient:
    """Client for interacting with GitHub API as a GitHub App installation."""

    def __init__(
        self,
        installation_id: int,
        access_token: Optional[str] = None,
    ) -> None:
        self.installation_id = installation_id
        self._access_token = access_token
        self._token_expires_at: float = 0

    def _get_access_token(self) -> str:
        """Get or refresh the installation access token.

        If no token is provided, fetches one from the DB (encrypted).
        If the token is expired, generates a new one using the App's JWT.
        """
        if self._access_token and time.time() < self._token_expires_at:
            return self._access_token

        # Try loading from DB
        try:
            import asyncio
            from qt_shared.database.connection import get_session_context
            from qt_shared.database.models import GitHubInstallation
            from qt_shared.vault import decrypt_dsn
            from sqlalchemy import select

            async def _load():
                async with get_session_context() as session:
                    stmt = select(GitHubInstallation).where(
                        GitHubInstallation.installation_id == self.installation_id
                    )
                    result = await session.execute(stmt)
                    inst = result.scalar_one_or_none()
                    if inst and inst.encrypted_access_token:
                        return decrypt_dsn(inst.encrypted_access_token)
                    return None

            token = asyncio.run(_load())
            if token:
                self._access_token = token
                # Assume 1 hour validity
                self._token_expires_at = time.time() + 3500
                return token
        except Exception as e:
            logger.warning("Failed to load token from DB: %s", e)

        # Fall back to generating JWT and requesting new installation token
        token = self._generate_installation_token()
        if token:
            self._access_token = token
            self._token_expires_at = time.time() + 3500
            return token

        raise RuntimeError(
            f"Could not obtain access token for installation {self.installation_id}"
        )

    def _generate_installation_token(self) -> Optional[str]:
        """Generate a new installation access token using the GitHub App's JWT."""
        try:
            from qt_shared.config import get_settings
            settings = get_settings()

            if not settings.github_app_id or not settings.github_app_private_key:
                logger.error("GitHub App ID or private key not configured")
                return None

            import jwt as pyjwt

            now = int(time.time())
            payload = {
                "iat": now - 60,
                "exp": now + (10 * 60),
                "iss": settings.github_app_id,
            }
            encoded_jwt = pyjwt.encode(
                payload, settings.github_app_private_key, algorithm="RS256"
            )

            resp = httpx.post(
                f"{GITHUB_API_BASE}/app/installations/{self.installation_id}/access_tokens",
                headers={
                    "Authorization": f"Bearer {encoded_jwt}",
                    "Accept": "application/vnd.github+json",
                },
                timeout=30,
            )
            resp.raise_for_status()
            return resp.json().get("token")

        except ImportError:
            logger.error("PyJWT not installed — cannot generate GitHub App JWT")
            return None
        except Exception as e:
            logger.error("Failed to generate installation token: %s", e)
            return None

    def _headers(self) -> dict:
        """Standard headers for GitHub API requests."""
        token = self._get_access_token()
        return {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
        }

    def get_pr_diff(self, repo_full_name: str, pr_number: int) -> str:
        """Fetch the diff for a pull request.

        Args:
            repo_full_name: e.g., "owner/repo"
            pr_number: PR number

        Returns:
            Raw unified diff text.
        """
        resp = httpx.get(
            f"{GITHUB_API_BASE}/repos/{repo_full_name}/pulls/{pr_number}",
            headers={
                **self._headers(),
                "Accept": "application/vnd.github.v3.diff",
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.text

    def post_pr_comment(
        self, repo_full_name: str, pr_number: int, body: str
    ) -> dict:
        """Post a comment on a pull request.

        Args:
            repo_full_name: e.g., "owner/repo"
            pr_number: PR number
            body: Markdown comment body

        Returns:
            GitHub API response dict.
        """
        resp = httpx.post(
            f"{GITHUB_API_BASE}/repos/{repo_full_name}/issues/{pr_number}/comments",
            headers=self._headers(),
            json={"body": body},
            timeout=30,
        )
        resp.raise_for_status()
        logger.info(
            "Posted PR comment on %s#%s (%d chars)",
            repo_full_name, pr_number, len(body),
        )
        return resp.json()

    def get_pr_files(self, repo_full_name: str, pr_number: int) -> list:
        """List files changed in a PR.

        Returns:
            List of file dicts with filename, status, additions, deletions.
        """
        resp = httpx.get(
            f"{GITHUB_API_BASE}/repos/{repo_full_name}/pulls/{pr_number}/files",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
