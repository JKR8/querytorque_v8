"""Fernet-based credential vault for encrypted DSN storage.

Uses AES-128-CBC via Python's cryptography.fernet module.
Key is loaded from QT_VAULT_KEY environment variable.
"""

import logging
import os

from cryptography.fernet import Fernet, InvalidToken

logger = logging.getLogger(__name__)

_fernet_instance: Fernet | None = None


def _get_fernet() -> Fernet:
    """Get or create Fernet instance from QT_VAULT_KEY."""
    global _fernet_instance
    if _fernet_instance is not None:
        return _fernet_instance

    key = os.environ.get("QT_VAULT_KEY", "")
    if not key:
        raise RuntimeError(
            "QT_VAULT_KEY not set. Generate one with: "
            "python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
        )
    _fernet_instance = Fernet(key.encode() if isinstance(key, str) else key)
    return _fernet_instance


def generate_vault_key() -> str:
    """Generate a new Fernet key for QT_VAULT_KEY."""
    return Fernet.generate_key().decode()


def encrypt_dsn(dsn: str) -> bytes:
    """Encrypt a DSN string. Returns Fernet ciphertext bytes."""
    f = _get_fernet()
    return f.encrypt(dsn.encode("utf-8"))


def decrypt_dsn(encrypted: bytes) -> str:
    """Decrypt a DSN from Fernet ciphertext bytes.

    Raises InvalidToken if key is wrong or data is corrupt.
    Never log the return value.
    """
    f = _get_fernet()
    return f.decrypt(encrypted).decode("utf-8")


def mask_dsn(dsn: str) -> str:
    """Mask password in DSN for safe logging.

    postgres://user:secret@host:5432/db -> postgres://user:***@host:5432/db
    """
    from urllib.parse import urlparse, urlunparse

    try:
        parsed = urlparse(dsn)
        if parsed.password:
            masked = parsed._replace(
                netloc=f"{parsed.username}:***@{parsed.hostname}"
                + (f":{parsed.port}" if parsed.port else "")
            )
            return urlunparse(masked)
    except Exception:
        pass
    return "***masked***"
