"""Shared helpers for per-session run log handlers."""

from __future__ import annotations

import logging
from threading import Lock

_LOG_LOCK = Lock()
_ACTIVE_SESSION_HANDLERS = 0
_ORIGINAL_ROOT_LEVEL: int | None = None


def attach_session_handler(handler: logging.Handler) -> None:
    """Attach a session file handler and ensure INFO logs are emitted."""
    global _ACTIVE_SESSION_HANDLERS, _ORIGINAL_ROOT_LEVEL
    root_logger = logging.getLogger()
    with _LOG_LOCK:
        if _ACTIVE_SESSION_HANDLERS == 0:
            _ORIGINAL_ROOT_LEVEL = root_logger.level
            if root_logger.level > logging.INFO:
                root_logger.setLevel(logging.INFO)
        root_logger.addHandler(handler)
        _ACTIVE_SESSION_HANDLERS += 1


def detach_session_handler(handler: logging.Handler) -> None:
    """Detach a session file handler and restore root level when last ends."""
    global _ACTIVE_SESSION_HANDLERS, _ORIGINAL_ROOT_LEVEL
    root_logger = logging.getLogger()
    with _LOG_LOCK:
        root_logger.removeHandler(handler)
        _ACTIVE_SESSION_HANDLERS = max(0, _ACTIVE_SESSION_HANDLERS - 1)
        if _ACTIVE_SESSION_HANDLERS == 0 and _ORIGINAL_ROOT_LEVEL is not None:
            root_logger.setLevel(_ORIGINAL_ROOT_LEVEL)
            _ORIGINAL_ROOT_LEVEL = None
