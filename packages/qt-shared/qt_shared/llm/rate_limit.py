"""Process-local LLM call staggering utilities."""

from __future__ import annotations

from contextlib import contextmanager
import os
import threading
import time

_STAGGER_LOCK = threading.Lock()
_NEXT_CALL_AT = 0.0
_SLOT_LOCK = threading.Lock()
_SLOTS: threading.BoundedSemaphore | None = None
_SLOT_COUNT: int | None = None


def _stagger_seconds() -> float:
    raw = os.environ.get("QT_LLM_CALL_STAGGER_SECONDS", "1.0").strip()
    try:
        value = float(raw)
    except Exception:
        value = 1.0
    if value < 0:
        return 0.0
    return value


def _max_concurrent_calls() -> int:
    raw = os.environ.get("QT_LLM_MAX_CONCURRENT_CALLS", "300").strip()
    try:
        value = int(raw)
    except Exception:
        value = 300
    return max(1, value)


def _slots() -> threading.BoundedSemaphore:
    global _SLOTS, _SLOT_COUNT
    desired = _max_concurrent_calls()
    with _SLOT_LOCK:
        if _SLOTS is None or _SLOT_COUNT != desired:
            _SLOTS = threading.BoundedSemaphore(desired)
            _SLOT_COUNT = desired
        return _SLOTS


def stagger_llm_call() -> None:
    """Enforce a minimum interval between outbound LLM call starts."""
    global _NEXT_CALL_AT

    interval = _stagger_seconds()
    if interval <= 0:
        return

    with _STAGGER_LOCK:
        now = time.monotonic()
        wait_s = max(0.0, _NEXT_CALL_AT - now)
        if wait_s > 0:
            time.sleep(wait_s)
            now = time.monotonic()
        _NEXT_CALL_AT = max(now, _NEXT_CALL_AT) + interval


@contextmanager
def llm_call_guard():
    """Acquire a global call slot and apply start-time staggering."""
    sem = _slots()
    sem.acquire()
    try:
        stagger_llm_call()
        yield
    finally:
        sem.release()
