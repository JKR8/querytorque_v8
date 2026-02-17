from __future__ import annotations

import importlib


def _reload_rate_limit():
    import qt_shared.llm.rate_limit as rate_limit

    return importlib.reload(rate_limit)


def test_llm_call_guard_uses_default_300_slots(monkeypatch) -> None:
    monkeypatch.delenv("QT_LLM_MAX_CONCURRENT_CALLS", raising=False)
    monkeypatch.setenv("QT_LLM_CALL_STAGGER_SECONDS", "0")
    rate_limit = _reload_rate_limit()

    with rate_limit.llm_call_guard():
        assert rate_limit._SLOT_COUNT == 300


def test_llm_call_guard_honors_custom_slot_count(monkeypatch) -> None:
    monkeypatch.setenv("QT_LLM_MAX_CONCURRENT_CALLS", "17")
    monkeypatch.setenv("QT_LLM_CALL_STAGGER_SECONDS", "0")
    rate_limit = _reload_rate_limit()

    with rate_limit.llm_call_guard():
        assert rate_limit._SLOT_COUNT == 17
