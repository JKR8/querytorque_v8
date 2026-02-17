from __future__ import annotations

from types import SimpleNamespace


def _install_fake_openai(monkeypatch, captured_kwargs: dict) -> None:
    class _FakeCompletions:
        @staticmethod
        def create(**kwargs):
            captured_kwargs.update(kwargs)
            usage = SimpleNamespace(
                prompt_tokens=10,
                completion_tokens=5,
                total_tokens=15,
                prompt_cache_hit_tokens=0,
                prompt_cache_miss_tokens=0,
            )
            message = SimpleNamespace(content='{"ok":true}')
            choice = SimpleNamespace(message=message)
            return SimpleNamespace(choices=[choice], usage=usage)

    class _FakeChat:
        completions = _FakeCompletions()

    class _FakeOpenAIClient:
        def __init__(self, api_key: str, base_url: str | None = None):
            self.api_key = api_key
            self.base_url = base_url
            self.chat = _FakeChat()

    fake_module = SimpleNamespace(OpenAI=_FakeOpenAIClient)
    monkeypatch.setitem(__import__("sys").modules, "openai", fake_module)


def test_openrouter_v32_enables_reasoning_payload(monkeypatch) -> None:
    from qt_shared.llm.openai import OpenAIClient

    captured = {}
    _install_fake_openai(monkeypatch, captured)
    monkeypatch.setenv("QT_OPENROUTER_REASONING_EFFORT", "high")

    client = OpenAIClient(
        api_key="test",
        model="deepseek/deepseek-v3.2",
        base_url="https://openrouter.ai/api/v1",
    )
    response = client.analyze("Return JSON")

    assert response == '{"ok":true}'
    assert "extra_body" in captured
    reasoning = captured["extra_body"].get("reasoning", {})
    assert reasoning.get("enabled") is True
    assert reasoning.get("effort") == "high"


def test_non_v32_model_does_not_add_reasoning_payload(monkeypatch) -> None:
    from qt_shared.llm.openai import OpenAIClient

    captured = {}
    _install_fake_openai(monkeypatch, captured)

    client = OpenAIClient(
        api_key="test",
        model="gpt-4o",
        base_url="https://openrouter.ai/api/v1",
    )
    client.analyze("Return JSON")

    assert "extra_body" not in captured
