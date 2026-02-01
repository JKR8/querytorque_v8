#!/usr/bin/env python3
"""Quick speed test: OpenRouter DeepSeek vs Direct DeepSeek API."""

import time
import os
from pathlib import Path
from openai import OpenAI

PROMPT = """Optimize this SQL query. Return only the optimized SQL.

SELECT * FROM orders o, customers c
WHERE o.customer_id = c.id AND c.country = 'USA'
ORDER BY o.created_at DESC LIMIT 10;"""

def test_openrouter_deepseek(api_key: str) -> tuple[str, float]:
    """Test DeepSeek via OpenRouter."""
    client = OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )
    start = time.time()
    response = client.chat.completions.create(
        model="deepseek/deepseek-v3.2",
        messages=[{"role": "user", "content": PROMPT}],
        temperature=0.1,
    )
    elapsed = time.time() - start
    return response.choices[0].message.content, elapsed


def test_direct_deepseek(api_key: str) -> tuple[str, float]:
    """Test DeepSeek direct API."""
    client = OpenAI(
        base_url="https://api.deepseek.com",
        api_key=api_key,
    )
    start = time.time()
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[{"role": "user", "content": PROMPT}],
        temperature=0.1,
    )
    elapsed = time.time() - start
    return response.choices[0].message.content, elapsed


def main():
    # Load API keys
    openrouter_key = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/openrouter.txt").read_text().strip()

    deepseek_key = os.environ.get("DEEPSEEK_API_KEY", "")
    if not deepseek_key:
        for path in [Path.home() / ".deepseek_api_key", Path("/mnt/c/Users/jakc9/.deepseek_api_key")]:
            if path.exists():
                deepseek_key = path.read_text().strip()
                break

    print("="*50)
    print("DeepSeek Speed Test: OpenRouter vs Direct")
    print("="*50)

    # Test OpenRouter
    print("\n1. OpenRouter (deepseek/deepseek-chat)...")
    try:
        _, or_time = test_openrouter_deepseek(openrouter_key)
        print(f"   Time: {or_time:.2f}s")
    except Exception as e:
        print(f"   Error: {e}")
        or_time = None

    # Test Direct
    if deepseek_key:
        print("\n2. Direct API (deepseek-chat)...")
        try:
            _, direct_time = test_direct_deepseek(deepseek_key)
            print(f"   Time: {direct_time:.2f}s")
        except Exception as e:
            print(f"   Error: {e}")
            direct_time = None
    else:
        print("\n2. Direct API: SKIPPED (no DEEPSEEK_API_KEY)")
        direct_time = None

    # Compare
    print("\n" + "="*50)
    print("RESULT:")
    print("="*50)
    if or_time and direct_time:
        faster = "Direct" if direct_time < or_time else "OpenRouter"
        diff = abs(or_time - direct_time)
        pct = (diff / max(or_time, direct_time)) * 100
        print(f"  OpenRouter: {or_time:.2f}s")
        print(f"  Direct:     {direct_time:.2f}s")
        print(f"  Winner:     {faster} (by {diff:.2f}s / {pct:.0f}%)")
    elif or_time:
        print(f"  OpenRouter: {or_time:.2f}s")
        print(f"  Direct:     N/A")


if __name__ == "__main__":
    main()
