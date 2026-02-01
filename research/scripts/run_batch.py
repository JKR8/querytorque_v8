#!/usr/bin/env python3
"""Batch runner for DeepSeek/Kimi API optimization."""

import os
import sys
import json
import time
from pathlib import Path
from openai import OpenAI

# Configuration
PROMPTS_DIR = Path(__file__).parent.parent / "agentic_optimizer" / "batch_prompts"
OUTPUT_DIR = Path(__file__).parent / "responses"

# API configs
APIS = {
    "deepseek": {
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-reasoner",
        "env_key": "DEEPSEEK_API_KEY",
    },
    "kimi": {
        "base_url": "https://api.moonshot.cn/v1",
        "model": "kimi-k2.5",
        "env_key": "KIMI_API_KEY",
    },
}


def call_api(prompt: str, api: str = "deepseek", max_tokens: int = 32000) -> dict:
    """Call LLM API and return response."""
    config = APIS[api]
    api_key = os.environ.get(config["env_key"])

    if not api_key:
        raise ValueError(f"Set {config['env_key']} environment variable")

    client = OpenAI(api_key=api_key, base_url=config["base_url"])

    start = time.time()
    response = client.chat.completions.create(
        model=config["model"],
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        temperature=0.0,
    )
    elapsed = time.time() - start

    content = response.choices[0].message.content
    usage = response.usage

    return {
        "content": content,
        "model": config["model"],
        "elapsed": elapsed,
        "input_tokens": usage.prompt_tokens if usage else 0,
        "output_tokens": usage.completion_tokens if usage else 0,
    }


def process_query(qnum: int, api: str = "deepseek") -> dict:
    """Process a single query."""
    prompt_file = PROMPTS_DIR / f"q{qnum}_prompt.txt"

    if not prompt_file.exists():
        return {"error": f"No prompt file: {prompt_file}"}

    prompt = prompt_file.read_text()

    try:
        result = call_api(prompt, api=api)

        # Save response
        OUTPUT_DIR.mkdir(exist_ok=True)
        output_file = OUTPUT_DIR / f"q{qnum}_{api}.json"

        with open(output_file, "w") as f:
            json.dump({
                "query": qnum,
                "api": api,
                "model": result["model"],
                "elapsed": result["elapsed"],
                "input_tokens": result["input_tokens"],
                "output_tokens": result["output_tokens"],
                "response": result["content"],
            }, f, indent=2)

        return {
            "query": qnum,
            "elapsed": result["elapsed"],
            "tokens": result["output_tokens"],
            "saved": str(output_file),
        }

    except Exception as e:
        return {"query": qnum, "error": str(e)}


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run DeepSeek/Kimi on TPC-DS queries")
    parser.add_argument("--api", choices=["deepseek", "kimi"], default="deepseek")
    parser.add_argument("--query", "-q", type=int, help="Single query number")
    parser.add_argument("--all", action="store_true", help="Run all queries 1-23")
    parser.add_argument("--range", "-r", type=str, help="Range like 1-10")
    args = parser.parse_args()

    queries = []
    if args.query:
        queries = [args.query]
    elif args.range:
        start, end = map(int, args.range.split("-"))
        queries = list(range(start, end + 1))
    elif args.all:
        queries = list(range(1, 24))
    else:
        parser.print_help()
        return

    print(f"Running {len(queries)} queries with {args.api}")
    print(f"Output dir: {OUTPUT_DIR}")
    print()

    for qnum in queries:
        print(f"Q{qnum}...", end=" ", flush=True)
        result = process_query(qnum, api=args.api)

        if "error" in result:
            print(f"ERROR: {result['error']}")
        else:
            print(f"OK ({result['elapsed']:.1f}s, {result['tokens']} tokens)")

    print("\nDone!")


if __name__ == "__main__":
    main()
