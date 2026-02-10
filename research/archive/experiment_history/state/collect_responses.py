#!/usr/bin/env python3
"""
Send all 99 TPC-DS prompts to DeepSeek Reasoner in parallel. Save responses.
No validation — just collect.

Usage:
    python3 research/state/collect_responses.py
"""

import sys
import json
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
sys.path.insert(0, str(PROJECT / "packages" / "qt-sql"))
sys.path.insert(0, str(PROJECT / "packages" / "qt-shared"))

from qt_shared.llm import create_llm_client

PROMPTS_DIR = PROJECT / "research" / "state" / "prompts"
RESPONSES_DIR = PROJECT / "research" / "state" / "responses"


def call_one(q: int, client) -> dict:
    """Send one prompt, return result dict."""
    prompt_path = PROMPTS_DIR / f"q{q}_prompt.txt"
    if not prompt_path.exists():
        return {"query": q, "status": "skip", "error": "no prompt"}

    prompt = prompt_path.read_text()
    start = time.time()
    try:
        response = client.analyze(prompt)
        duration = time.time() - start
        # Save response
        out = RESPONSES_DIR / f"q{q}_response.txt"
        out.write_text(response)
        return {"query": q, "status": "ok", "duration_s": round(duration, 1), "chars": len(response)}
    except Exception as e:
        duration = time.time() - start
        return {"query": q, "status": "error", "error": str(e), "duration_s": round(duration, 1)}


def main():
    RESPONSES_DIR.mkdir(parents=True, exist_ok=True)

    client = create_llm_client()
    if client is None:
        print("ERROR: No LLM client configured", file=sys.stderr)
        sys.exit(1)

    print(f"Provider: {client.__class__.__name__}, Model: {client.model}", file=sys.stderr)
    print(f"Sending 99 prompts in parallel...", file=sys.stderr)
    print(f"Output: {RESPONSES_DIR}", file=sys.stderr)

    start_all = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=99) as pool:
        futures = {pool.submit(call_one, q, client): q for q in range(1, 100)}

        for future in as_completed(futures):
            r = future.result()
            results.append(r)
            q = r["query"]
            if r["status"] == "ok":
                print(f"  Q{q}: OK {r['duration_s']}s ({r['chars']} chars)", file=sys.stderr)
            else:
                print(f"  Q{q}: {r['status'].upper()} - {r.get('error', '')}", file=sys.stderr)

    total_time = time.time() - start_all
    ok = sum(1 for r in results if r["status"] == "ok")
    err = sum(1 for r in results if r["status"] == "error")

    print(f"\nDone in {total_time:.0f}s: {ok} OK, {err} errors", file=sys.stderr)

    # Save summary
    summary = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_time_s": round(total_time, 1),
        "ok": ok,
        "errors": err,
        "results": sorted(results, key=lambda r: r["query"]),
    }
    summary_path = RESPONSES_DIR / "collection_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"Summary: {summary_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
