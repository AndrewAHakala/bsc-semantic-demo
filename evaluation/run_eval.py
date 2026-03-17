#!/usr/bin/env python3
"""
run_eval.py — Order Status Assistant evaluation harness.

Runs the golden prompt set against the live API and computes:
  - accuracy@1, accuracy@5
  - p50 / p95 total latency
  - per-prompt pass/fail details

Usage:
  python evaluation/run_eval.py [--api-url http://localhost:8000] [--output results.json]
"""

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx

EVAL_DIR = Path(__file__).parent
PROMPTS_FILE = EVAL_DIR / "datasets" / "golden_prompts.jsonl"
EXPECTED_FILE = EVAL_DIR / "datasets" / "expected_results.jsonl"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_jsonl(path: Path) -> List[Dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


# ---------------------------------------------------------------------------
# Check evaluation
# ---------------------------------------------------------------------------

def _get_top_n_results(response: Dict, n: int) -> List[Dict]:
    return response.get("results", [])[:n]


def _normalize(text: str) -> str:
    import re, unicodedata
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return text


def evaluate_checks(
    checks: List[Dict],
    response: Dict,
):
    """Returns (passed: bool, details: List[str])"""
    results_all = response.get("results", [])
    passed_checks = []
    failed_checks = []

    for check in checks:
        ctype = check["type"]
        top_n = check.get("in_top_n", 5)
        top_results = results_all[:top_n]

        if ctype == "exact_order_id":
            target = check["order_id"]
            found = any(r.get("order_id") == target for r in top_results)
            (passed_checks if found else failed_checks).append(
                f"exact_order_id={target} in top_{top_n}: {'✅' if found else '❌'}"
            )

        elif ctype == "order_id_suffix":
            suffix = check["value"]
            found = any(r.get("order_id", "").endswith(suffix) for r in top_results)
            (passed_checks if found else failed_checks).append(
                f"order_id_suffix={suffix} in top_{top_n}: {'✅' if found else '❌'}"
            )

        elif ctype == "po_suffix_in_result":
            suffix = check["suffix"]
            found = any(
                (r.get("purchase_order_id") or "").endswith(suffix) for r in top_results
            )
            (passed_checks if found else failed_checks).append(
                f"po_suffix={suffix} in top_{top_n}: {'✅' if found else '❌'}"
            )

        elif ctype == "facility_token_in_result":
            token = check["token"].lower()
            found = any(token in _normalize(r.get("facility_name", "")) for r in top_results)
            (passed_checks if found else failed_checks).append(
                f"facility_token={token} in top_{top_n}: {'✅' if found else '❌'}"
            )

        elif ctype == "customer_token_in_result":
            token = check["token"].lower()
            found = any(token in _normalize(r.get("customer_name", "")) for r in top_results)
            (passed_checks if found else failed_checks).append(
                f"customer_token={token} in top_{top_n}: {'✅' if found else '❌'}"
            )

        elif ctype == "field_not_null":
            field = check["field"]
            shipped_only = check.get("for_shipped_only", False)
            subset = [
                r for r in top_results
                if not shipped_only or r.get("status") in ("SHIPPED", "DELIVERED")
            ]
            found = any(r.get(field) is not None for r in subset)
            (passed_checks if found else failed_checks).append(
                f"field_not_null={field}: {'✅' if found else '❌'}"
            )

    all_passed = len(failed_checks) == 0
    return all_passed, passed_checks + failed_checks


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_single(
    client: httpx.Client,
    api_url: str,
    prompt: Dict,
) -> Dict:
    payload: Dict[str, Any] = {
        "mode": prompt["mode"],
        "top_n": 5,
    }
    if prompt.get("free_text"):
        payload["free_text"] = prompt["free_text"]
    if prompt.get("fields"):
        payload["fields"] = prompt["fields"]

    t0 = time.perf_counter()
    try:
        resp = client.post(f"{api_url}/search/orders", json=payload, timeout=30.0)
        resp.raise_for_status()
        data = resp.json()
        latency_ms = (time.perf_counter() - t0) * 1000
        api_latency = data.get("timings_ms", {}).get("total_ms", latency_ms)
        return {"ok": True, "response": data, "latency_ms": api_latency, "error": None}
    except Exception as exc:
        return {"ok": False, "response": {}, "latency_ms": (time.perf_counter() - t0) * 1000, "error": str(exc)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--output", default=None, help="Write JSON results to file")
    args = parser.parse_args()

    prompts = load_jsonl(PROMPTS_FILE)
    expected = {e["id"]: e for e in load_jsonl(EXPECTED_FILE)}

    print(f"\n=== Order Status Assistant — Evaluation Harness ===")
    print(f"API: {args.api_url}")
    print(f"Prompts: {len(prompts)}\n")

    results = []
    latencies = []
    passed_count = 0
    failed_count = 0

    with httpx.Client() as client:
        for prompt in prompts:
            pid = prompt["id"]
            print(f"[{pid}] {(prompt.get('free_text') or str(prompt.get('fields','')))[:70]}")

            run = run_single(client, args.api_url, prompt)
            latencies.append(run["latency_ms"])

            if not run["ok"]:
                print(f"  ❌ API ERROR: {run['error']}")
                failed_count += 1
                results.append({"id": pid, "passed": False, "error": run["error"], "latency_ms": run["latency_ms"]})
                continue

            exp = expected.get(pid, {})
            checks = exp.get("checks", [])
            passed, details = evaluate_checks(checks, run["response"])

            for detail in details:
                print(f"  {detail}")
            print(f"  Latency: {run['latency_ms']:.0f} ms  |  Candidates: {run['response'].get('candidate_count','?')}")

            if passed:
                passed_count += 1
            else:
                failed_count += 1

            results.append({
                "id": pid,
                "passed": passed,
                "details": details,
                "latency_ms": run["latency_ms"],
                "trace_id": run["response"].get("trace_id", ""),
            })
            print()

    # Summary
    total = passed_count + failed_count
    acc1 = passed_count / total if total else 0

    p50 = statistics.median(latencies) if latencies else 0
    p95 = sorted(latencies)[int(len(latencies) * 0.95)] if len(latencies) >= 2 else (latencies[0] if latencies else 0)

    print("=" * 55)
    print(f"Results:  {passed_count}/{total} passed  ({acc1*100:.1f}%)")
    print(f"p50 latency: {p50:.0f} ms")
    print(f"p95 latency: {p95:.0f} ms  {'✅ under SLO' if p95 < 5000 else '⚠️ EXCEEDS 5 s SLO'}")
    print("=" * 55)

    summary = {
        "total": total,
        "passed": passed_count,
        "failed": failed_count,
        "accuracy": round(acc1, 4),
        "p50_ms": round(p50, 1),
        "p95_ms": round(p95, 1),
        "slo_met": p95 < 5000,
        "results": results,
    }

    if args.output:
        with open(args.output, "w") as f:
            json.dump(summary, f, indent=2)
        print(f"\nResults written to {args.output}")

    sys.exit(0 if failed_count == 0 else 1)


if __name__ == "__main__":
    main()
