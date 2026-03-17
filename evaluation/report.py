#!/usr/bin/env python3
"""
report.py — Pretty-print a results JSON file produced by run_eval.py.

Usage:
  python evaluation/report.py results.json
"""

import json
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 2:
        print("Usage: python report.py <results.json>")
        sys.exit(1)

    path = Path(sys.argv[1])
    data = json.loads(path.read_text())

    print(f"\n{'='*60}")
    print("  Order Status Assistant — Evaluation Report")
    print(f"{'='*60}")
    print(f"  Total prompts : {data['total']}")
    print(f"  Passed        : {data['passed']}")
    print(f"  Failed        : {data['failed']}")
    print(f"  Accuracy      : {data['accuracy']*100:.1f}%")
    print(f"  p50 latency   : {data['p50_ms']:.0f} ms")
    print(f"  p95 latency   : {data['p95_ms']:.0f} ms  {'✅' if data['slo_met'] else '⚠️ EXCEEDS SLO'}")
    print(f"{'='*60}\n")

    failed = [r for r in data["results"] if not r.get("passed")]
    if failed:
        print(f"FAILURES ({len(failed)}):\n")
        for r in failed:
            print(f"  [{r['id']}]  {r.get('error', '')}")
            for d in r.get("details", []):
                if "❌" in d:
                    print(f"    {d}")
            print()
    else:
        print("  All checks passed! 🎉\n")


if __name__ == "__main__":
    main()
