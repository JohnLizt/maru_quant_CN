"""Query ETF CN top rotation candidates from composite signal scores."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def main(date: str | None, top_n: int) -> int:
    command = [
        sys.executable,
        str(REPO_ROOT / "app" / "cli" / "query_signal_scores.py"),
        "--asset-type",
        "etf_CN",
        "--top",
        str(top_n),
        "--format",
        "json",
    ]
    if date:
        command.extend(["--date", date])
    result = subprocess.run(command, cwd=REPO_ROOT, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        error_payload = {
            "error": "query_signal_scores_failed",
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        print(json.dumps(error_payload, ensure_ascii=False, indent=2))
        return result.returncode

    payload = result.stdout.strip()
    if payload:
        print(payload)
    else:
        print(json.dumps({"query": {"asset_type": "etf_CN", "top_n": top_n}, "row_count": 0, "results": []}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Query ETF CN rotation candidates")
    parser.add_argument("--date", default=None, help="单日查询日期 YYYY-MM-DD")
    parser.add_argument("--top", type=int, default=10, help="返回前 N 名，默认 10")
    args = parser.parse_args()
    raise SystemExit(main(args.date, args.top))
