#!/usr/bin/env python3
"""Run every offline test suite (tests/test_*.py). Each suite is a script that ends by printing "..._OK".

No network access is needed: every feed is replaced by fixtures (tests/offline.py makes any real request fail).
Exit status is non-zero when any suite fails, so `python tests/run_all.py` can gate a Docker build.
"""
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def main() -> int:
    only = set(sys.argv[1:])
    suites = sorted(p for p in HERE.glob("test_*.py") if not only or p.stem.removeprefix("test_") in only)
    failed = []
    for path in suites:
        started = time.monotonic()
        try:
            result = subprocess.run([sys.executable, str(path)], cwd=HERE.parent, capture_output=True, text=True,
                                    timeout=300, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
            stdout, output, code = result.stdout, result.stdout + result.stderr, result.returncode
        except subprocess.TimeoutExpired:
            stdout, output, code = "", "TIMEOUT after 300 s", -1
        lines = output.strip().splitlines()
        ok = code == 0 and result_ok(stdout)
        print(f"{'PASS' if ok else 'FAIL'} {path.name} ({time.monotonic() - started:.1f}s)")
        if not ok:
            failed.append(path.name)
            print("\n".join("    " + line for line in lines[-30:]))
    print(f"\n{len(suites) - len(failed)}/{len(suites)} suites passed")
    return 1 if failed or not suites else 0


def result_ok(stdout: str) -> bool:
    out = stdout.strip().splitlines()
    return bool(out) and out[-1].strip().endswith("_OK")


if __name__ == "__main__":
    sys.exit(main())
