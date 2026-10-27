#!/usr/bin/env python3
"""Build the deployable Lambda packages into build/.

CDK zips whatever directory it is given. The Lambda python runtime
ships boto3 but NOT requests, so the Ingestor (which imports requests)
must have its pip dependencies baked into its zip. This script does
that without Docker: it copies each function's top-level source files
into build/<name>/ and pip-installs the function's requirements.txt
into the same directory, so the zip is a self-contained package.

Run before every `cdk deploy`:

    python3 scripts/build_lambdas.py

Uses the active interpreter's pip (run it from your venv).
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# build dir name -> lambda source dir
FUNCTIONS = {
    "lambda-ingestor": "src/lambda/ingestor",
    "lambda-processor": "src/lambda/processor",
}


def build(name: str, source_dir: Path) -> Path:
    out = ROOT / "build" / name
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)

    # Top-level source only: tests, __pycache__ and anything else stay
    # out of the deployed zip on purpose.
    for py in sorted(source_dir.glob("*.py")):
        shutil.copy2(py, out / py.name)

    reqs = source_dir / "requirements.txt"
    if reqs.exists():
        print(f"[{name}] pip install -r {reqs.name} -> {out.relative_to(ROOT)}/")
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q",
             "-r", str(reqs), "--target", str(out), "--no-cache-dir"],
            check=True,
        )
    print(f"[{name}] built {out.relative_to(ROOT)}/")
    return out


def main() -> int:
    for name, rel in FUNCTIONS.items():
        build(name, ROOT / rel)
    print("\nbuild/ ready - run cdk deploy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
