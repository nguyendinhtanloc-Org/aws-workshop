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
        print(f"[{name}] pip install (linux/amd64...aarch64, cp312) -> {out.relative_to(ROOT)}/")
        # Cross-target the install at the exact target of the deployed
        # runtime: Lambda ARM_64 = Linux aarch64 + CPython 3.12 (the
        # pinned Lambda runtime). Without --platform/--python-version,
        # pip would pick wheels for the *local* machine (macOS arm64,
        # Python 3.13 on a dev box) and the Lambda import would fail with
        # an incompatible-binary error.
        # --only-binary=:all: forces resolution against wheel tags only,
        # which is what makes cross-targeting safe.
        # The pip timeouts / retries below are widened for low-bandwidth
        # links (a 15 s read timeout on a slow or congested connection
        # aborts the metadata fetch midway and surfaces as the
        # confusing "No matching distribution found for
        # charset-normalizer" error even though the only real problem
        # is a flaky download).
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q",
             "-r", str(reqs), "--target", str(out), "--no-cache-dir",
             "--platform", "manylinux2014_aarch64",
             "--python-version", "3.12",
             "--implementation", "cp",
             "--abi", "cp312",
             "--only-binary", ":all:",
             "--timeout", "60",
             "--retries", "10"],
            check=True,
        )
    _verify_wheel_tags(out)
    print(f"[{name}] built {out.relative_to(ROOT)}/")
    return out


def _verify_wheel_tags(out: Path) -> None:
    """Fail early if a wrong-platform wheel snuck into the package."""
    bad = []
    for wheel in out.glob("*/*.dist-info/WHEEL"):
        for line in wheel.read_text().splitlines():
            if not line.startswith("Tag:"):
                continue
            tag = line.split(":", 1)[1].strip()
            if "macosx" in tag or tag.endswith(tuple(f"cp{v}" for v in range(313, 320))):
                bad.append((wheel.parent.name, tag))
    if bad:
        raise SystemExit(
            f"wrong-platform wheels in {out.name}: {bad!r}. "
            f"The cross-target pip flags may have changed; check them."
        )


def main() -> int:
    for name, rel in FUNCTIONS.items():
        build(name, ROOT / rel)
    print("\nbuild/ ready - run cdk deploy")
    return 0


if __name__ == "__main__":
    sys.exit(main())
