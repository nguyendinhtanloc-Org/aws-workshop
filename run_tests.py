#!/usr/bin/env python3
"""Run the unit test suite without needing pytest installed.

This is the "zero-dependency" test runner: it discovers `test_*` functions
in the test modules, injects a minimal `pytest` shim (enough for
`pytest.raises`), and reports pass/fail.

Why it exists: the Lambda code is deployed as a zip with only its own
`requirements.txt`, so there is no pytest in the runtime. This script
lets you verify the pure-Python logic (validation, transformation,
idempotent writes, retry behaviour, data-quality rules) with nothing but
the standard library - useful on a fresh machine, in a Lambda-like
sandbox, or in CI before the full toolchain is set up.

If you have pytest installed, prefer it - it gives better output and
proper fixture support:

    pytest -m "not integration and not e2e"

Usage:
    python3 run_tests.py            # run everything
    python3 run_tests.py -v         # show each test name
"""
from __future__ import annotations

import argparse
import datetime as _dt
import importlib.util
import logging
import os
import re
import sys
import types

if not hasattr(_dt, "UTC"):  # Python 3.10 fallback
    _dt.UTC = _dt.timezone.utc

ROOT = os.path.dirname(os.path.abspath(__file__))


# --- Minimal pytest shim --------------------------------------------------
class _Raises:
    def __init__(self, expected: type[BaseException], match: str | None = None) -> None:
        self.expected = expected
        self.match = match
        self.exc: BaseException | None = None

    def __enter__(self) -> "_Raises":
        return self

    def __exit__(self, et, ev, tb) -> bool:
        if et is None:
            raise AssertionError(f"DID NOT RAISE {self.expected}")
        if not issubclass(et, self.expected):
            return False
        if self.match and not re.search(self.match, str(ev)):
            raise AssertionError(f"pattern {self.match!r} not found in {ev!r}")
        self.exc = ev
        return True


class _Mark:
    def __getattr__(self, _name: str):
        def deco(f=None, **_kw):
            if f is None:
                return lambda g: g
            return f

        return deco


def _skip(reason: str = "") -> None:
    raise _Skip(reason)


class _Skip(Exception):
    pass


def _install_pytest_shim() -> None:
    """Register a fake `pytest` module so `import pytest` works."""
    if "pytest" in sys.modules:
        return
    mod = types.ModuleType("pytest")
    mod.raises = lambda expected, match=None: _Raises(expected, match)
    mod.mark = _Mark()
    mod.skip = _skip
    mod.fail = lambda msg="": (_ for _ in ()).throw(AssertionError(msg))
    mod.fixture = lambda f=None, **_kw: (f if f is not None else (lambda g: g))
    sys.modules["pytest"] = mod


def _install_paths() -> None:
    """Make package modules importable the way the Lambda runtime sees them."""
    # Each Lambda package is deployed as its own zip, so its modules are
    # top-level at runtime. Mirror that here.
    for rel in ("src/glue", "src/lambda/processor", "src/lambda/ingestor"):
        path = os.path.join(ROOT, rel)
        if path not in sys.path:
            sys.path.insert(0, path)


TEST_FILES = [
    "src/lambda/ingestor/tests/test_validator.py",
    "src/lambda/ingestor/tests/test_ghn_client.py",
    "src/lambda/ingestor/tests/test_config.py",
    "src/lambda/ingestor/tests/test_handler.py",
    "src/lambda/processor/tests/test_transformer.py",
    "src/lambda/processor/tests/test_ddb_writer.py",
    "src/glue/tests/test_quality.py",
    "src/glue/tests/test_partitioning.py",
    "src/glue/tests/test_schema.py",
]


def _load(path: str) -> types.ModuleType:
    name = f"_t_{abs(hash(path))}"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-v", "--verbose", action="store_true", help="print each test")
    args = parser.parse_args()

    _install_pytest_shim()
    _install_paths()
    # The client deliberately logs its retries; silence that so the test
    # output shows only results.
    logging.disable(logging.CRITICAL)

    passed = 0
    failed: list[str] = []

    for rel in TEST_FILES:
        path = os.path.join(ROOT, rel)
        if not os.path.exists(path):
            failed.append(f"{rel}: file not found")
            continue
        try:
            mod = _load(path)
        except Exception as exc:
            failed.append(f"{rel}: import error {type(exc).__name__}: {exc}")
            continue

        for name in sorted(dir(mod)):
            if not name.startswith("test_"):
                continue
            fn = getattr(mod, name)
            if not callable(fn) or not hasattr(fn, "__code__"):
                continue
            try:
                fn()
            except Exception as exc:
                failed.append(f"{rel}::{name}: {type(exc).__name__}: {exc}")
                if args.verbose:
                    print(f"  FAIL {rel}::{name}")
                    print(f"       {type(exc).__name__}: {exc}")
            else:
                passed += 1
                if args.verbose:
                    print(f"  PASS {rel}::{name}")

    total = passed + len(failed)
    print(f"\n=== {passed}/{total} passed ===")
    for f in failed:
        print(f"  FAIL {f}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
