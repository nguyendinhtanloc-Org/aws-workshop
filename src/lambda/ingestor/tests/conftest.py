"""Pytest setup for this test directory."""
import os
import sys

_PACKAGE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, _PACKAGE_ROOT)
