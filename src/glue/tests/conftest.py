"""Pytest setup for this test directory.

The Lambda package is deployed to AWS as a standalone zip, so its modules
import each other at the top level (``transformer``, ``ghn_client``,
``lib.quality``). Mirror that here by putting the package root on
``sys.path`` - this is what makes those imports resolve when pytest runs
from the repository root.

This directory deliberately has no ``__init__.py``. Several test
directories in this repo are all named ``tests``; without ``__init__.py``
pytest imports each by its own path instead of as a package, so they do
not collide in ``sys.modules``.
"""
import os
import sys

_PACKAGE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if _PACKAGE_ROOT not in sys.path:
    sys.path.insert(0, _PACKAGE_ROOT)
