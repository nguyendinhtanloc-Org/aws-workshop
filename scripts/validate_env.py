#!/usr/bin/env python3
"""Pre-deploy sanity checks. Fails fast with an actionable message."""
from __future__ import annotations

import os
import sys

REQUIRED = {
    "AWS_DEFAULT_REGION": "region to deploy into (e.g. ap-southeast-1)",
    "AWS_DEFAULT_ACCOUNT": "AWS account id (set by the CDK CLI profile)",
}

OPTIONAL = {
    "GHN_API_TOKEN": "GHN API token, only needed if you seed the secret from this shell",
}


def main() -> int:
    missing = []
    for key, hint in REQUIRED.items():
        if not os.environ.get(key):
            missing.append(f"  - {key}: {hint}")

    if missing:
        print("Missing required environment variables:\n" + "\n".join(missing))
        print("\nRun `aws configure` or export the variables before deploying.")
        return 1

    print("Environment OK")
    for key in OPTIONAL:
        state = "set" if os.environ.get(key) else "not set"
        print(f"  {key}: {state}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
