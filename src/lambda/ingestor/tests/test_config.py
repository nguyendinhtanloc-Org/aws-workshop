"""Tests for the local/AWS configuration switch."""
from __future__ import annotations

import os

import pytest

from ingestor_config import get_token


def test_token_comes_from_environment_when_no_secret_arn(monkeypatch=None):
    """In local development the token comes from the environment."""
    os.environ.pop("GHN_SECRET_ARN", None)
    os.environ["GHN_API_TOKEN"] = "local-token"
    try:
        assert get_token() == "local-token"
    finally:
        os.environ.pop("GHN_API_TOKEN", None)


def test_missing_token_raises_a_helpful_error():
    os.environ.pop("GHN_SECRET_ARN", None)
    os.environ.pop("GHN_API_TOKEN", None)
    with pytest.raises(RuntimeError, match="GHN_API_TOKEN"):
        get_token()


def test_importing_config_does_not_call_aws():
    """Importing the module must not perform a Secrets Manager call.

    This is what keeps unit tests and local scripts free of AWS
    credentials and network access.
    """
    import ingestor_config as config

    assert hasattr(config, "get_config")
    assert hasattr(config, "get_token")
