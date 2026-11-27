"""Unit tests for cross-OS local Lambda packaging and deployment helpers."""
from __future__ import annotations

import io
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import build_lambdas  # noqa: E402
import deploy_processor_local as deploy  # noqa: E402


def test_normalize_lambda_arch() -> None:
    cases = (
        ("x86_64", "x86_64"),
        ("AMD64", "x86_64"),
        ("amd64", "x86_64"),
        ("x64", "x86_64"),
        ("arm64", "arm64"),
        ("ARM64", "arm64"),
        ("aarch64", "arm64"),
        ("arm64/v8", "arm64"),
    )
    for raw, expected in cases:
        assert deploy.normalize_lambda_arch(raw) == expected


def test_normalize_lambda_arch_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="Unsupported architecture"):
        deploy.normalize_lambda_arch("riscv64")


def test_detect_local_lambda_arch_prefers_env_override() -> None:
    with (
        patch.dict(os.environ, {"LOCAL_LAMBDA_ARCH": "AMD64"}, clear=False),
        patch.object(deploy.shutil, "which") as which,
    ):
        assert deploy.detect_local_lambda_arch() == "x86_64"
    which.assert_not_called()


def test_detect_local_lambda_arch_uses_docker_info() -> None:
    fake_proc = SimpleNamespace(returncode=0, stdout="aarch64\n")
    env = {k: v for k, v in os.environ.items() if k != "LOCAL_LAMBDA_ARCH"}
    with (
        patch.dict(os.environ, env, clear=True),
        patch.object(deploy.shutil, "which", return_value="/usr/bin/docker"),
        patch.object(deploy.subprocess, "run", return_value=fake_proc),
        patch.object(deploy.platform, "machine", return_value="x86_64"),
    ):
        assert deploy.detect_local_lambda_arch() == "arm64"


def test_detect_local_lambda_arch_falls_back_to_host_machine() -> None:
    env = {k: v for k, v in os.environ.items() if k != "LOCAL_LAMBDA_ARCH"}
    with (
        patch.dict(os.environ, env, clear=True),
        patch.object(deploy.shutil, "which", return_value=None),
        patch.object(deploy.platform, "machine", return_value="AMD64"),
    ):
        assert deploy.detect_local_lambda_arch() == "x86_64"


def test_ensure_lambda_runtime_image_skips_pull_when_cached_arch_matches() -> None:
    probe_ok = SimpleNamespace(returncode=0, stdout="x86_64\n")
    with (
        patch.object(deploy.shutil, "which", return_value="docker"),
        patch.object(deploy.subprocess, "run", return_value=probe_ok) as run,
    ):
        deploy.ensure_lambda_runtime_image("x86_64")
    assert run.call_count == 1
    assert "--pull=never" in run.call_args.args[0]


def test_ensure_lambda_runtime_image_replaces_mismatched_cached_image() -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(cmd)
        if cmd[1] == "run":
            return SimpleNamespace(returncode=255, stdout="")
        return SimpleNamespace(returncode=0, stdout="")

    with (
        patch.object(deploy.shutil, "which", return_value="docker"),
        patch.object(deploy.subprocess, "run", side_effect=fake_run),
    ):
        deploy.ensure_lambda_runtime_image("x86_64")

    assert [c[1] for c in calls] == ["run", "rmi", "pull"]
    assert calls[2] == [
        "docker",
        "pull",
        "--platform",
        "linux/amd64",
        deploy.LAMBDA_RUNTIME_IMAGE,
    ]


def test_ensure_lambda_runtime_image_replaces_emulated_wrong_arch() -> None:
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        calls.append(cmd)
        if cmd[1] == "run":
            return SimpleNamespace(returncode=0, stdout="aarch64\n")
        return SimpleNamespace(returncode=0, stdout="")

    with (
        patch.object(deploy.shutil, "which", return_value="docker"),
        patch.object(deploy.subprocess, "run", side_effect=fake_run),
    ):
        deploy.ensure_lambda_runtime_image("x86_64")

    assert [c[1] for c in calls] == ["run", "rmi", "pull"]


def test_zip_directory_sets_posix_permissions_and_posix_paths() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        sub = root / "shared"
        sub.mkdir()
        (root / "handler.py").write_text("print('ok')\n", encoding="utf-8")
        (sub / "trino_client.py").write_text("x = 1\n", encoding="utf-8")

        raw = deploy.zip_directory(root)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            names = archive.namelist()
            assert names == ["handler.py", "shared/trino_client.py"]
            for info in archive.infolist():
                assert (info.external_attr >> 16) == 0o644


def test_verify_wheel_tags_checks_target_architecture() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        dist_info = root / "pkg" / "pkg-1.0.dist-info"
        dist_info.mkdir(parents=True)
        wheel = dist_info / "WHEEL"

        wheel.write_text("Tag: py3-none-any\n", encoding="utf-8")
        build_lambdas._verify_wheel_tags(root, arch="arm64")
        build_lambdas._verify_wheel_tags(root, arch="x86_64")

        wheel.write_text("Tag: cp312-cp312-manylinux2014_x86_64\n", encoding="utf-8")
        build_lambdas._verify_wheel_tags(root, arch="x86_64")
        with pytest.raises(SystemExit):
            build_lambdas._verify_wheel_tags(root, arch="arm64")

        wheel.write_text("Tag: cp312-cp312-win_amd64\n", encoding="utf-8")
        with pytest.raises(SystemExit):
            build_lambdas._verify_wheel_tags(root, arch="x86_64")


def test_ensure_lambda_runtime_image_raises_on_pull_failure() -> None:
    def fake_run(cmd: list[str], **kwargs: object) -> SimpleNamespace:
        if cmd[1] == "run":
            return SimpleNamespace(returncode=125, stdout="")
        if cmd[1] == "pull":
            raise subprocess.CalledProcessError(1, cmd)
        return SimpleNamespace(returncode=0, stdout="")

    with (
        patch.object(deploy.shutil, "which", return_value="docker"),
        patch.object(deploy.subprocess, "run", side_effect=fake_run),
        pytest.raises(subprocess.CalledProcessError),
    ):
        deploy.ensure_lambda_runtime_image("arm64")
