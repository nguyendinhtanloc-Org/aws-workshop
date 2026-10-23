.DEFAULT_GOAL := help
SHELL := /bin/bash
PY := python3
VENV := .venv
BIN := $(VENV)/bin
export PATH := $(BIN):$(PATH)

.PHONY: help venv install lint format typecheck test test-standalone test-int test-e2e cov check quick ci cdk-synth cdk-diff deploy destroy validate-env test-api clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

venv: ## Create virtualenv
	$(PY) -m venv $(VENV)

install: venv ## Install dev + CDK dependencies
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements-dev.txt
	$(BIN)/pip install -e ".[dev]"

lint: ## Ruff check
	ruff check src cdk tests scripts

format: ## Ruff format
	ruff format src cdk tests scripts

typecheck: ## Mypy
	mypy src cdk tests

test: ## Run unit tests (needs pytest)
	pytest -m "not integration and not e2e"

test-standalone: ## Run unit tests with the stdlib only (no pytest needed)
	$(PY) run_tests.py

test-int: ## Run integration tests (needs localstack)
	pytest -m integration

test-e2e: ## Run e2e tests (needs deployed stack)
	pytest -m e2e

cov: ## Test with coverage report
	pytest --cov=src --cov-report=html --cov-report=term-missing
	@echo "HTML report: htmlcov/index.html"

check: lint typecheck test ## Full local gate (same as CI)

quick: ## Fast check that works with zero third-party packages
	$(PY) run_tests.py


ci: check ## Alias for CI gate

cdk-synth: ## Synthesize CDK templates
	cdk synth --app "python cdk/app.py"

cdk-diff: ## Show infra diff vs deployed
	cdk diff --app "python cdk/app.py"

deploy: validate-env cdk-synth ## Deploy all stacks
	cdk deploy --all --app "python cdk/app.py"

destroy: ## Destroy all stacks
	cdk destroy --all --app "python cdk/app.py"

validate-env: ## Pre-deploy sanity checks
	$(PY) scripts/validate_env.py

test-api: ## Probe the GHN API with your token (read-only, no AWS needed)
	$(PY) scripts/test_api.py

clean: ## Remove build artifacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache htmlcov coverage.xml cdk.out
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
