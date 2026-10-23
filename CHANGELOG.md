# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Initial CDK infrastructure: base, ingestion, streaming, processing,
  analytics and monitoring stacks.
- Lambda Ingestor with GHN API client, retry/backoff and schema validation.
- Lambda Processor with idempotent DynamoDB writes.
- Glue ETL jobs for bronze-to-silver and silver-to-gold layers.
- Data quality rules and unit test suite.
- GitHub Actions CI (lint, typecheck, test, CDK synth, secret scan).
- Operational scripts: backfill, DLQ replay, environment validation.
