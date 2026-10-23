# Contributing

## Before you start

Read `docs/architecture.md` and the relevant ADR in `docs/adr/`. Every
architectural choice in this project was recorded there — including the
alternatives that were rejected and why.

## Workflow

1. Branch off `develop` using `feat/<ticket>-<slug>` or `fix/<ticket>-<slug>`.
2. Make the change, keeping it small and single-purpose.
3. Add or update tests. A change without a test is not finished.
4. Run `make check` locally. This is exactly what CI runs.
5. Open a PR against `develop`. Fill in the whole template.
6. Get one approval. Changes touching `cdk/` need the infra owner's approval.
7. Merge with squash.

## Commit messages

Conventional Commits, enforced by commitlint:

```
<type>(<scope>): <subject>
```

Types: `feat`, `fix`, `refactor`, `perf`, `chore`, `docs`, `test`, `data`,
`revert`. Subject in imperative mood, no trailing period, 72 characters max.

Scopes used in this repo: `ingestor`, `processor`, `glue`, `infra`, `ci`,
`quality`, `monitoring`, `docs`.

## Code standards

- Python: `ruff` for lint and format, `mypy` for types. No exceptions.
- Public functions need type hints and a docstring explaining the contract,
  especially for anything crossing a service boundary.
- Lambda handlers stay thin: fetch, validate, delegate, return a summary.
- Every new AWS resource must be created in CDK. Clicking through a console
  and then committing the code later is not acceptable.
- Secrets never appear in code, config, logs, or commit messages.

## Definition of done

- [ ] `make check` passes
- [ ] Tests cover the new behaviour, including the failure path
- [ ] ADR added or updated if a design decision changed
- [ ] `docs/runbook.md` updated if operators need to know something new
- [ ] PR description explains what and why, not just how
