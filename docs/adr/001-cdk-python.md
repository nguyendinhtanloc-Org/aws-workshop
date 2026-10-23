# ADR 001: Use AWS CDK with Python for infrastructure

- **Status:** Accepted
- **Date:** 2026-10-23
- **Deciders:** Data Engineering team (5)

## Context

The project needs every AWS resource created from code: a VPC, S3 buckets
with encryption and lifecycle rules, Lambda functions, Kinesis Firehose,
DynamoDB, SQS, Glue, Athena, CloudWatch dashboards and SNS topics. The
original proposal mentioned both CDK and Terraform, which is contradictory —
we have to pick one.

The team writes Python daily (Lambda handlers, Glue jobs, tests). Four of the
five members have AWS experience but not all have used Terraform's HCL.

## Decision

Use **AWS CDK in Python** (`aws-cdk-lib` 2.x).

## Rationale

**Same language as the application code.** The team already maintains a
Python toolchain: `ruff`, `mypy`, `pytest`, one `pyproject.toml`. Adding CDK
means one more Python package, not a second language, a second dependency
manager and a second set of editor configs.

**Stronger constructs for the services we use.** CDK has L2 constructs for
Lambda, DynamoDB, S3, Step Functions and Kinesis Firehose that fill in the
IAM policies, encryption defaults and log groups. In Terraform the
equivalent wiring is explicit and is where most of the "works on my machine"
bugs come from.

**Unit testing the infrastructure.** `aws-cdk-lib/assertions` and `cdk-nag`
let us assert on the synthesized template in the same test suite as the
application code:

```python
template = Template.from_stack(stack)
template.has_resource_properties("AWS::DynamoDB::Table", {"BillingMode": "PAY_PER_REQUEST"})
```

In Terraform the equivalent is `terratest`, which means learning Go or a
second test idiom.

**Managed state.** CloudFormation handles the stack state, drift detection
and rollback. Terraform needs its own remote state backend with locking,
which is one more thing to get wrong before the pipeline even runs.

**Trade-off accepted:** CDK is AWS-only. If the project ever needs to manage
non-AWS infrastructure, Terraform would win. Nothing in this proposal needs
that, and a rewrite later is possible because everything is code.

## Consequences

- The CDK app lives in `cdk/` and is deployed with `cdk deploy --all`.
- CI runs `cdk synth` on every PR to catch template errors early.
- The team must learn CDK concepts: constructs, stacks, dependencies,
  context and escape hatches. The escape hatch (L1 `CfnResource`) should be a
  last resort, and used with a comment explaining why.
