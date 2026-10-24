"""Reusable CDK constructs.

Named `cdk_constructs` on purpose: a local package called `constructs`
would shadow the `constructs` library that aws-cdk-lib depends on,
because the directory of the CDK app ends up first on sys.path.
"""
from cdk_constructs.lambda_function import PipelineLambdaFunction

__all__ = ["PipelineLambdaFunction"]
