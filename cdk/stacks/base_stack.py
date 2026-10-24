"""Base stack: shared VPC, S3 buckets, KMS keys, and log retention.

Every other stack references resources created here so that encryption,
logging, and retention policies are defined exactly once.
"""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack, StackProps
from aws_cdk import aws_dynamodb as ddb
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_kms as kms
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct


class BaseStack(Stack):
    """Shared infrastructure used by every feature stack."""

    def __init__(self, scope: Construct, construct_id: str, **props: StackProps) -> None:
        super().__init__(scope, construct_id, **props)

        # --- VPC ---------------------------------------------------------
        # Nothing in this pipeline runs inside a VPC: the Lambdas call the
        # GHN API and the AWS APIs directly, and Glue runs in a managed
        # VPC. A VPC with a NAT gateway would cost ~$33/month - half the
        # project budget - for nothing.
        #
        # The VPC is still created because the Glue ETL jobs need one, and
        # it uses a single NAT gateway rather than one per AZ.
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
        )

        # --- KMS: one key per data domain so we can revoke access per domain
        self.raw_data_key = kms.Key(
            self,
            "RawDataKey",
            description="Encrypts S3 raw landing zone objects",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )
        self.curated_data_key = kms.Key(
            self,
            "CuratedDataKey",
            description="Encrypts S3 curated / analytics objects",
            enable_key_rotation=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- S3: raw landing (source of truth backup) ---------------------
        self.raw_bucket = s3.Bucket(
            self,
            "RawBucket",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.raw_data_key,
            bucket_name=f"{self.stack_name}-raw",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="raw-expiry",
                    enabled=True,
                    expiration=Duration.days(30),
                    noncurrent_version_expiration=Duration.days(7),
                )
            ],
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- S3: curated / analytics --------------------------------------
        self.curated_bucket = s3.Bucket(
            self,
            "CuratedBucket",
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.curated_data_key,
            bucket_name=f"{self.stack_name}-curated",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            versioned=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="curated-tidy",
                    enabled=True,
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        )
                    ],
                    noncurrent_version_expiration=Duration.days(30),
                )
            ],
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- S3: Athena query results + ad-hoc scratch --------------------
        self.athena_results_bucket = s3.Bucket(
            self,
            "AthenaResultsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="athena-results-expiry",
                    enabled=True,
                    expiration=Duration.days(7),
                )
            ],
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- DynamoDB: order status lookup (partitioned, hot-path) --------
        self.orders_table = ddb.Table(
            self,
            "OrdersTable",
            partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="sk", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            encryption=ddb.TableEncryption.AWS_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- CloudWatch: shared log group with explicit retention ---------
        self.log_retention = logs.RetentionDays.ONE_MONTH

        CfnOutput(self, "RawBucketName", value=self.raw_bucket.bucket_name)
        CfnOutput(self, "CuratedBucketName", value=self.curated_bucket.bucket_name)
        CfnOutput(self, "AthenaResultsBucketName", value=self.athena_results_bucket.bucket_name)
        CfnOutput(self, "OrdersTableName", value=self.orders_table.table_name)
        CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
