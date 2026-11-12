"""Base stack: shared data resources (S3, KMS, DynamoDB), optionally a VPC.

Every other stack references resources created here so that encryption,
logging, and retention policies are defined exactly once. The VPC exists
only for the Glue analytics stack - everything else in this pipeline
runs on public endpoints - so minimal deploys set with_vpc=False and
save the NAT gateway's ~$33/month.
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

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        with_vpc: bool = False,
        **props: StackProps,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- VPC: only Glue needs one (NAT gateway ~$33/month) -----------
        self.vpc: ec2.Vpc | None = None
        if with_vpc:
            self.vpc = self._create_vpc()
            CfnOutput(self, "VpcId", value=self.vpc.vpc_id)

        # --- KMS: one key per data domain so we can revoke per domain ----
        # When an S3 L2 bucket is passed a KMS key as its encryption key,
        # CDK automatically adds the S3 service grant to the key's own
        # key policy, so no separate grant helper is needed here.
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
            time_to_live_attribute="ttl",
            encryption=ddb.TableEncryption.AWS_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- DynamoDB: order watchlist (which order codes to poll) --------
        # The GHN API has no list-orders route, so the set of orders to
        # poll is state, not config. Rows are added by scripts/create_
        # demo_order.py (create route) and by a future webhook receiver;
        # the Ingestor records each polled order's status so rows that
        # reach a terminal status stop costing API calls.
        self.watchlist_table = ddb.Table(
            self,
            "OrderWatchlistTable",
            partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="sk", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            time_to_live_attribute="ttl",
            encryption=ddb.TableEncryption.AWS_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # --- CloudWatch: shared log group with explicit retention ---------
        self.log_retention = logs.RetentionDays.ONE_MONTH

        CfnOutput(self, "RawBucketName", value=self.raw_bucket.bucket_name)
        CfnOutput(self, "CuratedBucketName", value=self.curated_bucket.bucket_name)
        CfnOutput(self, "AthenaResultsBucketName", value=self.athena_results_bucket.bucket_name)
        CfnOutput(self, "OrdersTableName", value=self.orders_table.table_name)
        CfnOutput(self, "OrderWatchlistTableName", value=self.watchlist_table.table_name)

    def _create_vpc(self) -> ec2.Vpc:
        """One NAT gateway, public + private-with-egress in two AZs."""
        return ec2.Vpc(
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
