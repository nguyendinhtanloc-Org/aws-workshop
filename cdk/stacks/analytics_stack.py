"""Analytics stack: Glue Data Catalog, ETL jobs, Athena workgroup."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack, StackProps
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3_assets as s3_assets
from constructs import Construct

from stacks.base_stack import BaseStack


class AnalyticsStack(Stack):
    """Curates the curated layer into query-ready tables for Athena."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        **props: StackProps,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- Glue database -------------------------------------------------
        self.database = glue.CfnDatabase(
            self,
            "AnalyticsDatabase",
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="ghn_analytics",
                description="GHN logistics analytics - curated and aggregate layers",
            ),
        )

        # --- Glue job role -------------------------------------------------
        job_role = iam.Role(
            self,
            "GlueJobRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Execution role for GHN ETL jobs",
        )
        job_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateTable",
                    "glue:UpdateTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:CreatePartition",
                    "glue:BatchCreatePartition",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:DeletePartition",
                    "glue:BatchDeletePartition",
                ],
                resources=["*"],
            )
        )
        job_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                resources=[
                    base.curated_bucket.bucket_arn,
                    f"{base.curated_bucket.bucket_arn}/*",
                ],
            )
        )
        job_role.add_to_policy(
            iam.PolicyStatement(
                actions=["kms:Decrypt", "kms:Encrypt", "kms:GenerateDataKey"],
                resources=[base.curated_data_key.key_arn],
            )
        )
        job_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:AssociateKmsKey",
                ],
                resources=["*"],
            )
        )

        # --- Glue ETL script as a versioned S3 asset -----------------------
        # The job code lives in version control and is published to S3 by
        # CDK, so the job always runs the code from the deployed commit.
        self.script_asset = s3_assets.Asset(
            self,
            "EtlScriptAsset",
            path="src/glue/jobs/bronze_to_silver.py",
        )

        # --- Glue ETL job: curated -> silver --------------------------------
        self.etl_job = glue.CfnJob(
            self,
            "BronzeToSilverJob",
            name="ghn-bronze-to-silver",
            role=job_role.role_arn,
            command=glue.CfnJob.CommandProperty(
                name="glueetl",
                python_version="3",
                script_location=self.script_asset.s3_object_url,
            ),
            default_arguments={
                "job-language": "python",
                "enable-metrics": "true",
                "job-bookmark-option": "job-bookmark-disable",
                "curated-bucket": base.curated_bucket.bucket_name,
                "database-name": "ghn_analytics",
                "raw-prefix": "curated/orders/",
                "silver-prefix": "silver/orders/",
            },
            glue_version=glue.CfnJob.GlueVersion("4.0"),
            worker_type=glue.CfnJob.WorkerType("G.1X"),
            number_of_workers=1,
            timeout=Duration.minutes(30),
            max_concurrent_runs=1,
        )

        # --- Athena workgroup ------------------------------------------------
        self.workgroup = athena.CfnWorkGroup(
            self,
            "AnalyticsWorkgroup",
            name="ghn-analytics",
            state="ENABLED",
            recursive_delete_option=False,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloudwatch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=1024 * 1024 * 1024 * 10,  # 10 GB
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{base.athena_results_bucket.bucket_name}/athena-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    ),
                ),
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="AUTO"
                ),
            ),
        )

        CfnOutput(self, "GlueDatabaseName", value="ghn_analytics")
        CfnOutput(self, "AthenaWorkgroup", value="ghn-analytics")
        CfnOutput(self, "EtlJobName", value="ghn-bronze-to-silver")
