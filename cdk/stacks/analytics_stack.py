"""Analytics stack: Glue Data Catalog, ETL jobs, Athena workgroup."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Fn, Stack, StackProps
from aws_cdk import aws_athena as athena
from aws_cdk import aws_events as events
from aws_cdk import aws_events_targets as targets
from aws_cdk import aws_glue as glue
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3_assets as s3_assets
from aws_cdk import aws_stepfunctions as sfn
from aws_cdk import aws_stepfunctions_tasks as tasks
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
        # catalog_id is the AWS account id; the pseudo-parameter keeps this
        # region/account agnostic at synth time.
        self.database = glue.CfnDatabase(
            self,
            "AnalyticsDatabase",
            catalog_id=Fn.ref("AWS::AccountId"),
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

        # --- Glue ETL jobs run on Glue's public infrastructure -----------
        # The ETL jobs only read from and write to S3 (S3 -> S3 curation)
        # - they never call the GHN API - so they do not need a VPC or NAT.
        # A VPC can be re-added later only if a job ever needs private
        # egress to a VPC-only resource.
        #
        # (The Glue L1 in this CDK version does not expose a
        # security-configuration VPC-interface property, so pinning a job
        # to specific subnets/SGs is not expressible through the
        # CfnJob/ CfnSecurityConfiguration L1s anyway.)

        # --- Glue ETL scripts as versioned S3 assets ----------------------
        # The job code lives in version control and is published to S3 by
        # CDK, so each job always runs the code from the deployed commit.
        self.silver_script_asset = s3_assets.Asset(
            self,
            "SilverScriptAsset",
            path="src/glue/jobs/bronze_to_silver.py",
        )
        self.gold_script_asset = s3_assets.Asset(
            self,
            "GoldScriptAsset",
            path="src/glue/jobs/silver_to_gold.py",
        )

        # --- Glue ETL job: curated -> silver --------------------------------
        # Property names follow the exact Glue L1 in this CDK version:
        # the nested command type is CfnJob.JobCommandProperty, and
        # glue_version / worker_type are plain strings ("4.0", "G.1X"),
        # not typed enums. max_concurrent_runs lives inside
        # execution_property, not at the job level.
        self.etl_job = glue.CfnJob(
            self,
            "BronzeToSilverJob",
            name="ghn-bronze-to-silver",
            role=job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=self.silver_script_asset.s3_object_url,
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
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=1,
            # CfnJob L1 takes timeout as a plain number of minutes, not a
            # Duration.
            timeout=30,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
        )

        # --- Glue ETL job: silver -> gold ---------------------------------
        self.gold_job = glue.CfnJob(
            self,
            "SilverToGoldJob",
            name="ghn-silver-to-gold",
            role=job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=self.gold_script_asset.s3_object_url,
            ),
            default_arguments={
                "job-language": "python",
                "enable-metrics": "true",
                "job-bookmark-option": "job-bookmark-disable",
                "curated-bucket": base.curated_bucket.bucket_name,
                "database-name": "ghn_analytics",
                "silver-prefix": "silver/orders/",
                "gold-prefix": "gold/",
            },
            glue_version="4.0",
            worker_type="G.1X",
            number_of_workers=1,
            # CfnJob L1 takes timeout as a plain number of minutes, not a
            # Duration.
            timeout=30,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
        )

        # --- Orchestration: run the ETL on a schedule ----------------------
        # EventBridge cannot invoke a Glue job directly. A Step Functions
        # state machine calls the job and reports the outcome; without
        # this the job exists but never runs and the silver layer stays
        # empty.
        silver_task = tasks.GlueStartJobRun(
            self,
            "StartSilverJob",
            glue_job_name=self.etl_job.name,
        )
        gold_task = tasks.GlueStartJobRun(
            self,
            "StartGoldJob",
            glue_job_name=self.gold_job.name,
        )
        # Silver must finish (its Parquet is complete) before gold reads it,
        # so the two tasks run in sequence, not in parallel.
        etl_chain = sfn.Chain.start(silver_task).next(gold_task)

        state_machine = sfn.StateMachine(
            self,
            "EtlStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(etl_chain),
            timeout=Duration.minutes(45),
        )

        self.etl_schedule = events.Rule(
            self,
            "EtlSchedule",
            schedule=events.Schedule.rate(Duration.hours(1)),
            description="Curate the curated layer into silver tables",
        )
        self.etl_schedule.add_target(targets.SfnStateMachine(state_machine))

        # --- Athena workgroup ------------------------------------------------
        self.workgroup = athena.CfnWorkGroup(
            self,
            "AnalyticsWorkgroup",
            name="ghn-analytics",
            state="ENABLED",
            recursive_delete_option=False,
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                # Binding spells it with an underscore: publish_cloud_watch_*.
                publish_cloud_watch_metrics_enabled=True,
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
        CfnOutput(self, "GoldJobName", value="ghn-silver-to-gold")
