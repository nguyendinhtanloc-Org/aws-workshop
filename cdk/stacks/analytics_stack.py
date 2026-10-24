"""Analytics stack: Glue Data Catalog, ETL jobs, Athena workgroup."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack, StackProps
from aws_cdk import aws_athena as athena
from aws_cdk import aws_ec2 as ec2
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

        # --- Glue runs in the VPC -----------------------------------------
        # The job needs ENI permissions to create network interfaces in the
        # private subnets, and a security group that allows egress.
        job_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DeleteNetworkInterface",
                    "ec2:AssignPrivateIpAddresses",
                    "ec2:UnassignPrivateIpAddresses",
                ],
                resources="*",
            )
        )

        glue_security_group = ec2.SecurityGroup(
            self,
            "GlueSecurityGroup",
            vpc=base.vpc,
            description="Glue ETL jobs: outbound access to the GHN API and S3",
            allow_all_outbound=True,
        )

        glue_subnets = [
            subnet
            for subnet in base.vpc.private_subnets
        ] or base.vpc.select_subnets(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
        )

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
        self.etl_job = glue.CfnJob(
            self,
            "BronzeToSilverJob",
            name="ghn-bronze-to-silver",
            role=job_role.role_arn,
            command=glue.CfnJob.CommandProperty(
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
            glue_version=glue.CfnJob.GlueVersion("4.0"),
            worker_type=glue.CfnJob.WorkerType("G.1X"),
            number_of_workers=1,
            timeout=Duration.minutes(30),
            max_concurrent_runs=1,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                # The job runs in the VPC so it can reach the GHN API and
                # S3 through the NAT gateway.
                vpc=glue.CfnJob.VPCProperty(
                    security_group_ids=[glue_security_group.security_group_id],
                    subnet_ids=[subnet.subnet_id for subnet in glue_subnets],
                )
            ),
        )

        # --- Glue ETL job: silver -> gold ---------------------------------
        self.gold_job = glue.CfnJob(
            self,
            "SilverToGoldJob",
            name="ghn-silver-to-gold",
            role=job_role.role_arn,
            command=glue.CfnJob.CommandProperty(
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
            glue_version=glue.CfnJob.GlueVersion("4.0"),
            worker_type=glue.CfnJob.WorkerType("G.1X"),
            number_of_workers=1,
            timeout=Duration.minutes(30),
            max_concurrent_runs=1,
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
        CfnOutput(self, "GoldJobName", value="ghn-silver-to-gold")
