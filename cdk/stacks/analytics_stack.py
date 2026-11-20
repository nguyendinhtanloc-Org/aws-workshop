"""Analytics stack: Glue Data Catalog + Athena workgroup.

Silver/gold curation runs as Trino/Athena SQL (sql/02-04) orchestrated by
Step Functions + EventBridge (M3 D12/D13 build); the old Glue PySpark ETL
jobs were legacy from the Firehose-era architecture and are removed here.
"""
from __future__ import annotations

from typing import Any

from aws_cdk import CfnOutput, Fn, Stack
from aws_cdk import aws_athena as athena
from aws_cdk import aws_glue as glue
from constructs import Construct

from stacks.base_stack import BaseStack


class AnalyticsStack(Stack):
    """Hosts the ghn_analytics Glue catalog and the Athena workgroup."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        **props: Any,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- Glue catalog database ----------------------------------------
        # catalog_id is the AWS account id; the pseudo-parameter keeps this
        # region/account agnostic at synth time. Athena serverless reads the
        # Iceberg tables through this catalog.
        self.database = glue.CfnDatabase(
            self,
            "AnalyticsDatabase",
            catalog_id=Fn.ref("AWS::AccountId"),
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="ghn_analytics",
                description="GHN logistics analytics - silver and gold Iceberg tables",
            ),
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
