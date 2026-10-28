"""Monitoring stack: CloudWatch dashboards, alarms, SNS alerting, budget."""
from __future__ import annotations

from aws_cdk import CfnOutput, Duration, Stack, StackProps
from aws_cdk import aws_budgets as budgets
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_cloudwatch_actions as cw_actions
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as subscriptions
from constructs import Construct

from stacks.base_stack import BaseStack
from stacks.ingestion_stack import IngestionStack
from stacks.processing_stack import ProcessingStack


class MonitoringStack(Stack):
    """Surfaces pipeline health: metrics, dashboards, and alert routing."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
        ingestion: IngestionStack,
        processing: ProcessingStack,
        **props: StackProps,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        # --- Alert topic ---------------------------------------------------
        self.alert_topic = sns.Topic(
            self,
            "AlertTopic",
            display_name="GHN Pipeline Alerts",
            topic_name=f"{self.stack_name}-alerts",
        )
        # Allow CloudWatch (the alarm source) to publish to the topic.
        # add_to_resource_policy takes a single iam.PolicyStatement, not a
        # sns.TopicPolicy / sns.Statement (those do not exist in this
        # binding).
        self.alert_topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["SNS:Publish"],
                principals=[iam.ServicePrincipal("cloudwatch.amazonaws.com")],
                resources=[self.alert_topic.topic_arn],
            )
        )

        # --- Where the alerts go --------------------------------------------
        # Without a subscriber the topic is a black hole: CloudWatch raises
        # the alarm, SNS accepts the message, and nobody is told. The email
        # comes from context so each environment can route to a different
        # address without a code change.
        alert_email = self.node.try_get_context("alert_email") or ""
        if alert_email:
            self.alert_topic.add_subscription(
                subscriptions.EmailSubscription(alert_email)
            )

        # --- Ops dashboard ---------------------------------------------------
        # The dashboard is what gets shown during a demo, so it needs
        # widgets. An empty dashboard is worse than none.
        self.dashboard = cloudwatch.Dashboard(
            self,
            "PipelineDashboard",
            dashboard_name="GHN-Pipeline-Ops",
        )
        self._add_dashboard_widgets(base, ingestion, processing)

        # --- Alarms ----------------------------------------------------------
        self._add_alarms(base, ingestion, processing)

        # --- Cost budget ------------------------------------------------------
        # So a runaway Firehose or Athena query cannot burn the credits.
        budget_email = self.node.try_get_context("budget_alert_email") or alert_email

        self.budget = budgets.CfnBudget(
            self,
            "MonthlyBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name=f"{self.stack_name}-monthly",
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(amount=65, unit="USD"),
            ),
            notifications_with_subscribers=[
                budgets.CfnBudget.NotificationWithSubscribersProperty(
                    notification=budgets.CfnBudget.NotificationProperty(
                        notification_type="ACTUAL",
                        comparison_operator="GREATER_THAN",
                        threshold=80,
                    ),
                    subscribers=[
                        budgets.CfnBudget.SubscriberProperty(
                            address=budget_email,
                            subscription_type="EMAIL",
                        )
                    ],
                )
            ]
            if budget_email
            else None,
        )

        CfnOutput(self, "AlertTopicArn", value=self.alert_topic.topic_arn)
        CfnOutput(self, "DashboardName", value="GHN-Pipeline-Ops")

    def _add_dashboard_widgets(self, base: BaseStack, ingestion: IngestionStack, processing: ProcessingStack) -> None:
        """Add the widgets the ops team looks at first."""
        self.dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Ingestor invocations",
                # This Lambda L2 exposes metric_invocations (not a
                # metric_invocation_count alias).
                left=[ingestion.ingestor.metric_invocations()],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Processor invocations",
                left=[processing.processor.metric_invocations()],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Processor errors",
                left=[processing.processor.metric_errors()],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Orders written to DynamoDB",
                # No single "read + write" convenience metric in this
                # CDK version's DynamoDB L2; show consumed write capacity,
                # which tracks order writes.
                left=[base.orders_table.metric_consumed_write_capacity_units()],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="DLQ depth",
                left=[
                    processing.processing_dlq.metric_approximate_number_of_messages_visible(),
                    ingestion.ingest_dlq.metric_approximate_number_of_messages_visible(),
                ],
                width=12,
            ),
        )

    def _add_alarms(self, base: BaseStack, ingestion: IngestionStack, processing: ProcessingStack) -> None:
        """Alarms for the two failure modes that matter.

        A non-zero DLQ depth and a run of processor errors. Both are
        conditions the pipeline can detect about itself, and both mean
        somebody has to look.
        """
        for name, dlq in (
            ("ProcessingDLQNotEmpty", processing.processing_dlq),
            ("IngestDLQNotEmpty", ingestion.ingest_dlq),
        ):
            cloudwatch.Alarm(
                self,
                name,
                metric=dlq.metric_approximate_number_of_messages_visible(),
                threshold=0,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
                alarm_description=(
                    "Records are failing and waiting in the DLQ. "
                    "See docs/runbook.md -> 'Replaying failed records'."
                ),
            ).add_alarm_action(cw_actions.SnsAction(self.alert_topic))

        cloudwatch.Alarm(
            self,
            "ProcessorErrors",
            metric=processing.processor.metric_errors(),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            alarm_description="The Processor is throwing. Check its logs.",
        ).add_alarm_action(cw_actions.SnsAction(self.alert_topic))
