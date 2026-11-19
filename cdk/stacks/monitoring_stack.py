"""Monitoring stack: CloudWatch dashboards, alarms, SNS alerting, budget."""
from __future__ import annotations

from typing import Any

from aws_cdk import CfnOutput, Stack
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
        **props: Any,
    ) -> None:
        super().__init__(scope, construct_id, **props)

        self.alert_topic = sns.Topic(
            self,
            "AlertTopic",
            display_name="GHN Pipeline Alerts",
            topic_name=f"{self.stack_name}-alerts",
        )
        self.alert_topic.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["SNS:Publish"],
                principals=[iam.ServicePrincipal("cloudwatch.amazonaws.com")],
                resources=[self.alert_topic.topic_arn],
            )
        )

        alert_email = self.node.try_get_context("alert_email") or ""
        if alert_email:
            self.alert_topic.add_subscription(
                subscriptions.EmailSubscription(alert_email)
            )

        self.dashboard = cloudwatch.Dashboard(
            self,
            "PipelineDashboard",
            dashboard_name="GHN-Pipeline-Ops",
        )
        self._add_dashboard_widgets(base, ingestion, processing)

        self._add_alarms(base, ingestion, processing)

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
                title="Webhook, reconciliation and master data invocations",
                left=[
                    ingestion.webhook.metric_invocations(label="Webhook"),
                    ingestion.reconciler.metric_invocations(label="Reconciler"),
                    ingestion.master_data.metric_invocations(label="Master data"),
                ],
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
                left=[base.orders_table.metric_consumed_write_capacity_units()],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="DLQ depth",
                left=[
                    processing.processing_dlq.metric_approximate_number_of_messages_visible(),
                    ingestion.ingest_dlq.metric_approximate_number_of_messages_visible(),
                    ingestion.webhook_dlq.metric_approximate_number_of_messages_visible(),
                ],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Webhook FIFO backlog",
                left=[ingestion.webhook_queue.metric_approximate_number_of_messages_visible()],
                width=12,
            ),
        )

    def _add_alarms(self, base: BaseStack, ingestion: IngestionStack, processing: ProcessingStack) -> None:
        """Alert on queue backlog, dead letters and processing or API errors."""
        for name, dlq in (
            ("ProcessingDLQNotEmpty", processing.processing_dlq),
            ("IngestDLQNotEmpty", ingestion.ingest_dlq),
            ("WebhookDLQNotEmpty", ingestion.webhook_dlq),
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
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            alarm_description="The Processor is throwing. Check its logs.",
        ).add_alarm_action(cw_actions.SnsAction(self.alert_topic))

        for name, metric, threshold in (
            ("WebhookApi5xx", ingestion.api.metric_server_error(statistic="Sum"), 0),
            (
                "WebhookQueueOldestMessage",
                ingestion.webhook_queue.metric_approximate_age_of_oldest_message(
                    statistic="Maximum"
                ),
                300,
            ),
        ):
            cloudwatch.Alarm(
                self,
                name,
                metric=metric,
                threshold=threshold,
                evaluation_periods=1,
                comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
                treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
            ).add_alarm_action(cw_actions.SnsAction(self.alert_topic))
