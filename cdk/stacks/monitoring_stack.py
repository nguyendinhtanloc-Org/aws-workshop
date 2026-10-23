"""Monitoring stack: CloudWatch dashboards, SNS alerting, budget alarms."""
from __future__ import annotations

from aws_cdk import CfnOutput, Stack, StackProps
from aws_cdk import aws_budgets as budgets
from aws_cdk import aws_cloudwatch as cloudwatch
from aws_cdk import aws_iam as iam
from aws_cdk import aws_sns as sns
from constructs import Construct

from stacks.base_stack import BaseStack


class MonitoringStack(Stack):
    """Surfaces pipeline health: metrics, dashboards, and alert routing."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        base: BaseStack,
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
        self.alert_topic.add_to_resource_policy(
            sns.TopicPolicy(
                statements=[
                    sns.Statement(
                        actions=["SNS:Publish"],
                        principals=[iam.ServicePrincipal("cloudwatch.amazonaws.com")],
                        resources=[self.alert_topic.topic_arn],
                    )
                ]
            )
        )

        # --- Ops dashboard: the view we show during demos -------------------
        self.dashboard = cloudwatch.Dashboard(
            self,
            "PipelineDashboard",
            dashboard_name="GHN-Pipeline-Ops",
        )

        # Cost budget so a runaway Firehose/Athena query cannot burn the
        # credits. The subscriber email is set per-environment in cdk.context.
        subscriber_email = self.node.try_get_context("budget_alert_email") or ""

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
                            address=subscriber_email,
                            subscription_type="EMAIL",
                        )
                    ],
                )
            ]
            if subscriber_email
            else None,
        )

        CfnOutput(self, "AlertTopicArn", value=self.alert_topic.topic_arn)
        CfnOutput(self, "DashboardName", value="GHN-Pipeline-Ops")
