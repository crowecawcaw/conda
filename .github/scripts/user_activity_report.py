#!/usr/bin/env python3
"""
User Activity Report Generator

Connects to DSQL database to gather user activity data and
CloudWatch to gather application metrics.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
except ImportError as e:
    print(f"Error: Missing required dependency: {e}")
    print("Please install: pip install boto3 psycopg2-binary")
    sys.exit(1)


class UserActivityReporter:
    """Generate user activity reports from DSQL database and CloudWatch."""

    def __init__(self):
        """Initialize database and AWS connections."""
        # Database configuration
        self.db_config = {
            'host': os.environ.get('DSQL_HOST'),
            'port': os.environ.get('DSQL_PORT', '5432'),
            'database': os.environ.get('DSQL_DATABASE'),
            'user': os.environ.get('DSQL_USER'),
            'password': os.environ.get('DSQL_PASSWORD'),
        }

        # AWS configuration
        self.aws_region = os.environ.get('AWS_REGION', 'us-east-1')
        self.cloudwatch_namespace = os.environ.get('CLOUDWATCH_NAMESPACE', 'CondaService')

        # Validate configuration
        self._validate_config()

        # Initialize connections
        self.db_conn = None
        self.cloudwatch = None

    def _validate_config(self):
        """Validate required environment variables are set."""
        required_vars = ['DSQL_HOST', 'DSQL_DATABASE', 'DSQL_USER', 'DSQL_PASSWORD']
        missing = [var for var in required_vars if not os.environ.get(var)]

        if missing:
            print(f"Error: Missing required environment variables: {', '.join(missing)}")
            print("\nRequired environment variables:")
            print("  - DSQL_HOST: Database host address")
            print("  - DSQL_DATABASE: Database name")
            print("  - DSQL_USER: Database username")
            print("  - DSQL_PASSWORD: Database password")
            print("  - DSQL_PORT: Database port (optional, defaults to 5432)")
            print("\nFor CloudWatch metrics:")
            print("  - AWS_ACCESS_KEY_ID: AWS access key")
            print("  - AWS_SECRET_ACCESS_KEY: AWS secret key")
            print("  - AWS_REGION: AWS region (optional, defaults to us-east-1)")
            sys.exit(1)

    def connect_database(self):
        """Establish connection to DSQL database."""
        try:
            print("Connecting to DSQL database...")
            self.db_conn = psycopg2.connect(**self.db_config)
            print("✓ Database connection established")
        except psycopg2.Error as e:
            print(f"Error connecting to database: {e}")
            sys.exit(1)

    def connect_cloudwatch(self):
        """Establish connection to AWS CloudWatch."""
        try:
            print("Connecting to AWS CloudWatch...")
            self.cloudwatch = boto3.client('cloudwatch', region_name=self.aws_region)
            print("✓ CloudWatch connection established")
        except (ClientError, BotoCoreError) as e:
            print(f"Warning: Could not connect to CloudWatch: {e}")
            print("Continuing without CloudWatch metrics...")

    def get_user_activity(self) -> List[Dict[str, Any]]:
        """
        Query database for user activity data.

        Returns list of users with their follow counts and email counts.

        NOTE: Adjust the SQL queries below to match your actual database schema.
        This is a template that assumes the following tables exist:
        - users: (id, username, email, created_at)
        - follows: (user_id, followed_entity_id, created_at)
        - emails_sent: (user_id, sent_at, email_type)
        """
        cursor = self.db_conn.cursor(cursor_factory=RealDictCursor)

        # Calculate the date for "past week"
        one_week_ago = datetime.now() - timedelta(days=7)

        # TODO: Adjust this query to match your actual database schema
        query = """
        SELECT
            u.id,
            u.username,
            u.email,
            COUNT(DISTINCT f.followed_entity_id) AS follow_count,
            COUNT(DISTINCT e.id) FILTER (WHERE e.sent_at >= %s) AS emails_sent_week
        FROM users u
        LEFT JOIN follows f ON u.id = f.user_id
        LEFT JOIN emails_sent e ON u.id = e.user_id
        GROUP BY u.id, u.username, u.email
        ORDER BY u.username;
        """

        try:
            print("\nQuerying user activity data...")
            cursor.execute(query, (one_week_ago,))
            results = cursor.fetchall()
            print(f"✓ Retrieved data for {len(results)} users")
            return results
        except psycopg2.Error as e:
            print(f"Error querying database: {e}")
            print("\nNOTE: You may need to adjust the SQL query in this script")
            print("to match your actual database schema.")
            return []
        finally:
            cursor.close()

    def get_cloudwatch_metrics(self) -> Dict[str, Any]:
        """
        Query CloudWatch for application metrics.

        Returns dictionary with input and output metrics.

        NOTE: Adjust metric names and namespaces to match your actual CloudWatch setup.
        """
        if not self.cloudwatch:
            return {}

        # Time range: last 7 days
        end_time = datetime.now()
        start_time = end_time - timedelta(days=7)

        metrics = {}

        # TODO: Adjust these metric names to match your actual CloudWatch metrics
        metric_configs = [
            {
                'name': 'InputRequests',
                'stat': 'Sum',
                'label': 'Total Input Requests',
            },
            {
                'name': 'OutputResponses',
                'stat': 'Sum',
                'label': 'Total Output Responses',
            },
            {
                'name': 'InputBytes',
                'stat': 'Sum',
                'label': 'Total Input Bytes',
            },
            {
                'name': 'OutputBytes',
                'stat': 'Sum',
                'label': 'Total Output Bytes',
            },
        ]

        print("\nQuerying CloudWatch metrics...")

        for config in metric_configs:
            try:
                response = self.cloudwatch.get_metric_statistics(
                    Namespace=self.cloudwatch_namespace,
                    MetricName=config['name'],
                    StartTime=start_time,
                    EndTime=end_time,
                    Period=604800,  # 7 days in seconds
                    Statistics=[config['stat']],
                )

                if response['Datapoints']:
                    value = response['Datapoints'][0][config['stat']]
                    metrics[config['label']] = value
                    print(f"✓ Retrieved {config['label']}: {value:,.0f}")
                else:
                    metrics[config['label']] = 0
                    print(f"⚠ No data for {config['label']}")

            except (ClientError, BotoCoreError) as e:
                print(f"Warning: Could not retrieve {config['name']}: {e}")
                metrics[config['label']] = "N/A"

        return metrics

    def generate_report(self, users: List[Dict[str, Any]], metrics: Dict[str, Any]):
        """Generate and print formatted report."""
        report_lines = []

        # Header
        report_lines.append("=" * 80)
        report_lines.append("USER ACTIVITY REPORT")
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        report_lines.append("=" * 80)
        report_lines.append("")

        # User Activity Section
        report_lines.append("USER ACTIVITY (Past 7 Days)")
        report_lines.append("-" * 80)
        report_lines.append("")

        if users:
            # Table header
            report_lines.append(f"{'Username':<30} {'Follows':<15} {'Emails Sent':<15}")
            report_lines.append("-" * 80)

            # User rows
            total_follows = 0
            total_emails = 0

            for user in users:
                username = user.get('username', 'N/A')[:29]
                follows = user.get('follow_count', 0)
                emails = user.get('emails_sent_week', 0)

                report_lines.append(f"{username:<30} {follows:<15} {emails:<15}")

                total_follows += follows
                total_emails += emails

            # Summary
            report_lines.append("-" * 80)
            report_lines.append(f"{'TOTAL':<30} {total_follows:<15} {total_emails:<15}")
            report_lines.append("")
            report_lines.append(f"Total Users: {len(users)}")
        else:
            report_lines.append("No user data available.")

        report_lines.append("")
        report_lines.append("")

        # CloudWatch Metrics Section
        report_lines.append("CLOUDWATCH METRICS (Past 7 Days)")
        report_lines.append("-" * 80)
        report_lines.append("")

        if metrics:
            max_label_length = max(len(label) for label in metrics.keys())

            for label, value in metrics.items():
                if isinstance(value, (int, float)):
                    report_lines.append(f"{label:<{max_label_length}}: {value:>20,.0f}")
                else:
                    report_lines.append(f"{label:<{max_label_length}}: {value:>20}")
        else:
            report_lines.append("No CloudWatch metrics available.")

        report_lines.append("")
        report_lines.append("=" * 80)

        # Print report to console
        report_text = "\n".join(report_lines)
        print("\n" + report_text)

        # Save report to file
        with open('user-activity-report.txt', 'w') as f:
            f.write(report_text)

        print("\n✓ Report saved to: user-activity-report.txt")

    def run(self):
        """Execute the report generation process."""
        try:
            # Connect to services
            self.connect_database()
            self.connect_cloudwatch()

            # Gather data
            users = self.get_user_activity()
            metrics = self.get_cloudwatch_metrics()

            # Generate and print report
            self.generate_report(users, metrics)

        finally:
            # Clean up connections
            if self.db_conn:
                self.db_conn.close()
                print("\n✓ Database connection closed")


def main():
    """Main entry point."""
    reporter = UserActivityReporter()
    reporter.run()


if __name__ == '__main__':
    main()
