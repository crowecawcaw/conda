# User Activity Report

This GitHub Action generates weekly user activity reports by querying a DSQL database and CloudWatch metrics.

## Features

- **User Activity Data**: Lists all users with their follow counts and emails sent in the past week
- **CloudWatch Metrics**: Gathers input/output metrics from AWS CloudWatch
- **Automated Scheduling**: Runs every Tuesday at 2 PM UTC
- **Manual Trigger**: Can be triggered manually via workflow_dispatch
- **Report Artifact**: Saves report as a GitHub Actions artifact for 30 days

## Setup

### 1. Configure GitHub Secrets

Add the following secrets to your GitHub repository (Settings → Secrets and variables → Actions):

**Database Connection:**
- `DSQL_HOST`: Your DSQL database host address
- `DSQL_PORT`: Database port (optional, defaults to 5432)
- `DSQL_DATABASE`: Database name
- `DSQL_USER`: Database username
- `DSQL_PASSWORD`: Database password

**AWS CloudWatch:**
- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key
- `AWS_REGION`: AWS region (optional, defaults to us-east-1)

**CloudWatch Namespace (optional):**
- `CLOUDWATCH_NAMESPACE`: CloudWatch namespace (defaults to 'CondaService')

### 2. Customize Database Schema

The script assumes the following database tables exist:

```sql
-- Users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP
);

-- Follows table
CREATE TABLE follows (
    user_id INTEGER REFERENCES users(id),
    followed_entity_id INTEGER,
    created_at TIMESTAMP
);

-- Emails sent table
CREATE TABLE emails_sent (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    sent_at TIMESTAMP,
    email_type VARCHAR(100)
);
```

**If your schema differs**, edit the SQL query in `user_activity_report.py` at line ~85.

### 3. Customize CloudWatch Metrics

The script queries for these CloudWatch metrics:
- `InputRequests` - Total input requests
- `OutputResponses` - Total output responses
- `InputBytes` - Total input bytes
- `OutputBytes` - Total output bytes

**If your metrics differ**, edit the `metric_configs` in `user_activity_report.py` at line ~133.

## Usage

### Automatic Execution

The workflow runs automatically every Tuesday at 2 PM UTC. Check the Actions tab to view results.

### Manual Execution

1. Go to the **Actions** tab in your GitHub repository
2. Select **User Activity Report** workflow
3. Click **Run workflow**
4. Select the branch and click **Run workflow**

### Viewing Reports

After the workflow completes:

1. Go to the workflow run page
2. Scroll to **Artifacts** section
3. Download `user-activity-report-{run-id}`
4. Open `user-activity-report.txt` to view the report

## Report Format

```
================================================================================
USER ACTIVITY REPORT
Generated: 2026-01-31 14:00:00 UTC
================================================================================

USER ACTIVITY (Past 7 Days)
--------------------------------------------------------------------------------

Username                       Follows         Emails Sent
--------------------------------------------------------------------------------
alice_smith                    42              15
bob_jones                      18              8
charlie_brown                  63              22
--------------------------------------------------------------------------------
TOTAL                          123             45

Total Users: 3


CLOUDWATCH METRICS (Past 7 Days)
--------------------------------------------------------------------------------

Total Input Requests  :              12,345
Total Output Responses:              12,340
Total Input Bytes     :          5,678,901
Total Output Bytes    :         23,456,789

================================================================================
```

## Troubleshooting

### Database Connection Issues

- Verify all database credentials are correctly set in GitHub Secrets
- Ensure the DSQL database allows connections from GitHub Actions IP ranges
- Check database firewall rules and security groups

### CloudWatch Connection Issues

- Verify AWS credentials have CloudWatch read permissions
- Ensure the IAM policy includes `cloudwatch:GetMetricStatistics`
- Check that the CloudWatch namespace and metric names are correct

### Schema Mismatch

If you see SQL errors, your database schema likely differs from the assumed structure. Edit the SQL query in `user_activity_report.py` to match your actual schema.

## Development

### Local Testing

```bash
# Set environment variables
export DSQL_HOST=your-db-host
export DSQL_DATABASE=your-db
export DSQL_USER=your-user
export DSQL_PASSWORD=your-password
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret

# Install dependencies
pip install boto3 psycopg2-binary

# Run the script
python .github/scripts/user_activity_report.py
```

## License

Same as the parent repository.
