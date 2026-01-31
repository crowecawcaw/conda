# User Activity Report

This GitHub Action generates weekly user activity reports by querying a DSQL database and CloudWatch metrics.

## Features

- **User Activity Data**: Lists all users with their follow counts and emails sent in the past week
- **CloudWatch Metrics**: Gathers input/output metrics from AWS CloudWatch
- **Automated Scheduling**: Runs every Tuesday at 2 PM UTC
- **Manual Trigger**: Can be triggered manually via workflow_dispatch
- **Report Artifact**: Saves report as a GitHub Actions artifact for 30 days

## Setup

### 1. Configure AWS OIDC Authentication

This workflow uses AWS OIDC (OpenID Connect) to authenticate with AWS, eliminating the need for long-lived AWS credentials.

**Step 1: Set up AWS IAM OIDC Provider (if not already configured)**

In your AWS account, create an OIDC identity provider for GitHub Actions:
- Provider URL: `https://token.actions.githubusercontent.com`
- Audience: `sts.amazonaws.com`

**Step 2: Create IAM Role for GitHub Actions**

Create an IAM role with the following trust policy (replace `YOUR_ORG` and `YOUR_REPO`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_ORG/YOUR_REPO:*"
        }
      }
    }
  ]
}
```

**Step 3: Attach IAM Policies**

Attach the following permissions to the IAM role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter",
        "ssm:GetParameters"
      ],
      "Resource": "arn:aws:ssm:REGION:ACCOUNT_ID:parameter/conda/database/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:GetMetricStatistics"
      ],
      "Resource": "*"
    }
  ]
}
```

### 2. Configure SSM Parameters

Store database credentials in AWS Systems Manager Parameter Store as SecureString parameters:

```bash
aws ssm put-parameter \
  --name "/conda/database/host" \
  --value "your-database-host.amazonaws.com" \
  --type "String"

aws ssm put-parameter \
  --name "/conda/database/port" \
  --value "5432" \
  --type "String"

aws ssm put-parameter \
  --name "/conda/database/name" \
  --value "your-database-name" \
  --type "String"

aws ssm put-parameter \
  --name "/conda/database/user" \
  --value "your-database-user" \
  --type "String"

aws ssm put-parameter \
  --name "/conda/database/password" \
  --value "your-database-password" \
  --type "SecureString"
```

### 3. Configure GitHub Secrets

Add the following secrets to your GitHub repository (Settings → Secrets and variables → Actions):

**Required:**
- `AWS_ROLE_ARN`: ARN of the IAM role created in step 1 (e.g., `arn:aws:iam::123456789012:role/GitHubActionsRole`)

**Optional:**
- `AWS_REGION`: AWS region (defaults to us-east-1)
- `SSM_PARAMETER_PREFIX`: SSM parameter prefix (defaults to `/conda/database`)
- `CLOUDWATCH_NAMESPACE`: CloudWatch namespace (defaults to `CondaService`)

### 4. Customize Database Schema

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

**If your schema differs**, edit the SQL query in `user_activity_report.py` in the `get_user_activity()` method.

### 5. Customize CloudWatch Metrics

The script queries for these CloudWatch metrics:
- `InputRequests` - Total input requests
- `OutputResponses` - Total output responses
- `InputBytes` - Total input bytes
- `OutputBytes` - Total output bytes

**If your metrics differ**, edit the `metric_configs` in `user_activity_report.py` in the `get_cloudwatch_metrics()` method.

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

### AWS OIDC Authentication Issues

- Verify the `AWS_ROLE_ARN` secret is correctly set in GitHub
- Ensure the IAM role trust policy allows your GitHub repository
- Check that the OIDC provider is properly configured in AWS IAM
- Verify the IAM role has the necessary permissions (SSM and CloudWatch)

### SSM Parameter Issues

- Ensure all required parameters exist in SSM Parameter Store
- Verify the parameter names match the prefix configured (default: `/conda/database`)
- Check that the IAM role has `ssm:GetParameter` permission for the parameter path
- Use `aws ssm get-parameter --name /conda/database/host` to verify parameters exist

### Database Connection Issues

- Verify all database credentials are correctly stored in SSM Parameter Store
- Ensure the DSQL database allows connections from GitHub Actions IP ranges
- Check database firewall rules and security groups
- Test database connectivity from an EC2 instance in the same VPC

### CloudWatch Connection Issues

- Verify the IAM role has CloudWatch read permissions
- Ensure the IAM policy includes `cloudwatch:GetMetricStatistics`
- Check that the CloudWatch namespace and metric names are correct
- Verify metrics exist in CloudWatch for the specified time range

### Schema Mismatch

If you see SQL errors, your database schema likely differs from the assumed structure. Edit the SQL query in `user_activity_report.py` to match your actual schema.

## Development

### Local Testing

For local testing, you can either:

**Option 1: Use AWS SSM (recommended)**
```bash
# Configure AWS credentials (use AWS CLI or environment variables)
export AWS_REGION=us-east-1
export SSM_PARAMETER_PREFIX=/conda/database

# Install dependencies
pip install boto3 psycopg2-binary

# Run the script
python .github/scripts/user_activity_report.py
```

**Option 2: Mock SSM with direct environment variables**

You'll need to modify the script temporarily to read from environment variables instead of SSM, or create a separate test configuration.

## License

Same as the parent repository.
