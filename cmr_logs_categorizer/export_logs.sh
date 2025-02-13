#!/bin/bash
set -e

# Function to display usage
show_usage() {
    echo "This script exports CMR logs for the specified time range to a S3 bucket."
    echo
    echo "Ensure the dates are formatted according to the examples below."
    echo "The dates are inclusive: StartDate begins at midnight, and the"
    echo "EndDate ends at 11:59 PM."
    echo
    echo "Usage:   $0 StartDate EndDate"
    echo "Example: $0 \"Oct 08 2024\" \"Oct 15 2024\""
    echo
    echo "Optionally provide the Cloudwatch log group name (default: cmr-search-prod)"
    echo "AWS profile name (default: cmr-prod), AWS region (defualt us-east-1),"
    echo "and destination bucket (default: bigstac-cmr-prod-logs):"
    echo "Usage:   $0 StartDate EndDate LOG_GROUP_NAME AWS_PROFILE AWS_REGION S3_BUCKET"
}

# Check if no arguments were provided
if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    show_usage
    exit 1
fi

# Check if the required arguments are provided
if [ $# -lt 2 ]; then
    echo "Error: Missing required date arguments." >&2
    show_usage
    exit 1
fi

startDate=$1
endDate=$2
endDate+=" 23:59:59"

logGroupName=${3:-"cmr-search-prod"}
awsProfile=${4:-"cmr-prod"}
awsRegion=${5:-"us-east-1"}
bucket=${6:-"bigstac-cmr-prod-logs"}

# Calculate timestamp versions of dates, appending filler zeroes for milliseconds
startStamp=$(date -j -f "%b %d %Y" "$startDate" +"%s")"000"
endStamp=$(date -j -f "%b %d %Y %H:%M:%S" "$endDate" +"%s")"000"

# Run export task
taskID_JSON=$(aws --profile $awsProfile --region $awsRegion logs create-export-task --log-group-name $logGroupName --from $startStamp --to $endStamp --destination $bucket)
taskID=$(jq '.taskId' <<< "$taskID_JSON" | tr -d '"')

echo "Export task $taskID started."
echo "When complete, the log data will be at:"
echo "s3://$bucket/exportedlogs/$taskID/"
echo
echo "Check status of the export job with:"
echo "aws --profile $awsProfile --region $awsRegion logs describe-export-tasks --task-id $taskID"
