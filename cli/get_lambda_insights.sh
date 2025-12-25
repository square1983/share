#!/bin/bash
set -euo pipefail

REQUEST_ID=$1
OUTPUT_FILE=$2

if [ -z "$REQUEST_ID" ] || [ -z "$OUTPUT_FILE" ]; then
  echo "使用法: $0 <REQUEST_ID> <OUTPUT_FILE>"
  exit 1
fi

# Query Lambda Insights log group
# Notes:
# 1. Lambda Insights logs are usually in /aws/lambda-insights
# 2. We filter by request_id field in the embedded JSON
aws logs filter-log-events \
  --log-group-name "/aws/lambda-insights" \
  --filter-pattern "{ $.request_id = \"$REQUEST_ID\" }" \
  --output json \
  > "$OUTPUT_FILE"

echo "✅ Lambda Insights を $OUTPUT_FILE に保存しました"
