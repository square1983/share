#!/bin/bash
set -euo pipefail

EXECUTION_ARN=$1
OUTPUT_FILE=$2

if [ -z "$EXECUTION_ARN" ] || [ -z "$OUTPUT_FILE" ]; then
  echo "使用法: $0 <EXECUTION_ARN> <OUTPUT_FILE>"
  exit 1
fi

aws stepfunctions get-execution-history \
  --execution-arn "$EXECUTION_ARN" \
  --include-execution-data \
  --max-results 1000 \
  --output json \
  > "$OUTPUT_FILE"

echo "✅ 実行履歴を $OUTPUT_FILE に保存しました"
