#!/bin/bash
EXECUTION_ARN=$1

if [ -z "$EXECUTION_ARN" ]; then
  echo "使用法: $0 <execution-arn|MOCK>"
  exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "エラー: jq が必要ですがインストールされていません。"
    exit 1
fi

if [ "$EXECUTION_ARN" == "MOCK" ]; then
  cat "$DIR/mock_sf_history.json"
else
  bash "$DIR/get_step_functions_execution_history.sh" "$EXECUTION_ARN" /dev/stdout 2>/dev/null
fi | jq -f "$DIR/sf_parser.jq"
