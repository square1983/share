#!/bin/bash
set -euo pipefail

CLUSTER=$1
TASK_ID=$2
OUTPUT_FILE=$3

if [ -z "$CLUSTER" ] || [ -z "$TASK_ID" ] || [ -z "$OUTPUT_FILE" ]; then
  echo "使用法: $0 <CLUSTER> <TASK_ID> <OUTPUT_FILE>"
  exit 1
fi

TEMP_DIR=$(dirname "$OUTPUT_FILE")/temp_ecs_${TASK_ID}
mkdir -p "$TEMP_DIR"

# 1. Get Task Details to determine start/stop time
aws ecs describe-tasks \
  --cluster "$CLUSTER" \
  --tasks "$TASK_ID" \
  --output json \
  > "$TEMP_DIR/task_details.json"

# Extract timestamps (ISO 8601)
# Use startedAt as start, and stoppedAt (or now) as end
START_TIME=$(jq -r '.tasks[0].startedAt // .tasks[0].createdAt' "$TEMP_DIR/task_details.json")
END_TIME=$(jq -r '.tasks[0].stoppedAt // empty' "$TEMP_DIR/task_details.json")

# If task is still running or no stoppedAt, use current time
if [ -z "$END_TIME" ] || [ "$END_TIME" == "null" ]; then
    END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
fi

echo "タスク $TASK_ID ($START_TIME から $END_TIME) のメトリクスを取得中..."

# 2. Get Metrics via CloudWatch
# Note: ContainerInsights must be enabled.
# We fetch CPUUtil and MemoryUtil
# Period=60s

aws cloudwatch get-metric-statistics \
    --namespace ECS/ContainerInsights \
    --metric-name CPUUtilization \
    --dimensions Name=TaskId,Value="$TASK_ID" Name=ClusterName,Value="$CLUSTER" \
    --statistics Average Maximum \
    --period 60 \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --output json \
    > "$TEMP_DIR/cpu.json"

aws cloudwatch get-metric-statistics \
    --namespace ECS/ContainerInsights \
    --metric-name MemoryUtilization \
    --dimensions Name=TaskId,Value="$TASK_ID" Name=ClusterName,Value="$CLUSTER" \
    --statistics Average Maximum \
    --period 60 \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --output json \
    > "$TEMP_DIR/memory.json"

# Combine into one output
jq -n --slurpfile task "$TEMP_DIR/task_details.json" \
      --slurpfile cpu "$TEMP_DIR/cpu.json" \
      --slurpfile memory "$TEMP_DIR/memory.json" \
      '{ task: $task[0].tasks[0], metrics: { cpu: $cpu[0], memory: $memory[0] } }' \
      > "$OUTPUT_FILE"

rm -rf "$TEMP_DIR"
echo "✅ ECSメトリクスを $OUTPUT_FILE に保存しました"
