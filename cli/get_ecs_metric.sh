#!/bin/bash
set -euo pipefail

CLUSTER=$1
TASK_ID=$2
START_TIME=$3
END_TIME=$4
FAMILY=$5
OUTPUT_FILE=$6

if [ -z "$CLUSTER" ] || [ -z "$TASK_ID" ] || [ -z "$START_TIME" ] || [ -z "$END_TIME" ] || [ -z "$FAMILY" ] || [ -z "$OUTPUT_FILE" ]; then
  echo "使用法: $0 <CLUSTER> <TASK_ID> <START_TIME> <END_TIME> <FAMILY> <OUTPUT_FILE>"
  exit 1
fi

TEMP_DIR=$(dirname "$OUTPUT_FILE")/temp_ecs_${TASK_ID}
mkdir -p "$TEMP_DIR"

echo "タスク $TASK_ID (Family: $FAMILY) ($START_TIME から $END_TIME) のメトリクスを取得中..."

# 1. Get Metrics via CloudWatch (directly, no describe-tasks for time)
# Note: ContainerInsights must be enabled.
# We fetch CPUUtil and MemoryUtil
# Period=60s

CMD_CPU="aws cloudwatch get-metric-statistics --namespace ECS/ContainerInsights --metric-name CPUUtilization --dimensions Name=TaskId,Value=$TASK_ID Name=ClusterName,Value=$CLUSTER --statistics Average Maximum --period 60 --start-time $START_TIME --end-time $END_TIME --output json"
echo "DEBUG: Executing: $CMD_CPU"
echo "$CMD_CPU" >> cmds.txt
$CMD_CPU > "$TEMP_DIR/cpu.json"

CMD_MEM="aws cloudwatch get-metric-statistics --namespace ECS/ContainerInsights --metric-name MemoryUtilization --dimensions Name=TaskId,Value=$TASK_ID Name=ClusterName,Value=$CLUSTER --statistics Average Maximum --period 60 --start-time $START_TIME --end-time $END_TIME --output json"
echo "DEBUG: Executing: $CMD_MEM"
echo "$CMD_MEM" >> cmds.txt
$CMD_MEM > "$TEMP_DIR/memory.json"

# Combine into one output (Constructing minimal task object)
jq -n --arg cluster "$CLUSTER" --arg taskArn "$TASK_ID" --arg start "$START_TIME" --arg stop "$END_TIME" \
      --slurpfile cpu "$TEMP_DIR/cpu.json" \
      --slurpfile memory "$TEMP_DIR/memory.json" \
      '{ 
         task: { clusterArn: $cluster, taskArn: $taskArn, startedAt: $start, stoppedAt: $stop }, 
         metrics: { cpu: $cpu[0], memory: $memory[0] } 
       }' \
      > "$OUTPUT_FILE"

rm -rf "$TEMP_DIR"
echo "✅ ECSメトリクスを $OUTPUT_FILE に保存しました"
