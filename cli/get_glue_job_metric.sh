#!/bin/bash
set -euo pipefail

JOB_NAME=$1
RUN_ID=$2
OUTPUT_FILE=$3

if [ -z "$JOB_NAME" ] || [ -z "$RUN_ID" ] || [ -z "$OUTPUT_FILE" ]; then
  echo "使用法: $0 <JOB_NAME> <RUN_ID> <OUTPUT_FILE>"
  exit 1
fi

TEMP_DIR=$(dirname "$OUTPUT_FILE")/temp_glue_${RUN_ID}
mkdir -p "$TEMP_DIR"

# 1. Get Job Run Details
aws glue get-job-run \
  --job-name "$JOB_NAME" \
  --run-id "$RUN_ID" \
  --output json \
  > "$TEMP_DIR/job_run.json"

# Extract timestamps (ISO 8601) specific to Glue structure
# .JobRun.StartedOn, .JobRun.CompletedOn
START_TIME=$(jq -r '.JobRun.StartedOn' "$TEMP_DIR/job_run.json")
END_TIME=$(jq -r '.JobRun.CompletedOn // empty' "$TEMP_DIR/job_run.json")

# If job is running
if [ -z "$END_TIME" ] || [ "$END_TIME" == "null" ]; then
    END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
fi

echo "Glueジョブ $JOB_NAME / $RUN_ID ($START_TIME から $END_TIME) のメトリクスを取得中..."

# 2. Get Metrics via CloudWatch
# Namespace: Glue
# Metric: glue.driver.cpuLoad (?) or glue.driver.aggregate.numExecutorAllocation (?)
# Common one: glue.driver.cpuLoad for standard jobs
# Dimensions: JobName, JobRunId, Type=gauge

aws cloudwatch get-metric-statistics \
    --namespace Glue \
    --metric-name glue.driver.cpuLoad \
    --dimensions Name=JobName,Value="$JOB_NAME" Name=JobRunId,Value="$RUN_ID" Name=Type,Value=gauge \
    --statistics Average Maximum \
    --period 300 \
    --start-time "$START_TIME" \
    --end-time "$END_TIME" \
    --output json \
    > "$TEMP_DIR/cpu_load.json"

aws cloudwatch get-metric-statistics \
     --namespace Glue \
     --metric-name glue.driver.memoryUsed \
     --dimensions Name=JobName,Value="$JOB_NAME" Name=JobRunId,Value="$RUN_ID" Name=Type,Value=gauge \
     --statistics Average Maximum \
     --period 300 \
     --start-time "$START_TIME" \
     --end-time "$END_TIME" \
     --output json \
     > "$TEMP_DIR/memory_used.json"

# Combine into one output
jq -n --slurpfile job "$TEMP_DIR/job_run.json" \
      --slurpfile cpu "$TEMP_DIR/cpu_load.json" \
      --slurpfile memory "$TEMP_DIR/memory_used.json" \
      '{ jobRun: $job[0].JobRun, metrics: { cpuLoad: $cpu[0], memoryUsed: $memory[0] } }' \
      > "$OUTPUT_FILE"

rm -rf "$TEMP_DIR"
echo "✅ Glueメトリクスを $OUTPUT_FILE に保存しました"
