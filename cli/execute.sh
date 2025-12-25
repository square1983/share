#!/bin/bash
set -e

EXECUTION_ARN=$1
S3_BUCKET=$2

if [ -z "$EXECUTION_ARN" ] || [ -z "$S3_BUCKET" ]; then
    echo "使用法: $0 <execution-arn> <s3-bucket>"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
EXECUTION_NAME=$(echo "$EXECUTION_ARN" | awk -F: '{print $NF}')
BASE_DIR="sf_execution_${EXECUTION_NAME}_${TIMESTAMP}"

trap "echo '[クリーンアップ] 一時ディレクトリを削除中...'; rm -rf $BASE_DIR $BASE_DIR.tar.gz" EXIT

mkdir -p "$BASE_DIR"

# 1. Fetch Execution History raw
echo "[フェーズ 1] 実行履歴を取得中..."
if [ ! -f "$DIR/get_step_functions_execution_history.sh" ]; then
    echo "エラー: get_step_functions_execution_history.sh が見つかりません！"
    exit 1
fi

"$DIR/get_step_functions_execution_history.sh" "$EXECUTION_ARN" "$BASE_DIR/history.json"

# 2. Parse Index (using local parsing logic)
echo "[フェーズ 2] 実行グラフを解析中..."
cat "$BASE_DIR/history.json" | jq -f "$DIR/sf_parser.jq" > "$BASE_DIR/index.json"

echo "インデックスが $BASE_DIR/index.json に作成されました"

echo "[フェーズ 3] メトリクスを収集中..."

# Iterate through steps
cat "$BASE_DIR/index.json" | jq -c '.steps[]' | while read step; do
    TYPE=$(echo "$step" | jq -r '.type')
    NAME=$(echo "$step" | jq -r '.stepName')
    
    if [ "$TYPE" == "generic" ] || [ "$TYPE" == "null" ]; then
        continue
    fi
    
    echo "$NAME ($TYPE) を処理中..."
    OUTPUT_PATH="$BASE_DIR/$NAME"
    mkdir -p "$OUTPUT_PATH"
    
    if [ "$TYPE" == "lambda" ]; then
       EXEC_ID=$(echo "$step" | jq -r '.executionId')
       
       # 1. Lambda Insights
       "$DIR/get_lambda_insights.sh" "$EXEC_ID" "$OUTPUT_PATH/insights.json"
       
       # 2. Standard Logs/Meta
       FN_NAME=$(echo "$step" | jq -r '.resource' | awk -F: '{print $7}')
        if [ "$FN_NAME" == "null" ] || [ -z "$FN_NAME" ]; then
           FN_NAME=$(echo "$step" | jq -r '.resource')
       fi
       
       "$DIR/collect_node.sh" --service lambda --name "$FN_NAME" --execution-id "$EXEC_ID" --output-dir "$OUTPUT_PATH"

    elif [ "$TYPE" == "ecs" ]; then
       CLUSTER=$(echo "$step" | jq -r '.clusterArn')
       CLUSTER_NAME=$(echo "$CLUSTER" | awk -F/ '{print $NF}')
       TASK_ID=$(echo "$step" | jq -r '.taskId')
       
       # 1. ECS Metrics (via get_ecs_metric.sh)
       "$DIR/get_ecs_metric.sh" "$CLUSTER_NAME" "$TASK_ID" "$OUTPUT_PATH/ecs_metrics.json"

    elif [ "$TYPE" == "glue" ]; then
       JOB_NAME=$(echo "$step" | jq -r '.jobName')
       RUN_ID=$(echo "$step" | jq -r '.jobRunId')
       
       # 1. Glue Metrics (via get_glue_job_metric.sh)
       "$DIR/get_glue_job_metric.sh" "$JOB_NAME" "$RUN_ID" "$OUTPUT_PATH/glue_metrics.json"
       
       # 2. Support collected via collect_node (logs) is redundant if get_glue_job_metric handles it?
       # get_glue_job_metric handles job run + metrics. 
       # Logs? collect_node handles `aws logs filter-log-events --log-group-name /aws-glue/jobs/output`.
       # get_glue_job_metric does NOT currently fetch text logs.
       # The user request was "find glue job and get metric data".
       # I will prioritize metrics. 
       # But let's verify if collect_node has glue logic.
       # Yes, existing collect_node.sh has a Glue block for logs.
       # So I should keep it for logs OR move log fetching to get_glue_job_metric.sh?
       # The prompt specifically asked for "get metric data". 
       # I will keep get_glue_job_metric focused on Metrics + JobRun Meta.
       # And I will KEEP calling collect_node for logs if it adds value, 
       # however, I'd rather move towards the focused scripts.
       # Actually, checking `collect_node.sh` again... it does metrics too.
       # Using `get_glue_job_metric.sh` is cleaner.
       # If I don't call `collect_node.sh`, I miss text logs.
       # But maybe that's fine as per user request (focus on metrics).
       # I will omit collect_node.sh for Glue to rely on the new script, assuming get_glue_job_metric.sh is the "Gold Standard" now.
       # Unless user wants logs. Steps usually imply focusing on what was requested.
       pass
    fi
done

echo "[フェーズ 4] 圧縮してアップロード中..."
PKG_NAME="${BASE_DIR}.tar.gz"

tar czf "$PKG_NAME" "$BASE_DIR"
aws s3 cp "$PKG_NAME" "s3://$S3_BUCKET/executions/$PKG_NAME"

echo "✅ 成功！完全な実行レポートがアップロードされました: s3://$S3_BUCKET/executions/$PKG_NAME"
