#!/bin/bash
set -euo pipefail

# collect_and_upload.sh
# Step Functionsの実行データを収集し、S3にアップロードするオーケストレーションスクリプト。

EXECUTION_ARN=$1
S3_DESTINATION=$2
MOCK_MODE="${3:-false}" # "true" に設定するとモック履歴を使用

if [ -z "$EXECUTION_ARN" ] || [ -z "$S3_DESTINATION" ]; then
  echo "使用法: $0 <EXECUTION_ARN> <S3_DESTINATION> [true|false(mock_mode)]"
  echo "例: $0 arn:aws:states:us-east-1:123:execution:MySM:ID s3://my-bucket/path"
  exit 1
fi

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BASE_DIR="sf_data_${TIMESTAMP}"
METRICS_DIR="${BASE_DIR}/metrics"
mkdir -p "$METRICS_DIR"
rm -f "${METRICS_DIR}"/*

echo "作業ディレクトリ: $BASE_DIR"

# ==========================================
# 関数定義
# ==========================================

adjust_time() {
    local input=$1
    local shift_mins=$2
    
    # 1. Clean input for universal parsing
    # Handle "Z" -> "+0000"
    local clean_input="$input"
    if [[ "$input" == *Z ]]; then
        clean_input="${input%Z}+0000"
    elif [[ "$input" =~ :[0-9]{2}$ ]]; then
        # If matches :XX at end (timezone), strip colon for BSD compatibility 
        # (GNU date handles both, usually)
        clean_input=$(echo "$input" | sed 's/\(.*\):/\1/')
    fi
    
    # Strip subseconds (.123) for robust parsing on both
    local clean_input_simplified=$(echo "$clean_input" | sed -E 's/\.[0-9]+//')

    local epoch=""
    
    # 2. Try GNU date (Linux/CloudShell)
    if date --version >/dev/null 2>&1; then
        # GNU date uses -d
        epoch=$(date -d "$clean_input_simplified" +%s 2>/dev/null)
    else
        # BSD date (macOS) uses -j -f
        # Format expected: %Y-%m-%dT%H:%M:%S%z (since we stripped logic)
        epoch=$(date -j -f "%Y-%m-%dT%H:%M:%S%z" "$clean_input_simplified" "+%s" 2>/dev/null)
    fi
    
    if [ -z "$epoch" ]; then
        echo "Error parsing $input (cleaned: $clean_input_simplified)" >&2
        echo "$input"
        return
    fi
    
    # 3. Shift
    local new_epoch=$((epoch + (shift_mins * 60)))
    
    # 4. Format back to ISO 8601 UTC
    if date --version >/dev/null 2>&1; then
        # GNU date
        date -u -d "@$new_epoch" "+%Y-%m-%dT%H:%M:%SZ"
    else
        # BSD date
        date -u -r "$new_epoch" "+%Y-%m-%dT%H:%M:%SZ"
    fi
}

get_lambda_insights() {
    local REQUEST_ID=$1
    local OUTPUT_FILE=$2

    if [ -z "$REQUEST_ID" ] || [ -z "$OUTPUT_FILE" ]; then
        echo "エラー: get_lambda_insights 引数が不足しています"
        return 1
    fi

    echo "   (Lambda) RequestID: $REQUEST_ID のインサイトを取得中..."
    CMD_LOGS="aws logs filter-log-events --log-group-name \"/aws/lambda-insights\" --filter-pattern \"{ $.request_id = \\\"$REQUEST_ID\\\" }\" --output json"
    echo "$CMD_LOGS" >> cmds.txt
    aws logs filter-log-events \
      --log-group-name "/aws/lambda-insights" \
      --filter-pattern "{ $.request_id = \"$REQUEST_ID\" }" \
      --output json \
      > "$OUTPUT_FILE"
    
    echo "   (Lambda) 完了: $OUTPUT_FILE"
}

get_ecs_metric() {
    local CLUSTER=$1
    local TASK_ID=$2
    local START_TIME=$3
    local END_TIME=$4
    local FAMILY=$5
    local OUTPUT_FILE=$6

    if [ -z "$CLUSTER" ] || [ -z "$TASK_ID" ] || [ -z "$START_TIME" ] || [ -z "$END_TIME" ] || [ -z "$OUTPUT_FILE" ]; then
        echo "エラー: get_ecs_metric 引数が不足しています (CLUSTER TASK_ID START_TIME END_TIME FAMILY OUTPUT_FILE)"
        return 1
    fi

    local TEMP_DIR=$(dirname "$OUTPUT_FILE")/temp_ecs_${TASK_ID}
    mkdir -p "$TEMP_DIR"

    echo "   (ECS) タスク $TASK_ID (Family: $FAMILY) ($START_TIME から $END_TIME) のメトリクスを取得中..."

    # 2. CloudWatch metrics
    CMD_CPU="aws cloudwatch get-metric-statistics --namespace ECS/ContainerInsights --metric-name CPUUtilization --dimensions Name=TaskId,Value=$TASK_ID Name=ClusterName,Value=$CLUSTER --statistics Average Maximum --period 60 --start-time $START_TIME --end-time $END_TIME --output json"
    echo "      DEBUG: Executing: $CMD_CPU"
    echo "$CMD_CPU" >> cmds.txt
    $CMD_CPU > "$TEMP_DIR/cpu.json"

    CMD_MEM="aws cloudwatch get-metric-statistics --namespace ECS/ContainerInsights --metric-name MemoryUtilization --dimensions Name=TaskId,Value=$TASK_ID Name=ClusterName,Value=$CLUSTER --statistics Average Maximum --period 60 --start-time $START_TIME --end-time $END_TIME --output json"
    echo "      DEBUG: Executing: $CMD_MEM"
    echo "$CMD_MEM" >> cmds.txt
    $CMD_MEM > "$TEMP_DIR/memory.json"

    # 3. 結合 (Construct a minimal task object since we don't call describe-tasks)
    jq -n --arg cluster "$CLUSTER" --arg taskArn "$TASK_ID" --arg start "$START_TIME" --arg stop "$END_TIME" --arg family "$FAMILY" \
          --slurpfile cpu "$TEMP_DIR/cpu.json" \
          --slurpfile memory "$TEMP_DIR/memory.json" \
          '{ 
             task: { clusterArn: $cluster, taskArn: $taskArn, startedAt: $start, stoppedAt: $stop, family: $family }, 
             metrics: { cpu: $cpu[0], memory: $memory[0] } 
           }' \
          > "$OUTPUT_FILE"

    rm -rf "$TEMP_DIR"
    echo "   (ECS) 完了: $OUTPUT_FILE"
}

get_glue_job_metric() {
    local JOB_NAME=$1
    local RUN_ID=$2
    local OUTPUT_FILE=$3

    if [ -z "$JOB_NAME" ] || [ -z "$RUN_ID" ] || [ -z "$OUTPUT_FILE" ]; then
        echo "エラー: get_glue_job_metric 引数が不足しています"
        return 1
    fi

    local TEMP_DIR=$(dirname "$OUTPUT_FILE")/temp_glue_${RUN_ID}
    mkdir -p "$TEMP_DIR"

    # 1. Job Run 詳細取得
    CMD_GLUE_RUN="aws glue get-job-run --job-name $JOB_NAME --run-id $RUN_ID --output json"
    echo "$CMD_GLUE_RUN" >> cmds.txt
    aws glue get-job-run \
      --job-name "$JOB_NAME" \
      --run-id "$RUN_ID" \
      --output json \
      > "$TEMP_DIR/job_run.json"

    local START_TIME=$(jq -r '.JobRun.StartedOn' "$TEMP_DIR/job_run.json")
    local END_TIME=$(jq -r '.JobRun.CompletedOn // empty' "$TEMP_DIR/job_run.json")

    if [ -z "$END_TIME" ] || [ "$END_TIME" == "null" ]; then
        END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    fi

    # Adjust Time Window (+/- 10 minutes)
    # Using shell function adjust_time
    ADJUSTED_START=$(adjust_time "$START_TIME" "-10")
    ADJUSTED_END=$(adjust_time "$END_TIME" "10")

    echo "   (Glue) ジョブ $JOB_NAME / $RUN_ID ($START_TIME -> $END_TIME) を調整: ($ADJUSTED_START -> $ADJUSTED_END)"

    # 2. CloudWatch metrics
    # Use ADJUSTED_START and ADJUSTED_END
    CMD_GLUE_CPU="aws cloudwatch get-metric-statistics --namespace Glue --metric-name glue.driver.cpuLoad --dimensions Name=JobName,Value=$JOB_NAME Name=JobRunId,Value=$RUN_ID Name=Type,Value=gauge --statistics Average Maximum --period 300 --start-time $ADJUSTED_START --end-time $ADJUSTED_END --output json"
    echo "$CMD_GLUE_CPU" >> cmds.txt
    aws cloudwatch get-metric-statistics \
        --namespace Glue \
        --metric-name glue.driver.cpuLoad \
        --dimensions Name=JobName,Value="$JOB_NAME" Name=JobRunId,Value="$RUN_ID" Name=Type,Value=gauge \
        --statistics Average Maximum \
        --period 300 \
        --start-time "$ADJUSTED_START" \
        --end-time "$ADJUSTED_END" \
        --output json \
        > "$TEMP_DIR/cpu_load.json"

    CMD_GLUE_MEM="aws cloudwatch get-metric-statistics --namespace Glue --metric-name glue.driver.memoryUsed --dimensions Name=JobName,Value=$JOB_NAME Name=JobRunId,Value=$RUN_ID Name=Type,Value=gauge --statistics Average Maximum --period 300 --start-time $ADJUSTED_START --end-time $ADJUSTED_END --output json"
    echo "$CMD_GLUE_MEM" >> cmds.txt
    aws cloudwatch get-metric-statistics \
         --namespace Glue \
         --metric-name glue.driver.memoryUsed \
         --dimensions Name=JobName,Value="$JOB_NAME" Name=JobRunId,Value="$RUN_ID" Name=Type,Value=gauge \
         --statistics Average Maximum \
         --period 300 \
         --start-time "$ADJUSTED_START" \
         --end-time "$ADJUSTED_END" \
         --output json \
         > "$TEMP_DIR/memory_used.json"

    # 3. 結合
    jq -n --slurpfile job "$TEMP_DIR/job_run.json" \
          --slurpfile cpu "$TEMP_DIR/cpu_load.json" \
          --slurpfile memory "$TEMP_DIR/memory_used.json" \
          '{ jobRun: $job[0].JobRun, metrics: { cpuLoad: $cpu[0], memoryUsed: $memory[0] } }' \
          > "$OUTPUT_FILE"

    rm -rf "$TEMP_DIR"
    echo "   (Glue) 完了: $OUTPUT_FILE"
}

# ==========================================
# メイン処理
# ==========================================

# 1. 実行履歴の取得
echo "実行履歴を取得中..."
if [ "$MOCK_MODE" == "true" ]; then
    echo "MOCK モード: mock_sf_history.json を使用します"
    cp mock_sf_history.json "$BASE_DIR/history.json"
else
    # 既存のスクリプトを使用して履歴を取得 (ここはBash呼び出しのままにするか、インライン化可能だが一旦維持)
    bash get_step_functions_execution_history.sh "$EXECUTION_ARN" "$BASE_DIR/history.json"
fi

# 2. 履歴の解析
echo "実行履歴を解析中..."

DERIVED_SM_ARN=$(echo "$EXECUTION_ARN" | sed 's/:execution:/:stateMachine:/' | sed 's/:[^:]*$//')
if [ "$MOCK_MODE" == "true" ]; then
   DERIVED_SM_ARN="arn:aws:states:ap-northeast-1:123456789012:stateMachine:MockStateMachine"
fi

echo "推定された StateMachineArn: $DERIVED_SM_ARN"

jq -f parser.jq --arg inputStateMachineArn "$DERIVED_SM_ARN" "$BASE_DIR/history.json" > "$BASE_DIR/index.json"

echo "インデックス作成完了: $BASE_DIR/index.json"

# 3. メトリクスの収集
echo "各ステップのメトリクスを収集内..."

jq -r '.steps[] | @base64' "$BASE_DIR/index.json" | while read -r step_b64; do
    _jq() {
     echo "$step_b64" | base64 --decode | jq -r "$1"
    }

    TYPE=$(_jq '.type')
    STATUS=$(_jq '.status')
    STEP_NAME=$(_jq '.stepName')
    
    # ファイル名用にステップ名をサニタイズ
    # 日本語文字を維持し、ファイルシステムで問題になる文字のみ置換 (/ : スペース)
    SAFE_STEP_NAME=$(echo "$STEP_NAME" | sed 's/[/:[:space:]]/_/g')

    echo "   処理中 [$TYPE] $STEP_NAME ($STATUS)..."

    if [ "$STATUS" != "Succeeded" ] && [ "$STATUS" != "Failed" ] && [ "$STATUS" != "TimedOut" ]; then
        echo "      ステータスが対象外のためスキップ: $STATUS"
        continue
    fi
    
    if [ "$MOCK_MODE" == "true" ]; then
        echo "{\"mock\": true, \"step\": \"$STEP_NAME\"}" > "$METRICS_DIR/${TYPE}_${SAFE_STEP_NAME}.json"
        continue
    fi

    if [ "$TYPE" == "lambda" ]; then
        # Try to use requestId first, fallback to executionId or resource
        RESOURCE_ID=$(_jq '.requestId // .executionId // .resource')
        
        if [ "$RESOURCE_ID" == "UNKNOWN" ] || [ "$RESOURCE_ID" == "null" ]; then
             echo "      ⚠️ Lambda RequestID が見つかりません。リソースARNを使用します。"
             RESOURCE_ID=$(_jq '.resource')
        fi
        
        get_lambda_insights "$RESOURCE_ID" "$METRICS_DIR/lambda_${SAFE_STEP_NAME}.json" || echo "      Lambdaメトリクスの取得に失敗しました"

    elif [ "$TYPE" == "ecs" ]; then
        CLUSTER=$(_jq '.clusterArn')
        TASK_ID=$(_jq '.taskId')
        START_TIME=$(_jq '.startTime')
        END_TIME=$(_jq '.endTime')
        FAMILY=$(_jq '.family')
        # If END_TIME is null (e.g. running?), default to now or handle it. 
        # But sf_parser usually provides timestamps.
        
        get_ecs_metric "$CLUSTER" "$TASK_ID" "$START_TIME" "$END_TIME" "$FAMILY" "$METRICS_DIR/ecs_${SAFE_STEP_NAME}.json" || echo "      ECSメトリクスの取得に失敗しました"

    elif [ "$TYPE" == "glue" ]; then
        JOB_NAME=$(_jq '.jobName')
        RUN_ID=$(_jq '.jobRunId')
        get_glue_job_metric "$JOB_NAME" "$RUN_ID" "$METRICS_DIR/glue_${SAFE_STEP_NAME}.json" || echo "      Glueメトリクスの取得に失敗しました"
        
    elif [ "$TYPE" == "step_function" ]; then
        EXEC_ARN=$(_jq '.executionArn')
        echo "      ネストされた実行を検出: $EXEC_ARN"
    fi
done

# 4. S3 へのアップロード (Zip圧縮してアップロード)
ZIP_FILE="${BASE_DIR}.zip"
echo "データを圧縮中: $ZIP_FILE ..."

# zip コマンドの確認
if ! command -v zip &> /dev/null; then
    echo "エラー: zip コマンドが見つかりません。圧縮できません。"
    exit 1
fi

zip -r "$ZIP_FILE" "$BASE_DIR" > /dev/null

echo "S3へアップロード中: $S3_DESTINATION/sf_data_${TIMESTAMP}.zip"
if [ "$MOCK_MODE" == "true" ]; then
    echo "MOCK モード: S3アップロードをスキップします。ファイル保存先: $ZIP_FILE"
else
    aws s3 cp "$ZIP_FILE" "$S3_DESTINATION/"
fi

echo "完了! データ収集が終了しました。"
