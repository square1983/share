#!/usr/bin/env bash
set -euo pipefail

# -------------------------
# 参数解析
# -------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --service) SERVICE="$2"; shift 2 ;;
    --name) NAME="$2"; shift 2 ;;
    --execution-id) EXECUTION_ID="$2"; shift 2 ;;
    --s3-bucket) S3_BUCKET="$2"; shift 2 ;;
    --s3-prefix) S3_PREFIX="$2"; shift 2 ;;
    --output-dir) OUTPUT_DIR="$2"; shift 2 ;;
    *)
      echo "不明な引数: $1"; exit 1 ;;
  esac
done

if [[ -z "${SERVICE:-}" || -z "${NAME:-}" || -z "${EXECUTION_ID:-}" ]]; then
  echo "必要な引数が不足しています: --service, --name, --execution-id"
  exit 1
fi

if [[ -z "${S3_BUCKET:-}" && -z "${OUTPUT_DIR:-}" ]]; then
  echo "必要な引数が不足しています: --s3-bucket または --output-dir のいずれかを指定する必要があります"
  exit 1
fi

if [ -n "${OUTPUT_DIR:-}" ]; then
  WORKDIR="${OUTPUT_DIR}"
else
  TS=$(date +%Y%m%d_%H%M%S)
  WORKDIR="execution_${TS}"
fi
mkdir -p "${WORKDIR}"

# -------------------------
# Meta 信息
# -------------------------
cat <<EOF > "${WORKDIR}/meta.json"
{
  "service": "${SERVICE}",
  "name": "${NAME}",
  "execution_id": "${EXECUTION_ID}",
  "collected_at": "$(date -Iseconds)"
}
EOF

# -------------------------
# Lambda
# -------------------------
if [[ "${SERVICE}" == "lambda" ]]; then
  LOG_GROUP="/aws/lambda/${NAME}"

  # 执行日志
  aws logs filter-log-events \
    --log-group-name "${LOG_GROUP}" \
    --filter-pattern "${EXECUTION_ID}" \
    --output json \
    > "${WORKDIR}/logs.json"

  # Lambda Insights
  aws logs filter-log-events \
    --log-group-name "/aws/lambda-insights" \
    --filter-pattern "{ $.request_id = \"${EXECUTION_ID}\" }" \
    --output json \
    > "${WORKDIR}/metrics.json"

fi

# -------------------------
# ECS
# -------------------------
if [[ "${SERVICE}" == "ecs" ]]; then
  CLUSTER="${NAME}"

  aws ecs describe-tasks \
    --cluster "${CLUSTER}" \
    --tasks "${EXECUTION_ID}" \
    --output json \
    > "${WORKDIR}/meta_ecs.json"

  START=$(jq -r '.tasks[0].startedAt' "${WORKDIR}/meta_ecs.json")
  END=$(jq -r '.tasks[0].stoppedAt' "${WORKDIR}/meta_ecs.json")

  aws cloudwatch get-metric-statistics \
    --namespace ECS/ContainerInsights \
    --metric-name CPUUtilization \
    --dimensions Name=TaskId,Value="${EXECUTION_ID}" Name=ClusterName,Value="${CLUSTER}" \
    --statistics Average Maximum \
    --period 60 \
    --start-time "${START}" \
    --end-time "${END}" \
    --output json \
    > "${WORKDIR}/metrics.json"
fi

# -------------------------
# Glue
# -------------------------
if [[ "${SERVICE}" == "glue" ]]; then
  aws glue get-job-run \
    --job-name "${NAME}" \
    --run-id "${EXECUTION_ID}" \
    --output json \
    > "${WORKDIR}/meta_glue.json"

  START=$(jq -r '.JobRun.StartedOn' "${WORKDIR}/meta_glue.json")
  END=$(jq -r '.JobRun.CompletedOn' "${WORKDIR}/meta_glue.json")

  aws cloudwatch get-metric-statistics \
    --namespace Glue \
    --metric-name glue.driver.cpuLoad \
    --dimensions \
      Name=JobName,Value="${NAME}" \
      Name=JobRunId,Value="${EXECUTION_ID}" \
      Name=Type,Value=gauge \
    --statistics Average Maximum \
    --period 300 \
    --start-time "${START}" \
    --end-time "${END}" \
    --output json \
    > "${WORKDIR}/metrics.json"
fi

# -------------------------
# 打包 & 上传 (仅在没有指定 OUTPUT_DIR 时执行)
# -------------------------
if [ -z "${OUTPUT_DIR:-}" ]; then
  tar czf "${WORKDIR}.tar.gz" "${WORKDIR}"

  aws s3 cp \
    "${WORKDIR}.tar.gz" \
    "s3://${S3_BUCKET}/${S3_PREFIX:-executions}/${WORKDIR}.tar.gz"

  echo "✅ 収集してアップロードしました: s3://${S3_BUCKET}/${S3_PREFIX:-executions}/${WORKDIR}.tar.gz"
fi