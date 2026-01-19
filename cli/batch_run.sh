#!/bin/bash
set -euo pipefail

# batch_run.sh
# Reads a list of Step Functions Execution ARNs from a file and runs exe.sh for each.
# Finally aggregates all data into a single Excel report.

INPUT_FILE=$1
S3_BASE=$2
MOCK_MODE="${3:-false}"

if [ -z "$INPUT_FILE" ] || [ -z "$S3_BASE" ]; then
    echo "Usage: $0 <executions_list_file> <s3_base_path> [true|false(mock_mode)]"
    echo "Example: $0 executions.txt s3://my-bucket/logs"
    exit 1
fi

if [ ! -f "$INPUT_FILE" ]; then
    echo "Error: Input file '$INPUT_FILE' not found."
    exit 1
fi

# 1. Run exe.sh for each execution
while IFS= read -r arn || [ -n "$arn" ]; do
    # Skip empty lines or comments
    [[ -z "$arn" || "$arn" =~ ^# ]] && continue
    
    # Trim whitespace
    arn=$(echo "$arn" | xargs)
    
    echo "----------------------------------------------------------------"
    echo "Start Processing: $arn"
    echo "----------------------------------------------------------------"
    
    # Run exe.sh
    # We use a dummy S3 path per execution or just pass the base.
    # exe.sh doesn't enforce unique S3 usage, it just uses it for upload if implemented.
    # We'll append execution ID to S3 path to avoid conflicts if exe.sh uploads things.
    # Extract Exec ID (last part of ARN)
    EXEC_ID=$(echo "$arn" | awk -F: '{print $NF}')
    S3_DEST="${S3_BASE}/${EXEC_ID}"
    
    ./exe.sh "$arn" "$S3_DEST" "$MOCK_MODE"
    
    echo "----------------------------------------------------------------"
    echo "Finished: $arn"
    echo "----------------------------------------------------------------"
    
done < "$INPUT_FILE"

# 2. Aggregate all results into Excel
OUTPUT_XLSX="metrics_batch_report.xlsx"
echo "Aggregating metrics to $OUTPUT_XLSX ..."

# Run metrics.py on the current directory (which contains sf_data_* folders created by exe.sh)
python3 metrics.py . "$OUTPUT_XLSX"

echo "Batch execution and reporting complete."
