#!/bin/bash

# Check if arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <s3_path> <yyyyMM>"
    exit 1
fi

S3_PATH=$1
YYYYMM=$2

# Check if YYYYMM is valid format
if ! [[ "$YYYYMM" =~ ^[0-9]{6}$ ]]; then
    echo "Error: Date format must be yyyyMM"
    exit 1
fi

# Local directory name
BASE_DIR="zero_file"

# Clean up and recreate base directory
if [ -d "$BASE_DIR" ]; then
    echo "Cleaning up existing $BASE_DIR..."
    rm -rf "$BASE_DIR"
fi
mkdir "$BASE_DIR"

# Determine number of days in the month using python for robustness
# Extract Year and Month
YEAR=${YYYYMM:0:4}
# Remove leading zero from month to avoid octal interpretation in some contexts, though python handles int conversion fine
MONTH=${YYYYMM:4:2}

# Use python to get the number of days in the month
DAYS_IN_MONTH=$(python3 -c "import calendar; print(calendar.monthrange(int($YEAR), int($MONTH))[1])")

echo "Generating files for $YYYYMM ($DAYS_IN_MONTH days)..."

for (( d=1; d<=DAYS_IN_MONTH; d++ )); do
    # Pad day with leading zero if needed
    printf -v DAY_STR "%02d" $d
    DATE_STR="${YYYYMM}${DAY_STR}"
    
    # Create daily directory
    DAILY_DIR="${BASE_DIR}/${DATE_STR}"
    mkdir -p "$DAILY_DIR"
    
    # Create empty file
    FILENAME="${DATE_STR}000000_linkage_VDB_z.ndjson"
    touch "${DAILY_DIR}/${FILENAME}"
done

echo "Files created locally in $BASE_DIR"

# Upload to S3
# echo "Uploading to S3: $S3_PATH"
# aws s3 cp "$BASE_DIR" "$S3_PATH" --recursive

echo "Done."
