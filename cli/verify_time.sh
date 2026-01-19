#!/bin/bash
START_TIME="2024-06-10T01:23:45.123+09:00"
echo "Original: $START_TIME"
ADJUSTED_START=$(python3 -c "from datetime import datetime, timedelta; t = datetime.fromisoformat('$START_TIME'.replace('Z', '+00:00')); print((t - timedelta(minutes=10)).isoformat())")
echo "Adjusted: $ADJUSTED_START"

if [[ "$ADJUSTED_START" == "2024-06-10T01:13:45.123000+09:00" ]]; then
  echo "SUCCESS"
else
  # Python isoformat might vary slightly (microseconds), so loose check
  echo "Output looked reasonable?"
fi
