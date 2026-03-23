#!/usr/bin/env bash
#
# collect_and_combine.sh
#
# Collects all CSVs from a specific orchestrator run and combines them
# into a single report in the data/ directory.
#
# Usage:
#   bash collect_and_combine.sh <RunTag>
#
# Example:
#   bash collect_and_combine.sh 20260317-093000
#
# Run this from the top-level OrchFolder directory.
#

set -Eeuo pipefail

if [ -z "${1:-}" ]; then
  echo "Usage: bash collect_and_combine.sh <RunTag>"
  echo "Example: bash collect_and_combine.sh 20260317-093000"
  echo ""
  echo "Available runs in out/:"
  ls -1 out/ 2>/dev/null || echo "  (none found)"
  exit 1
fi

RUNTAG="$1"
RUN_DIR="./out/${RUNTAG}"
DATA_DIR="./data/${RUNTAG}"

if [ ! -d "$RUN_DIR" ]; then
  echo "ERROR: Run directory not found: ${RUN_DIR}"
  echo ""
  echo "Available runs in out/:"
  ls -1 out/ 2>/dev/null || echo "  (none found)"
  exit 1
fi

# Step 1: Collect and rename CSVs into data/<RunTag>/
echo "Collecting CSVs from ${RUN_DIR}..."

csv_count=0
for file in $(find "$RUN_DIR" -name "*.csv"); do
  npath="$(realpath "$file" | grep -o 'out.*')"
  date="$(echo "$npath" | awk -F '/' '{ print $2 }' | sed -e 's/-.*//g')"
  foundation="$(echo "$npath" | awk -F '/' '{ print $4 }')"
  file_group="$(echo "$npath" | awk -F '/' '{ print $7 }')"

  # Skip empty values (e.g. cache files or unexpected paths)
  if [ -z "$file_group" ] || [ -z "$foundation" ]; then
    continue
  fi

  new_file="${file_group}_${foundation}_${date}.csv"
  mkdir -p "${DATA_DIR}/${file_group}"
  cp "$file" "${DATA_DIR}/${file_group}/${new_file}"
  echo "  -> ${file_group}/${new_file}"
  csv_count=$((csv_count + 1))
done

if [ "$csv_count" -eq 0 ]; then
  echo "WARNING: No CSVs found in ${RUN_DIR}"
  exit 1
fi

echo ""
echo "Collected ${csv_count} CSVs into ${DATA_DIR}/"

# Step 2: Combine all CSVs into a single report
echo ""
echo "Combining CSVs into combined report..."

# Extract date (YYYYMMDD) from RunTag (YYYYMMDD-HHMMSS)
RUN_DATE="${RUNTAG%%-*}"
COMBINED="${DATA_DIR}/cf_fs4migration_${RUN_DATE}.csv"
first=true

for f in $(find "$DATA_DIR" -name "*.csv" ! -name "cf_fs4migration_*.csv" | sort); do
  if [ "$first" = true ]; then
    head -1 "$f" > "$COMBINED"
    first=false
  fi
  tail -n +2 "$f" >> "$COMBINED"
done

total_rows=$(tail -n +2 "$COMBINED" | wc -l | tr -d ' ')
echo "Done. Combined report: ${COMBINED} (${total_rows} data rows)"