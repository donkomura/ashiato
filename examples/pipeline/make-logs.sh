#!/bin/sh
set -eu

out=${1:?usage: make-logs.sh <output directory>}
here=$(cd "$(dirname "$0")" && pwd)

for record in "$here"/records/*.json; do
  name=$(basename "$record" .json)
  account=${name%%_*}
  rest=${name#*_}
  region=${rest%%_*}
  time=${rest#*_}
  day=$(echo "$time" | sed 's|^\(....\)\(..\)\(..\).*|\1/\2/\3|')
  dir="$out/AWSLogs/$account/CloudTrail/$region/$day"
  mkdir -p "$dir"
  gzip -cn "$record" > "$dir/${account}_CloudTrail_${region}_${time}_example.json.gz"
done
