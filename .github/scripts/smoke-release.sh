#!/bin/sh
set -eu

archive=${1:?usage: smoke-release.sh <release archive>}
here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)

tar -xzf "$archive" -C "$work"
ashiato=$(echo "$work"/ashiato-*/bin/ashiato)

"$here/../../examples/pipeline/make-logs.sh" "$work/logs"
"$ashiato" ingest --workspace "$work/case" --source "$work/logs"
"$ashiato" analyze --workspace "$work/case" \
  --since 2026-10-01 --until 2026-10-02 --emit-signals > "$work/output"

signals=$(grep -o '"signal_id":"[^"]*"' "$work/output" | sort -u)
expected='"signal_id":"cloudtrail_logging_disabled"
"signal_id":"console_login_without_mfa"'

if [ "$signals" != "$expected" ]; then
  echo "unexpected signals:" >&2
  echo "$signals" >&2
  exit 1
fi
echo "release smoke test passed"
