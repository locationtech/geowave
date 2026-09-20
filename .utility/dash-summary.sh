#!/usr/bin/env bash
#
# Generate the Eclipse Dash dependency summary for the whole reactor.
#
# license-check is an aggregator goal, so one invocation covers every module.
# It resolves test-scope dependencies, which means the whole reactor has to
# resolve -- run "mvn install" first so GeoWave's own inter-module artifacts are
# available, otherwise they are silently missing from the summary.
#
# Usage: .utility/dash-summary.sh <output-file> [extra mvn args...]
set -eu -o pipefail

OUT="${1:?usage: dash-summary.sh <output-file> [extra mvn args...]}"
shift || true

cd "$(cd "$(dirname "$0")/.." && pwd)"

# The tool asks ClearlyDefined about every dependency, and that service times
# out often enough to fail a run on its own: three consecutive local attempts
# timed out on one occasion, and it has failed a pull request the same way. A
# gate that goes red for reasons unrelated to the change is a gate people learn
# to ignore, so retry before believing it.
ATTEMPTS=${DASH_ATTEMPTS:-4}
for attempt in $(seq 1 "$ATTEMPTS"); do
  rm -f "$OUT"
  if mvn -B org.eclipse.dash:license-tool-plugin:license-check -Ddash.summary="$OUT" "$@"; then
    break
  fi
  if [ "$attempt" -eq "$ATTEMPTS" ]; then
    echo "ERROR: license-check failed $ATTEMPTS times" >&2
    exit 1
  fi
  echo "license-check attempt $attempt failed, retrying in 30s..." >&2
  sleep 30
done

if [ ! -s "$OUT" ]; then
  echo "ERROR: no summary was produced at $OUT" >&2
  exit 1
fi

echo "$(wc -l < "$OUT" | tr -d ' ') dependencies written to $OUT"
