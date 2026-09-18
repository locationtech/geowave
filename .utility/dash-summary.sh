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

rm -f "$OUT"
mvn -B org.eclipse.dash:license-tool-plugin:license-check -Ddash.summary="$OUT" "$@"

if [ ! -s "$OUT" ]; then
  echo "ERROR: no summary was produced at $OUT" >&2
  exit 1
fi

echo "$(wc -l < "$OUT" | tr -d ' ') dependencies written to $OUT"
