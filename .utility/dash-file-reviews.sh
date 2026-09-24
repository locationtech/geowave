#!/usr/bin/env bash
#
# File Eclipse IP review requests for all restricted content.
#
# The Dash tool stops after 100 review requests per run, and requests that
# already exist count toward that, so a single unfiltered run revisits the same
# first 100 every time and never reaches the rest. This splits the restricted
# group IDs in the baseline into batches under that limit. The last batch
# excludes every earlier batch rather than listing its own groups, so restricted
# content that is not in the baseline yet is still filed.
#
# Needs DASH_IPLAB_TOKEN, and "mvn install" first (see dash-summary.sh).
# Each batch's summary and review summary are written to target/dash.
#
# Usage: .utility/dash-file-reviews.sh <baseline-file>
set -eu -o pipefail

BASELINE="${1:?usage: dash-file-reviews.sh <baseline-file>}"
: "${DASH_IPLAB_TOKEN:?DASH_IPLAB_TOKEN must be set}"

if [ ! -f "$BASELINE" ]; then
  echo "ERROR: $BASELINE does not exist" >&2
  exit 1
fi

# GitLabSupport.MAXIMUM_REVIEWS in dash-licenses
LIMIT=100

cd "$(cd "$(dirname "$0")/.." && pwd)"
OUT=target/dash
mkdir -p "$OUT"

# The group ID filter matches by prefix, so a group that an earlier, shorter one
# covers (org.geotools.xsd by org.geotools) has to be counted with it. In C
# collation each prefix sorts directly before the groups it covers.
batches=()
while IFS= read -r batch; do
  batches+=("$batch")
done < <(awk -F', ' '$3 == "restricted" { split($1, c, "/"); print c[3] }' "$BASELINE" \
  | LC_ALL=C sort \
  | awk -v limit="$LIMIT" '
      root == "" || index($0, root) != 1 { root = $0; roots[++n] = root }
      { size[root]++ }
      END {
        for (i = 1; i <= n; i++) {
          r = roots[i]
          if (batch != "" && used + size[r] > limit) { print batch; batch = ""; used = 0 }
          batch = batch == "" ? r : batch "," r
          used += size[r]
        }
        if (batch != "") print batch
      }')

file_batch() {
  local n=$1
  shift
  ./.utility/dash-summary.sh "$OUT/DEPENDENCIES.$n" \
    -Ddash.iplab.token="$DASH_IPLAB_TOKEN" -Ddash.projectId=locationtech.geowave \
    -Ddash.review.summary="$OUT/review-summary.$n" "$@"
  local restricted
  restricted=$(awk -F', ' '$3 == "restricted"' "$OUT/DEPENDENCIES.$n" | wc -l | tr -d ' ')
  if [ "$restricted" -gt "$LIMIT" ]; then
    echo "::warning::Batch $n has $restricted restricted entries but Dash files at most $LIMIT" \
      "per run, so the rest were not filed. Regenerating DEPENDENCIES recomputes the batches."
  fi
}

earlier=""
for ((i = 0; i < ${#batches[@]} - 1; i++)); do
  file_batch "$((i + 1))" -DincludeGroupIds="${batches[i]}"
  earlier="${earlier:+$earlier,}${batches[i]}"
done

if [ -n "$earlier" ]; then
  file_batch "${#batches[@]}" -DexcludeGroupIds="$earlier"
else
  file_batch 1
fi
