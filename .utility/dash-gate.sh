#!/usr/bin/env bash
#
# Compare a freshly generated Dash summary against the committed baseline and
# fail if anything newly requires Eclipse IP review.
#
# The project has a substantial backlog of restricted content, so gating on
# "any restricted" would fail every build and teach everyone to ignore it. This
# gates on "restricted content that is not already known", which is what keeps
# the backlog from growing while it is worked down.
#
# Usage: .utility/dash-gate.sh <baseline-file> <generated-file>
set -u

BASELINE="${1:?usage: dash-gate.sh <baseline> <generated>}"
GENERATED="${2:?usage: dash-gate.sh <baseline> <generated>}"

if [ ! -f "$GENERATED" ]; then
  echo "ERROR: $GENERATED does not exist"
  exit 1
fi

restricted() { awk -F', ' '$3 == "restricted" { print $1 }' "$1" | sort -u; }

total=$(wc -l < "$GENERATED" | tr -d ' ')
now=$(restricted "$GENERATED" | wc -l | tr -d ' ')

if [ ! -f "$BASELINE" ]; then
  cat <<MSG
No baseline at $BASELINE, so there is nothing to compare against yet.

The generated summary has $total dependencies, $now of which need Eclipse IP
review. Download the DEPENDENCIES artifact from this run, commit it to the
repository root, and this check will start failing on newly introduced
restricted content.
MSG
  exit 0
fi

added=$(comm -13 <(restricted "$BASELINE") <(restricted "$GENERATED"))
removed=$(comm -23 <(restricted "$BASELINE") <(restricted "$GENERATED"))

echo "$total dependencies, $now requiring review (baseline: $(restricted "$BASELINE" | wc -l | tr -d ' '))"

if [ -n "$removed" ]; then
  echo
  echo "Resolved since the baseline:"
  echo "$removed" | sed 's/^/  - /'
  echo "Please regenerate DEPENDENCIES so the baseline keeps shrinking."
fi

if [ -n "$added" ]; then
  echo
  echo "ERROR: new content requires Eclipse IP review:"
  echo "$added" | sed 's/^/  + /'
  echo
  echo "Either avoid the dependency, or have a committer file a review request:"
  echo "  mvn org.eclipse.dash:license-tool-plugin:license-check \\"
  echo "      -Ddash.iplab.token=\$DASH_IPLAB_TOKEN -Ddash.projectId=locationtech.geowave"
  exit 1
fi

echo "No newly restricted content."
