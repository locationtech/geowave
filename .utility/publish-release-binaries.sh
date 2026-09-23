#!/bin/bash

MVN="$(cd "$(dirname "$0")/.." && pwd)/mvnw"
set -ev -o pipefail

# Get the version from the build.properties file
filePath=deploy/target/classes/build.properties
GEOWAVE_VERSION=$(grep project.version "$filePath" | awk -F= '{print $2}')

if [[ "$GEOWAVE_VERSION" == *SNAPSHOT* ]] ; then
  echo -e "Skipping release binaries for snapshot $GEOWAVE_VERSION...\n"
  exit 0
fi

echo -e "Building release binaries...\n"
"$MVN" package -pl deploy -P geowave-tools-singlejar -B -DskipTests -Dspotbugs.skip
"$MVN" package -pl deploy -P geotools-container-singlejar -B -DskipTests -Dspotbugs.skip

pushd deploy/target
ASSETS=()
for type in tools geoserver; do
  jar="geowave-deploy-${GEOWAVE_VERSION}-${type}.jar"
  sha256sum "$jar" > "$jar.sha256"
  ASSETS+=("$jar" "$jar.sha256")
done

TAG="v${GEOWAVE_VERSION}"
echo -e "Attaching release binaries to $TAG...\n"
if gh release view "$TAG" > /dev/null 2>&1; then
  gh release upload "$TAG" "${ASSETS[@]}" --clobber
else
  CREATE_ARGS=(--title "$TAG" --target "$GITHUB_SHA" --generate-notes)
  if [[ "$GEOWAVE_VERSION" == *RC* ]] ; then
    CREATE_ARGS+=(--prerelease)
  fi
  gh release create "$TAG" "${ASSETS[@]}" "${CREATE_ARGS[@]}"
fi
popd
