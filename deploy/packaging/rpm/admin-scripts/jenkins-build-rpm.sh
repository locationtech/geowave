#!/bin/bash
#
# For use by rpm building jenkins jobs. Handles job race conditions and
# reindexing the existing rpm repo
#
set -x

echo '###### Build Variables'

declare -A ARGS
while [ $# -gt 0 ]; do
    # Trim the first two chars off of the arg name ex: --foo
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done

echo '###### Clean up workspace'

cd "${WORKSPACE}/${ARGS[buildroot]}"
./rpm.sh --command clean

echo '###### Update artifact(s)'

./rpm.sh \
    --command update \
    --job ${ARGS[job]} \
    --geoserver ${ARGS[geoserver]} \

echo '###### Build rpm'

./rpm.sh \
    --command build \
    --rpmname ${ARGS[rpmname]} \
    --buildarg ba # ba = build all (binary and source rpms)

echo '###### Build tarball distribution archive'

# Copy the SRPM into an extract directory
mkdir -p ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave
cp ${WORKSPACE}/${ARGS[buildroot]}/SRPMS/*.rpm ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave
cd ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave

# Extract all the files
rpm2cpio *.rpm | cpio -idmv

# Remove what we don't want to distribute within the tarball
rm -f *.rpm *.xml *.spec

# Extract the build metadata from one of the artifacts
unzip -p geowave-accumulo.jar build.properties > build.properties

# Extract the pdf version of the docs so it's more visibly available
tar xzf site.tar.gz --strip-components=1  site/documentation.pdf

# Archive things, copy some artifacts up to AWS if available and get rid of our temp area
cd ..
githash=$(cat geowave/build.properties | grep project.scm.revision | sed -e 's/project.scm.revision=//g')
version=$(cat geowave/build.properties | grep project.version | sed -e 's/project.version=//g')
tar cvzf geowave-$version-${githash:0:7}.tar.gz geowave

# Push our compiled docs to S3 if aws command has been installed
if command -v aws >/dev/null 2>&1 ; then
    aws s3 cp geowave/documentation.pdf s3://geowave/docs/
fi

rm -rf geowave

# Copy the Jace artifacts
cp ${WORKSPACE}/${ARGS[buildroot]}/SOURCES/jace-linux-amd64-debug.tar.gz ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave-$version-${githash:0:7}-jace-linux-amd64-debug.tar.gz
cp ${WORKSPACE}/${ARGS[buildroot]}/SOURCES/jace-linux-amd64-release.tar.gz ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave-$version-${githash:0:7}-jace-linux-amd64-release.tar.gz
cp ${WORKSPACE}/${ARGS[buildroot]}/SOURCES/jace-source.tar.gz ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave-$version-${githash:0:7}-jace-source.tar.gz

echo '###### Copy rpm to repo and reindex'

SNAPSHOT_DIR=/var/www/html/repos/snapshots
cp -R ${WORKSPACE}/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/*.rpm ${SNAPSHOT_DIR}/${ARGS[repo]}/${ARGS[buildtype]}/${ARGS[arch]}/
cp -fR ${WORKSPACE}/${ARGS[buildroot]}/SRPMS/*.rpm ${SNAPSHOT_DIR}/${ARGS[repo]}/${ARGS[buildtype]}/SRPMS/
cp -fR ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/*.tar.gz ${SNAPSHOT_DIR}/${ARGS[repo]}/${ARGS[buildtype]}/TARBALL/

# When several processes run createrepo concurrently they will often fail with problems trying to
# access index files that are in the process of being overwritten by the other processes. The command
# below uses two utilities that will cause calls to createrepo (from this script) to wait to gain an
# exclusive file lock before proceeding with a maximum wait time set at 10 minutes before they give
# up and fail. the ha* commands are from the hatools rpm available via EPEL.
/usr/bin/hatimerun -t 10:00 \
/usr/bin/halockrun /var/lock/subsys/rpmrepo \
/usr/bin/createrepo --update --workers 2 ${SNAPSHOT_DIR}/${ARGS[repo]}/${ARGS[buildtype]}/${ARGS[arch]}/
