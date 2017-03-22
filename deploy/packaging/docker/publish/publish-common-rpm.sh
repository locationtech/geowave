#!/bin/bash
#
# For use by rpm building jenkins jobs. Handles job race conditions and
# reindexing the existing rpm repo
#

# This script runs with a volume mount to $WORKSPACE, this ensures that any signal failure will leave all of the files $WORKSPACE editable by the host  
trap 'chmod -R 777 $WORKSPACE/deploy/packaging/rpm' EXIT

# Get the version
GEOWAVE_VERSION=$(cat $WORKSPACE/deploy/target/version.txt)
BUILD_TYPE=$(cat $WORKSPACE/deploy/target/build-type.txt)
GEOWAVE_VERSION_URL=$(cat $WORKSPACE/deploy/target/version-url.txt)
echo "---------------------------------------------------------------"
echo "         Publishing GeoWave Common RPMs"
echo "GEOWAVE_VERSION=${GEOWAVE_VERSION}"
echo "GEOWAVE_VERSION_URL=${GEOWAVE_VERSION_URL}"
echo "BUILD_TYPE=${BUILD_TYPE}"
echo "TIME_TAG=${TIME_TAG}"
echo "---------------------------------------------------------------"


echo "###### Build Variables"

set -x
declare -A ARGS
while [ $# -gt 0 ]; do
    # Trim the first two chars off of the arg name ex: --foo
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done

if [[ ${BUILD_TYPE} = "dev" ]]
then
	TIME_TAG_STR="-${TIME_TAG}"
fi
echo '###### Build tarball distribution archive'

# Copy the SRPM into an extract directory
mkdir -p ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave
cp ${WORKSPACE}/${ARGS[buildroot]}/SRPMS/*.rpm ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave
cd ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/geowave

# Extract all the files
rpm2cpio *.rpm | cpio -idmv

# Push our compiled docs and scripts to S3 if aws command has been installed and version url is defined
if command -v aws >/dev/null 2>&1 ; then
	if [[ ! -z "$GEOWAVE_VERSION_URL" ]]; then
		echo '###### Cleaning and copying documentation to S3'
		aws s3 rm --recursive --quiet s3://geowave/${GEOWAVE_VERSION_URL}/docs/
		aws s3 cp --acl public-read --quiet --recursive ${WORKSPACE}/target/site/ s3://geowave/${GEOWAVE_VERSION_URL}/docs/
		echo '###### Cleaning and copying scripts to S3'
		${WORKSPACE}/deploy/packaging/emr/generate-emr-scripts.sh --buildtype ${BUILD_TYPE} --version ${GEOWAVE_VERSION} --workspace ${WORKSPACE}
		aws s3 rm --recursive --quiet s3://geowave/${GEOWAVE_VERSION_URL}/scripts/
		aws s3 cp --acl public-read --quiet --recursive ${WORKSPACE}/deploy/packaging/emr/generated/ s3://geowave/${GEOWAVE_VERSION_URL}/scripts/emr/
	else
		echo '###### Skipping publish to S3: GEOWAVE_VERSION_URL not defined'
	fi
else
	echo '###### Skipping publish to S3: AWS command not found'
fi

# Archive things, copy some artifacts up to AWS if available and get rid of our temp area
cd ..

tar cvzf geowave-${GEOWAVE_VERSION}${TIME_TAG_STR}.tar.gz geowave

rm -rf geowave

echo '###### Copy rpm to repo and reindex'

mkdir -p ${LOCAL_REPO_DIR}/${ARGS[repo]}/${BUILD_TYPE}/{SRPMS,TARBALL,${ARGS[arch]}}/
cp -R ${WORKSPACE}/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/*.rpm ${LOCAL_REPO_DIR}/${ARGS[repo]}/${BUILD_TYPE}/${ARGS[arch]}/
cp -fR ${WORKSPACE}/${ARGS[buildroot]}/SRPMS/*.rpm ${LOCAL_REPO_DIR}/${ARGS[repo]}/${BUILD_TYPE}/SRPMS/
cp -fR ${WORKSPACE}/${ARGS[buildroot]}/TARBALL/*.tar.gz ${LOCAL_REPO_DIR}/${ARGS[repo]}/${BUILD_TYPE}/TARBALL/

# When several processes run createrepo concurrently they will often fail with problems trying to
# access index files that are in the process of being overwritten by the other processes. The command
# below uses two utilities that will cause calls to createrepo (from this script) to wait to gain an
# exclusive file lock before proceeding with a maximum wait time set at 10 minutes before they give
# up and fail. the ha* commands are from the hatools rpm available via EPEL.
hatimerun -t 10:00 \
halockrun -c ${LOCK_DIR}/rpmrepo \
createrepo --update --workers 2 ${LOCAL_REPO_DIR}/${ARGS[repo]}/${BUILD_TYPE}/${ARGS[arch]}/
