#!/bin/bash
#
# RPM build script
#

# Source all our reusable functionality, argument is the location of this script.
. ../../admin-scripts/rpm-functions.sh "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

declare -A ARGS
while [ $# -gt 0 ]; do
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done

# Artifact settings
ARTIFACT_01_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/geowave-deploy/target/geowave-accumulo.jar
ARTIFACT_02_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/geowave-deploy/target/geowave-geoserver.jar
ARTIFACT_03_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/geowave-types/target/geowave-ingest-tool.jar
ARTIFACT_04_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/target/site.tar.gz
ARTIFACT_05_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/geowave-deploy/target/puppet-scripts.tar.gz
ARTIFACT_06_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/docs/target/manpages.tar.gz
ARTIFACT_07_URL=$LOCAL_JENKINS/userContent/geoserver/${ARGS[geoserver]}
RPM_ARCH=noarch

case ${ARGS[command]} in
    build) rpmbuild \
                --define "_topdir $(pwd)" \
                --define "_name ${ARGS[rpmname]}" \
                --define "_version $(parseVersion)" \
                $(buildArg "${ARGS[buildarg]}") SPECS/*.spec ;;
    clean) clean ;;
   update)
        update_artifact $ARTIFACT_01_URL;
        update_artifact $ARTIFACT_02_URL;
        update_artifact $ARTIFACT_03_URL;
        update_artifact $ARTIFACT_04_URL;
        update_artifact $ARTIFACT_05_URL;
        update_artifact $ARTIFACT_06_URL;
        update_artifact $ARTIFACT_07_URL geoserver.zip; ;;
        *) about ;;
esac
