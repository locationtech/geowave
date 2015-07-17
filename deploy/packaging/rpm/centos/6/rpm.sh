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
ARTIFACT_01_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/geowave-accumulo.jar
ARTIFACT_02_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/geowave-geoserver.jar
ARTIFACT_03_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/jace-linux-amd64-debug.tar.gz
ARTIFACT_04_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/jace-linux-amd64-release.tar.gz
ARTIFACT_05_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/jace-source.tar.gz
ARTIFACT_06_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/geowave-tools.jar
ARTIFACT_07_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/plugins.tar.gz
ARTIFACT_08_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/target/site.tar.gz
ARTIFACT_09_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/puppet-scripts.tar.gz
ARTIFACT_10_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/docs/target/manpages.tar.gz
ARTIFACT_11_URL=$LOCAL_JENKINS/job/${ARGS[job]}/lastSuccessfulBuild/artifact/deploy/target/geowave-analytic-mapreduce.jar
ARTIFACT_12_URL=$LOCAL_JENKINS/userContent/geoserver/${ARGS[geoserver]}
RPM_ARCH=noarch

GEOWAVE_VERSION=$(parseVersion)

case ${ARGS[command]} in
    build) rpmbuild \
                --define "_topdir $(pwd)" \
                --define "_version $GEOWAVE_VERSION" \
                --define "_vendor_version ${ARGS[vendor-version]}" \
                --define "_priority $(parsePriorityFromVersion $GEOWAVE_VERSION)" \
                $(buildArg "${ARGS[buildarg]}") SPECS/*.spec ;;
    clean) clean ;;
   update)
        update_artifact $ARTIFACT_01_URL;
        update_artifact $ARTIFACT_02_URL;
        update_artifact $ARTIFACT_03_URL;
        update_artifact $ARTIFACT_04_URL;
        update_artifact $ARTIFACT_05_URL;
        update_artifact $ARTIFACT_06_URL;
        update_artifact $ARTIFACT_07_URL;
        update_artifact $ARTIFACT_08_URL;
        update_artifact $ARTIFACT_09_URL;
        update_artifact $ARTIFACT_10_URL;
        update_artifact $ARTIFACT_11_URL;
        update_artifact $ARTIFACT_12_URL geoserver.zip; ;;
        *) about ;;
esac
