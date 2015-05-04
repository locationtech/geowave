#!/bin/bash
#
# RPM build script
#

# Source all our reusable functionality, argument is the location of this script.
. ../admin-scripts/rpm-functions.sh "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

declare -A ARGS
while [ $# -gt 0 ]; do
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done

# Artifact settings
RPM_ARCH=noarch

case ${ARGS[command]} in
    build) rpmbuild \
            --define "_topdir $(pwd)" \
            $(buildArg "${ARGS[buildarg]}") SPECS/*.spec ;;
    clean) clean ;;
   update) echo "Nothing to update" ;;
        *) about ;;
esac
