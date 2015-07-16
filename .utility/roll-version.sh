#!/bin/bash
#
# Update all the pom.xml files with new version. Ensure you verify with diff before committing
#
# Examples:
#     Release: .utility/roll-version.sh --from 0.8.8-SNAPSHOT --to 0.8.8
#     Dev:     .utility/roll-version.sh --from 0.8.8 --to 0.8.9-SNAPSHOT
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SRC_ROOT="${SCRIPT_DIR}/.."

declare -A ARGS
while [ $# -gt 0 ]; do
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS["$NAME"]="$1" ;;
    esac
    shift
done

usage() {
    echo "$0: --from VERSION_STR --to VERSION_STR"; exit -1
}

# Ensure required arguments are present
if [ -z "${ARGS[from]}" ] || [ -z "${ARGS[to]}" ]; then echo "from and to arguments required"; usage; fi

# Only send pom.xml files to sed and try to match the line exactly to limit stray substitutions
grep -r -l --include pom.xml "${ARGS[from]}" "$SRC_ROOT" | xargs sed -i "s|<version>${ARGS[from]}</version>|<version>${ARGS[to]}</version>|"
