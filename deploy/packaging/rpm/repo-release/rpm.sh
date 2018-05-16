#-------------------------------------------------------------------------------
# Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
# 
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License,
# Version 2.0 which accompanies this distribution and is available at
# http://www.apache.org/licenses/LICENSE-2.0.txt
#-------------------------------------------------------------------------------
#!/bin/bash
#
# RPM build script
#

# Source all our reusable functionality, argument is the location of this script.
. ../rpm-functions.sh "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

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
        *) about ;;
esac
