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
# The reusable functionality needed to update, build and deploy RPMs. 
# Should be sourced by individual projects which then only need to override 
# any unique behavior
#

# Absolute path to the directory containing admin scripts
ADMIN_SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# When sourcing this script the directory of the calling script is passed
CALLING_SCRIPT_DIR=$1

about() {
	echo "Usage: $0 --command [clean|update|build]"
	echo "	clean  - Removes build files and RPMs"
	echo "	update - Pulls down new artifact from Jenkins"
	echo "	build [-ba|-bb|-bp|-bc|-bi|-bl|-bs] - Builds artifacts, default is -ba (build all)"
}

# Check for valid RPM build lifecycle argument or use default
buildArg() {
    # ba : Build binary and source packages (after doing the %prep, %build, and %install stages)
    # bb : Build a binary package (after doing the %prep, %build, and %install stages)
    # bp : Build a binary package (after doing the %prep, %build, and %install stages)
    # bc : Do the "%build" stage from the spec file (after doing the %prep stage)
    # bi : Do the "%install" stage from the spec file (after doing the %prep and %build stages)
    # bl : Do a "list check". The "%files" section from the spec file is macro expanded, and checks are made to verify that each file exists
    # bs : Build just the source package
    VALID_ARGS=('ba' 'bb' 'bp' 'bc' 'bi' 'bl' 'bs')
    DEFAULT_ARG='ba'
    BUILD_ARG="$1"

    # No arg uses default  
    if [ -z "$BUILD_ARG" ]; then
        echo "-$DEFAULT_ARG"
        exit
    fi

    # A bad arg uses default (as long as our default is build all the worst case is it will do more than you asked)
    match=0
    for arg in "${VALID_ARGS[@]}"
    do
        if [ "$BUILD_ARG" = $arg ]; then
            match=1
            break
        fi
    done
    if [ $match -eq 0 ]; then
        echo "-$DEFAULT_ARG"
        exit
    fi 
    
    # Pass along valid build arg
    echo "-$BUILD_ARG"
}

# Given a version string, remove all dots and patch version dash labels, then take the first three sets of digits
# and interpret as an integer to determine the install priority number used by alternatives in an automated way
parsePriorityFromVersion() {
    # Drop trailing bug fix or pre-release labels (0.8.8-alpha2 or 0.8.8-1)
    VERSION=${1%-*}

    # Truncate the version string after the first three groups delimited by dots
    VERSION=$(echo $VERSION | cut -d '.' -f1-3)

    # Remove non digits (dots)
    VERSION=$(echo ${VERSION//[^0-9]/})

    # If empty or not a number is the result return a low priority
    if [ -z "$VERSION" ] || [ "$VERSION" -ne "$VERSION" ] ; then
        echo 1
    else
        # Interpret as a base 10 number (drop leading zeros)
        echo $(( 10#$VERSION ))
    fi
}

# Removes all files except spec and sources
clean() {
    rm -rf $CALLING_SCRIPT_DIR/BUILD/*
    rm -rf $CALLING_SCRIPT_DIR/BUILDROOT/*
    rm -rf $CALLING_SCRIPT_DIR/RPMS/*
    rm -rf $CALLING_SCRIPT_DIR/SRPMS/*
    rm -rf $CALLING_SCRIPT_DIR/TARBALL/*
}

# Just grabbed off the Interwebs, looks to give sane results in the 
# couple of tests I've written. Add more and tweak if found to be defective
isValidUrl() {
	VALID_URL_REGEX='(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[-A-Za-z0-9\+&@#/%=~_|]'
	[[ $1 =~ $VALID_URL_REGEX ]] && return 0 || return 1
}

if [ ! -d "$CALLING_SCRIPT_DIR" ]; then
	echo >&2 "Usage: . $0 [calling script directory]"
	exit 1
fi
