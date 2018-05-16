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
# This will take the template and generate a set of scripts, replacing tokens appropriately
# required parameters are --version and --workspace 

declare -A ARGS
while [ $# -gt 0 ]; do
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done

TARGET_ROOT=${ARGS[workspace]}/deploy/packaging/sandbox/generated
TEMPLATE_ROOT=${ARGS[workspace]}/deploy/packaging/sandbox/template

mkdir -p $TARGET_ROOT/quickstart

# temporarily cp templates to replace common tokens and then cp it to data store locations and rm it here 
cp $TEMPLATE_ROOT/quickstart/geowave-env.sh.template $TARGET_ROOT/quickstart/geowave-env.sh

# replace version token first
sed -i -e s/'$GEOWAVE_VERSION_TOKEN'/${ARGS[version]}/g $TARGET_ROOT/quickstart/geowave-env.sh

