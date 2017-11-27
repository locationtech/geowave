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
# required parameters are --buildtype (dev or release), --version, and --workspace 
DATASTORES=(
	"accumulo"
	"hbase"
)
declare -A ARGS
while [ $# -gt 0 ]; do
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done

if [[ "${ARGS[buildtype]}" = "dev" ]]
then
	#its a dev/latest build
	GEOWAVE_REPO_RPM_TOKEN=geowave-repo-dev-1.0-3.noarch.rpm
	GEOWAVE_VERSION_URL_TOKEN=latest
	GEOWAVE_REPO_NAME_TOKEN=geowave-dev
	GEOWAVE_REPO_BASE_URL_TOKEN=http://s3.amazonaws.com/geowave-rpms/dev/noarch/
else
	#its a release
	GEOWAVE_REPO_RPM_TOKEN=geowave-repo-1.0-3.noarch.rpm
	GEOWAVE_VERSION_URL_TOKEN="${ARGS[version]}"
	GEOWAVE_REPO_NAME_TOKEN=geowave
	GEOWAVE_REPO_BASE_URL_TOKEN=http://s3.amazonaws.com/geowave-rpms/release/noarch/
fi
TARGET_ROOT=${ARGS[workspace]}/deploy/packaging/emr/generated
TEMPLATE_ROOT=${ARGS[workspace]}/deploy/packaging/emr/template
SLD_DIR=${ARGS[workspace]}/examples/data/slds

mkdir -p $TARGET_ROOT/quickstart

# temporarily cp templates to replace common tokens and then cp it to data store locations and rm it here 
cp $TEMPLATE_ROOT/bootstrap-geowave.sh.template $TEMPLATE_ROOT/bootstrap-geowave.sh
cp $TEMPLATE_ROOT/geowave-install-lib.sh.template $TEMPLATE_ROOT/geowave-install-lib.sh
cp $TEMPLATE_ROOT/quickstart/geowave-env.sh.template $TARGET_ROOT/quickstart/geowave-env.sh
cp $TEMPLATE_ROOT/bootstrap-jupyter.sh.template $TEMPLATE_ROOT/bootstrap-jupyter.sh
cp $TEMPLATE_ROOT/create-configure-kernel.sh.template $TEMPLATE_ROOT/create-configure-kernel.sh
cp $TEMPLATE_ROOT/bootstrap-zeppelin.sh.template $TEMPLATE_ROOT/bootstrap-zeppelin.sh
cp $TEMPLATE_ROOT/configure-zeppelin.sh.template $TEMPLATE_ROOT/configure-zeppelin.sh

# copy permanent resources that don't need a template
cp $TEMPLATE_ROOT/quickstart/setup-geoserver-geowave-workspace.sh $TARGET_ROOT/quickstart/setup-geoserver-geowave-workspace.sh
cp $SLD_DIR/*.sld $TARGET_ROOT/quickstart

# replace version token first
sed -i -e s/'$GEOWAVE_VERSION_TOKEN'/${ARGS[version]}/g $TEMPLATE_ROOT/bootstrap-geowave.sh
sed -i -e s/'$GEOWAVE_VERSION_URL_TOKEN'/${GEOWAVE_VERSION_URL_TOKEN}/g $TEMPLATE_ROOT/bootstrap-geowave.sh
sed -i -e s/'$GEOWAVE_REPO_RPM_TOKEN'/${GEOWAVE_REPO_RPM_TOKEN}/g $TEMPLATE_ROOT/bootstrap-geowave.sh

sed -i -e s~'$GEOWAVE_REPO_BASE_URL_TOKEN'~${GEOWAVE_REPO_BASE_URL_TOKEN}~g $TEMPLATE_ROOT/geowave-install-lib.sh
sed -i -e s/'$GEOWAVE_REPO_NAME_TOKEN'/${GEOWAVE_REPO_NAME_TOKEN}/g $TEMPLATE_ROOT/geowave-install-lib.sh

sed -i -e s/'$GEOWAVE_VERSION_TOKEN'/${ARGS[version]}/g $TARGET_ROOT/quickstart/geowave-env.sh

# replacing tokens for jupyter bootstrap scripts
sed -i -e s/'$GEOWAVE_VERSION_TOKEN'/${ARGS[version]}/g $TEMPLATE_ROOT/bootstrap-jupyter.sh
sed -i -e s/'$GEOWAVE_VERSION_URL_TOKEN'/${GEOWAVE_VERSION_URL_TOKEN}/g $TEMPLATE_ROOT/bootstrap-jupyter.sh
sed -i -e s/'$GEOWAVE_VERSION_TOKEN'/${ARGS[version]}/g $TEMPLATE_ROOT/create-configure-kernel.sh

sed -i -e s/'$GEOWAVE_VERSION_TOKEN'/${ARGS[version]}/g $TEMPLATE_ROOT/bootstrap-zeppelin.sh
sed -i -e s/'$GEOWAVE_VERSION_URL_TOKEN'/${GEOWAVE_VERSION_URL_TOKEN}/g $TEMPLATE_ROOT/bootstrap-zeppelin.sh
sed -i -e s/'$GEOWAVE_VERSION_TOKEN'/${ARGS[version]}/g $TEMPLATE_ROOT/configure-zeppelin.sh

for datastore in "${DATASTORES[@]}"
do
	mkdir -p $TARGET_ROOT/quickstart/$datastore
	mkdir -p $TARGET_ROOT/$datastore
	cp $TEMPLATE_ROOT/bootstrap-geowave.sh $TARGET_ROOT/$datastore/bootstrap-geowave.sh
	sed -e '/$DATASTORE_BOOTSTRAP_TOKEN/ {' -e 'r '$TEMPLATE_ROOT/$datastore/DATASTORE_BOOTSTRAP_TOKEN'' -e 'd' -e '}' -i $TARGET_ROOT/$datastore/bootstrap-geowave.sh
	sed -e '/$DATASTORE_CONFIGURE_GEOWAVE_TOKEN/ {' -e 'r '$TEMPLATE_ROOT/$datastore/DATASTORE_CONFIGURE_GEOWAVE_TOKEN'' -e 'd' -e '}' -i $TARGET_ROOT/$datastore/bootstrap-geowave.sh
	sed -i -e s/'$DATASTORE_TOKEN'/$datastore/g $TARGET_ROOT/$datastore/bootstrap-geowave.sh
	
	cp $TARGET_ROOT/$datastore/bootstrap-geowave.sh $TARGET_ROOT/quickstart/$datastore/bootstrap-geowave.sh
	sed -i -e s/'$QUICKSTART_BOOTSTRAP_TOKEN'//g $TARGET_ROOT/$datastore/bootstrap-geowave.sh
	sed -e '/$QUICKSTART_BOOTSTRAP_TOKEN/ {' -e 'r '$TEMPLATE_ROOT/quickstart/QUICKSTART_BOOTSTRAP_TOKEN'' -e 'd' -e '}' -i $TARGET_ROOT/quickstart/$datastore/bootstrap-geowave.sh
	sed -i -e s/'$GEOWAVE_VERSION_URL_TOKEN'/$GEOWAVE_VERSION_URL_TOKEN/g $TARGET_ROOT/quickstart/$datastore/bootstrap-geowave.sh
	sed -i -e s/'$DATASTORE_TOKEN'/$datastore/g $TARGET_ROOT/quickstart/$datastore/bootstrap-geowave.sh
	
	cp $TEMPLATE_ROOT/geowave-install-lib.sh $TARGET_ROOT/$datastore/geowave-install-lib.sh
	sed -i -e s/'$DATASTORE_TOKEN'/${datastore}/g $TARGET_ROOT/$datastore/geowave-install-lib.sh
	sed -e '/$DATASTORE_LIB_TOKEN/ {' -e 'r '$TEMPLATE_ROOT/$datastore/DATASTORE_LIB_TOKEN'' -e 'd' -e '}' -i $TARGET_ROOT/$datastore/geowave-install-lib.sh
	
	cp $TEMPLATE_ROOT/quickstart/ingest-and-kde-gdelt.sh.template $TARGET_ROOT/quickstart/$datastore/ingest-and-kde-gdelt.sh
	sed -e '/$DATASTORE_PARAMS_TOKEN/ {' -e 'r '$TEMPLATE_ROOT/$datastore/DATASTORE_PARAMS_TOKEN'' -e 'd' -e '}' -i $TARGET_ROOT/quickstart/$datastore/ingest-and-kde-gdelt.sh
done

# Copy jupyter additions to separate generated folder
# This will put scripts into separate jupyter folder on s3 when published.
mkdir -p $TARGET_ROOT/jupyter
cp $TEMPLATE_ROOT/bootstrap-jupyter.sh $TARGET_ROOT/jupyter/bootstrap-jupyter.sh
cp $TEMPLATE_ROOT/create-configure-kernel.sh $TARGET_ROOT/jupyter/create-configure-kernel.sh

# Copy zeppelin additions to separate generated folder
# This will put scripts into separate zeppelin folder on s3 when published.
mkdir -p $TARGET_ROOT/zeppelin
cp $TEMPLATE_ROOT/bootstrap-zeppelin.sh $TARGET_ROOT/zeppelin/bootstrap-zeppelin.sh
cp $TEMPLATE_ROOT/configure-zeppelin.sh $TARGET_ROOT/zeppelin/configure-zeppelin.sh

# clean up temporary templates
rm $TEMPLATE_ROOT/bootstrap-geowave.sh
rm $TEMPLATE_ROOT/geowave-install-lib.sh
rm $TEMPLATE_ROOT/bootstrap-jupyter.sh
rm $TEMPLATE_ROOT/create-configure-kernel.sh
rm $TEMPLATE_ROOT/bootstrap-zeppelin.sh
rm $TEMPLATE_ROOT/configure-zeppelin.sh

