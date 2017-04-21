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
SLD_DIR=${ARGS[workspace]}/examples/example-slds

mkdir -p $TARGET_ROOT/quickstart

# temporarily cp templates to replace common tokens and then cp it to data store locations and rm it here 
cp $TEMPLATE_ROOT/bootstrap-geowave.sh.template $TEMPLATE_ROOT/bootstrap-geowave.sh
cp $TEMPLATE_ROOT/geowave-install-lib.sh.template $TEMPLATE_ROOT/geowave-install-lib.sh
cp $TEMPLATE_ROOT/quickstart/geowave-env.sh.template $TARGET_ROOT/quickstart/geowave-env.sh

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

# clean up temporary templates
rm $TEMPLATE_ROOT/bootstrap-geowave.sh
rm $TEMPLATE_ROOT/geowave-install-lib.sh