#!/bin/bash
source geowave-env.sh
${STAGING_DIR}/ingest-and-kde-gdelt.sh
${STAGING_DIR}/setup-geoserver-geowave-workspace.sh