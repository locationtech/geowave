#!/bin/bash

# Build and Run Java Gateway
mvn -q package -P geowave-tools-singlejar -Dfindbugs.skip=true -DskipTests=true >/dev/null
GEOWAVE_VERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
nohup java -cp deploy/target/geowave-deploy-${GEOWAVE_VERSION}-tools.jar org.locationtech.geowave.core.cli.GeoWaveMain python rungateway &

# Install pip and venv
sudo apt-get install -yq python3-pip python3-venv

# Run Python tests
cd python/src/main/python
python3 -m venv tests-venv

source ./tests-venv/bin/activate

pip install -r requirements.txt

pytest
EXIT_CODE=$?

deactivate
rm -rf tests-venv

exit $EXIT_CODE
