#!/bin/bash

# Build and Run Java Gateway
mvn -q package -P geowave-tools-singlejar -Dfindbugs.skip=true -DskipTests=true -Dspotbugs.skip=true
GEOWAVE_VERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
echo -e "GeoWave version: $GEOWAVE_VERSION\n"
nohup java -cp deploy/target/geowave-deploy-${GEOWAVE_VERSION}-tools.jar org.locationtech.geowave.core.cli.GeoWaveMain util python rungateway &
echo -e "Gateway started...\n"

# Install pip and venv
sudo apt-get install -yq python3-pip python3-venv

# Run Python tests
cd python/src/main/python
python3 -m venv tests-venv

source ./tests-venv/bin/activate

pip install --upgrade pip

pip install wheel
pip install -r requirements.txt

pytest --junitxml=test-report.xml --cov-report= --cov=pygw pygw/test/
EXIT_CODE=$?

deactivate

exit $EXIT_CODE
