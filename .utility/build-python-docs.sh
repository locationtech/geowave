#!/bin/bash

# Build and Run Java Gateway
mvn -q package -P geowave-tools-singlejar -Dspotbugs.skip -DskipTests >/dev/null
GEOWAVE_VERSION=$(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
nohup java -cp deploy/target/geowave-deploy-${GEOWAVE_VERSION}-tools.jar org.locationtech.geowave.core.cli.GeoWaveMain util python rungateway &

# Install pip and venv
sudo apt-get install -yq python3-pip python3-venv

# Build Python docs
cd python/src/main/python
python3 -m venv tests-venv

source ./tests-venv/bin/activate

pip install --upgrade pip

pip install wheel
pip install -r requirements.txt

pdoc --html pygw
EXIT_CODE=$?

cd ../../../..
mv python/src/main/python/html/pygw target/site/pydocs

deactivate

exit $EXIT_CODE
