#!/bin/bash
set -ev
if [ "$PYTHON_BUILD" == "true" ]; then
	cd python/src/main/python
	source ./tests-venv/bin/activate
	pip install python-coveralls
	coveralls
	deactivate
else
    cd test
    mvn coveralls:report -P ${MAVEN_PROFILES}
fi