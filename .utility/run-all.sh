#!/bin/bash
chmod +x .utility/*.sh

.utility/build-dev-resources.sh

.utility/build.sh

.utility/run-tests.sh
.utility/publish.sh