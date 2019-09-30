#
# Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
import subprocess

def get_maven_version():
    version = subprocess.check_output([
        "mvn", "-q", "-Dexec.executable=echo", "-Dexec.args='${project.version}'", "-f", "../../..", "exec:exec"
    ]).strip()
    return version.decode().replace("-", "+").lower()
