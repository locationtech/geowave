#
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
import os
import subprocess
import re

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))


def get_maven_version():
    # The repository's wrapper rather than whatever mvn is installed, whose version varies
    version = subprocess.check_output([
        os.path.join(_REPO_ROOT, "mvnw"), "help:evaluate", "-Dexpression=project.version", "-q",
        "-DforceStdout", "-f", _REPO_ROOT
    ]).strip().decode().replace("-", ".")
    if "SNAPSHOT" in version:
        git_description = subprocess.check_output(["git", "describe", "--always"]).strip().decode()
        count_search = re.search("-(.*)-", git_description)
        if count_search is not None:
            dev_count = count_search.group(1)
        else:
            dev_count = "0"
        version = version.replace("SNAPSHOT", "dev" + dev_count)
    return version.lower()
