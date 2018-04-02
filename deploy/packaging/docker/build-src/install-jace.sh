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

if [ ! -f ~/.m2/repository/com/googlecode/jace/jace-maven-plugin/1.3.0/jace-maven-plugin-1.3.0.jar ]
then
    if [ -d jace-maven-plugin ]
    then
        rm -rf jace-maven-plugin
    fi
    mkdir jace-maven-plugin;
    cd jace-maven-plugin;
    git clone https://github.com/jwomeara/jace.git;
    cd jace;
    git checkout tags/v1.3.0;
    mvn -q install -Dsources "$@"
    cd ../..;
fi 
