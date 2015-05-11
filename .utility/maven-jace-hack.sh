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
    mvn -q clean install -Dsources;
    cd ../..;
fi