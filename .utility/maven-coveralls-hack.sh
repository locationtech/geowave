#!/bin/bash

if [ ! -f ~/.m2/repository/org/eluder/coveralls/coveralls-maven-plugin/3.1.1-munge-hack/coveralls-maven-plugin-3.1.1-munge-hack.jar ]
then
    mkdir mcp;
    cd mcp;
    git clone --branch=maven-munge-hack git://github.com/chrisbennight/coveralls-maven-plugin.git;
    cd coveralls-maven-plugin;
    git submodule init;
    git submodule update;
    mvn -q clean install -DskipTests;
    cd ../..;
	rm -rf mcp;
fi