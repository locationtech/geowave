#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ]
then
    # Build web site, documentation and java docs
    mvn -P docs install
fi
