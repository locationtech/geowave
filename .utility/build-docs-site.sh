#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ] && $BUILD_DOCS
then
    mvn -P docs -pl docs install
fi
