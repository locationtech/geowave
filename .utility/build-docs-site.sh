#!/bin/bash

if [ "$TRAVIS_BRANCH" == "master" ]
then
    mvn -P docs -pl docs install
fi
