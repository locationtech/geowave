#!/bin/bash

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ] && [ "$TRAVIS_JDK_VERSION" == "oraclejdk7" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]
then
  gem install github_changelog_generator
  github_changelog_generator
  pandoc -f markdown -t html CHANGELOG.md > changelog.html
  cp changelog.html target/site/
  echo -e "Published CHANGELOG.md\n"
fi
