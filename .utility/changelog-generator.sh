#!/bin/bash

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ] && [ "$TRAVIS_JDK_VERSION" == "oraclejdk7" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]
then
  gem install github_changelog_generator
  github_changelog_generator
  cp --parents CHANGELOG.md $HOME/site/CHANGELOG.md
  echo -e "Published CHANGELOG.md to $HOME/site/CHANGELOG.md.\n"
fi
