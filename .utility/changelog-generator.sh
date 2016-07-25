#!/bin/bash

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "0.9.2.1_branch" ]
then
  gem install github_changelog_generator
  github_changelog_generator
  pandoc -f markdown -t html -s -c stylesheets/changelog.css CHANGELOG.md > changelog.html
  cp changelog.html target/site/
  echo -e "Published changelog.html\n"
fi
