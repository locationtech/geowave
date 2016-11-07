#!/bin/bash

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ] && [ "$BUILD_DOCS" == "true" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then  
  echo -e "Generating changelog...\n"
  gem install github_changelog_generator
  github_changelog_generator
  pandoc -f markdown -t html -s -c stylesheets/changelog.css CHANGELOG.md > changelog.html
  cp changelog.html target/site/
  
  echo -e "Publishing site ...\n"
  cp -R target/site $HOME/site

  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/ngageoint/geowave gh-pages > /dev/null

  cd gh-pages
  git rm -rf .
  cp -Rf $HOME/site/* .

  # Don't check in big binary blobs
  # TODO: Push to S3 if we want to link to them via the web site
  rm -f *.epub *.pdf *.pdfmarks

  git add -f .
  git commit -m "Lastest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
  git push -fq origin gh-pages > /dev/null

  echo -e "Published Javadoc to gh-pages.\n"
  
fi