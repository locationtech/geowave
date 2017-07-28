#!/bin/bash

if [ "$TRAVIS_REPO_SLUG" == "locationtech/geowave" ] && [ "$BUILD_DOCS" == "true" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then  
  echo -e "Generating changelog...\n"
  export CHANGELOG_GITHUB_TOKEN=$GH_TOKEN
  gem install github_changelog_generator
  github_changelog_generator
  pandoc -f markdown -t html -s -c stylesheets/changelog.css CHANGELOG.md > changelog.html
  cp changelog.html target/site/
  
  # Get the version from the build.properties file
  filePath=deploy/target/classes/build.properties
  GEOWAVE_VERSION=$(grep project.version $filePath|  awk -F= '{print $2}')

  echo -e "Publishing site ...\n"
  cp -R target/site $HOME/site

  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "travis-ci"
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/locationtech/geowave gh-pages > /dev/null

  cd gh-pages 

  # Check if this is not a snapshot version
  if [[ ! "$GEOWAVE_VERSION" =~ "SNAPSHOT" ]] ; then

    # Make a new directory for this release and copy the contents to it
    echo "creating a new versioned documentation directory"
    mkdir previous-versions/$GEOWAVE_VERSION
    cp -Rf $HOME/site/* previous-versions/$GEOWAVE_VERSION/
    rm -f previous-versions/$GEOWAVE_VERSION/*.epub *.pdf *.pdfmarks

  fi

  # Save previous versions of the documentation
  cp -r previous-versions/ $HOME/site/

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
