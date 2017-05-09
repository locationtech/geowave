#!/bin/bash
pushd /mnt/tweets

# Get the first tweet file
firstFile=$(ls *.json | sort -n | head -1)
echo Zipping $firstFile

# Zip it up
gzip $firstFile
zippedFile=$firstFile.gz
echo Zipped $zippedFile

# Do the copy to S3
aws s3 cp $zippedFile s3://geowave-twitter-archive/geo-per-day/$zippedFile

# Move the zipped file to the done folder
mv $zippedFile uploaded