#!/bin/bash
archivePath=/mnt/tweets
echo Processing tweet files in $archivePath
pushd $archivePath

# Get today's date
today=$(date +%Y%m%d)
echo Today is $today

currentTweets=tweets-$today.json
echo Making sure we omit the current tweet file: $currentTweets

# Find any old tweet files
for f in tweets-*.json
do
	echo Processing $f...
	if [ "$f" != "$currentTweets" ]; then
		# Zip it up
		gzip $f
		zippedFile=$f.gz
		echo Zipped $zippedFile

		# Do the copy to S3
		echo Copying $zippedFile to S3...
		aws s3 cp $zippedFile s3://geowave-twitter-archive/geo-per-day/$zippedFile

		# Move the zipped file to the done folder
		echo Moving $zippedFile to uploaded folder...
		mv $zippedFile uploaded
	else
		echo Omitting current tweet file: $f
	fi
done

echo Done!