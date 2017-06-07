#!/bin/bash
archivePath=/mnt/tweets
echo Processing tweet files in $archivePath
pushd $archivePath

# Get today's date
today=$(date +%Y%m%d)
echo Today is $today

currentTweetsP1=tweets-${today}p1.json
currentTweetsP2=tweets-${today}p2.json
echo Making sure we omit the current tweet files: $currentTweetsP1, $currentTweetsP2

# Find any old tweet files
for f in tweets-*p*.json
do
        echo Processing $f...
        if [ "$f" != "$currentTweetsP1" ] && [ "$f" != "$currentTweetsP2" ]; then
                # Zip it up
                gzip $f
                zippedFile=$f.gz
                echo Zipped $zippedFile

                # Move the zipped file to the done folder
                echo Moving $zippedFile to zipped folder...
                mv $zippedFile zipped
        else
                echo Omitting current tweet file: $f
        fi
done

echo Done!
