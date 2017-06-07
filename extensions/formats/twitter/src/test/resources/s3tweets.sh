#!/bin/bash
archivePath=/mnt/tweets/zipped
echo Processing tweet files in $archivePath
pushd $archivePath

# Find any zipped tweet files
for f in tweets-*.gz
do
        echo Processing $f...

        # Do the copy to S3
        echo Copying $f to S3...
        aws s3 cp $f s3://geowave-twitter-archive/geo-12-hr/$f

        # Move the zipped file to the done folder
        echo Moving $f to uploaded folder...
        mv $f ../uploaded
done

echo Done!
