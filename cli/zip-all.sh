#!/bin/bash
set -e

BUCKET=my-bucket
SRC_PREFIX=path/to/folder
DST_PREFIX=path/to/archive
WORKDIR=/tmp/s3-zip
ZIP_NAME=data.zip

rm -rf $WORKDIR
mkdir -p $WORKDIR
cd $WORKDIR

# 1. download
aws s3 sync s3://$BUCKET/$SRC_PREFIX ./data

# 2. zip
zip -r $ZIP_NAME data

# 3. upload
aws s3 cp $ZIP_NAME s3://$BUCKET/$DST_PREFIX/$ZIP_NAME

echo "ZIP uploaded successfully."
