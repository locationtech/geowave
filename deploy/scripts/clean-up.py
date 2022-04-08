#!/usr/bin/python

# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
#
#  See the NOTICE file distributed with this work for additional
#  information regarding copyright ownership.
#  All rights reserved. This program and the accompanying materials
#  are made available under the terms of the Apache License,
#  Version 2.0 which accompanies this distribution and is available at
#  http://www.apache.org/licenses/LICENSE-2.0.txt
##############################################################################
# This script relies on an EC2 instance with IAM role that enables S3 access.
# It pulls metadata from the underlying OS and does not require locally defined credentials
import re
import os
import sys
import boto3
from collections import OrderedDict
import argparse
from datetime import datetime, timedelta

class CleanUp():
    def __init__(self, workspace_path):
        self.workspace_path = None

        if workspace_path.startswith(os.sep):
            self.workspace_path = workspace_path
        else:
            print("ERROR: Path provided for workspace is invalid. Please ensure it is an absolute path")
            sys.exit(1)

        # Information for builds to keep
        session = boto3.Session()
        creds = session.get_credentials()
        os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key
        os.environ["AWS_SESSION_TOKEN"] = creds.token
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        # Delete everything older than 3 days
        self.date_threshhold = datetime.now() - timedelta(days=3)
        self.rpm_bucket = os.environ['rpm_bucket']

    def find_build_type(self):
        build_type_file = os.path.join(self.workspace_path, 'deploy', 'target', 'build-type.txt')
        build_type = ""
        if os.path.isfile(build_type_file):
            fileptr = open(build_type_file, 'r')
            build_type = fileptr.readline().rstrip()
            fileptr.close()
        else:
            print("WARNING: \"{}\" file not found. Script will not run clean".format(build_type_file)) 
            build_type = None
        return build_type

    def clean_bucket(self):
        s3 = boto3.client('s3')
        resp = s3.list_objects_v2(Bucket="geowave-rpms", Prefix="dev")

        for obj in resp['Contents']:
            key = obj['Key']
            if 'repo' not in key:
                if 'noarch' in key:
                    artifact_date_str = os.path.basename(key).split('.')[3]
                else:
                    artifact_date_str = os.path.basename(key).rsplit('-', 1)[1].split('.')[0]
                    
                try:
                    date_time = datetime.strptime(artifact_date_str, "%Y%m%d%H%M")

                    if date_time < self.date_threshhold and date_time != None:
                        s3.delete_object(Bucket=self.rpm_bucket, Key=key)
                except ValueError as error:
                        print(error)
                        print("Incorrect date format, skipping")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('workspace', type=str,
                        help='The path to the jenkins workspace. Must be absolute path.')
    args = parser.parse_args()

    cleaner = CleanUp(args.workspace)
    build_type = cleaner.find_build_type()
    if build_type == 'dev':
        cleaner.clean_bucket()
    elif build_type == 'release':
        print("Build type detected as release. Not doing clean up.")
