#!/usr/bin/python
import boto3, re, os

"""
Developer: Ahmed Kamel
Company: Digital Globe | Radiant Blue
Contact: ahmed.kamel@digitalglobe.com
Description:
This script is used to access the geowave-rpms s3 container and clean up the dev dir
"""

class CleanUp():
    def __init__(self, bucket_name = None):
        self.latest_build_time = None
        #lists for cleaning up s3 bucket
        self.max_number_of_objs = 500
        self.objs_in_dev_noarch = []
        self.objs_in_dev_tarball = []
        self.objs_in_dev_srpms = []
        self.objs_in_dev_jar = []
        
        # Variables for cleaning local workspace
        self.dev_path = os.path.join(os.sep, 'var','www','html','repos','snapshots','geowave','dev')
        self.dev_jar_path = os.path.join(os.sep, 'var','www','html','repos','snapshots','geowave','dev-jars')


    def query_s3_bucket(self):
        """
        This function is used to get a list of all the keys with prefix dev and dev-jars
        in the bucket geowave-rpms. 
        """
        resource = 's3'
        bucket_name = 'geowave-rpms'
        filter_prefix = 'dev' 

        s3 = boto3.resource(resource)
        my_bucket = s3.Bucket(bucket_name)
        filtered_bucket = my_bucket.objects.filter(Prefix=filter_prefix)
        for obj in filtered_bucket:
            #There is an extra folder called repodata in 'dev/' so ignore it for now
            if obj.key.startswith('dev/noarch') and 'repo' not in obj.key:
                self.objs_in_dev_noarch.append(obj)
            if obj.key.startswith('dev/TARBALL') and 'repo' not in obj.key:
                self.objs_in_dev_tarball.append(obj)
            if obj.key.startswith('dev/SRPMS') and 'repo' not in obj.key:
                self.objs_in_dev_srpms.append(obj)
            if obj.key.startswith('dev-jars/'):
                self.objs_in_dev_jar.append(obj)

    @staticmethod
    def find_date(fname):
        if isinstance(fname, str):
            reg = re.search('(?P<date>\d{12})',fname)
        else:
            reg = re.search('(?P<date>\d{12})',fname.key)
            #ideally I would use is instance here as well but that does not play nice with boto3 objects
        if reg:
            return int(reg.group('date'))
        else:
            raise NameError("Found a file in bucket with improper name: {}".format(fname))

    def clean_bucket(self):
        """
        This function is used to clean up the s3 bucket by deleting no longer needed artifacts.
        """
        #Call query buckets to know what to delete
        self.query_s3_bucket()
        
        #Sort the list to find the newest build
        ordered_objs_in_dev_noarch = sorted(self.objs_in_dev_noarch, key = self.find_date, reverse=True)
        self.latest_build_time = self.find_date(ordered_objs_in_dev_noarch[0])

        for obj in self.objs_in_dev_noarch:
            if self.latest_build_time > self.find_date(obj):
                print("Deleting from s3://geowave-rpms: {}".format(obj.key))
                obj.delete()
        for obj in self.objs_in_dev_tarball:
            if self.latest_build_time > self.find_date(obj):
                print("Deleting from s3://geowave-rpms: {}".format(obj.key))
                obj.delete()
        for obj in self.objs_in_dev_srpms:
            if self.latest_build_time > self.find_date(obj):
                print("Deleting from s3://geowave-rpms: {}".format(obj.key))
                obj.delete()
        for obj in self.objs_in_dev_jar:
            if self.latest_build_time > self.find_date(obj):
                print("Deleting from s3://geowave-rpms: {}".format(obj.key))
                obj.delete()
    
    def delete_files(self, path):
        """
        Helper function for clean_dirs that does the actual deleting.
        """
        for f in os.listdir(path):
            if f.startswith('geowave-repo'):
                continue

            if f.startswith('geowave'):
                if self.latest_build_time > self.find_date(f):
                    file_path = os.path.join(path,f)
                    print("Deleting file: {}".format(file_path))
                    os.remove(file_path)

    def clean_dirs(self):
        """
        This function is used to clean up the local space of previous build artifacts.
        Requires that the clean_bucket function to have already been ran to set self.latest_build_time.
        This dependency is to maintain a level of syncronization between local workspace and s3 bucket.
        """
        paths = [self.dev_path, self.dev_jar_path]
        for path in paths:
            try:
                subdirs_list = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
                for subdir in subdirs_list: 
                    self.delete_files(os.path.join(path,subdir))
            except OSError:
                #If the file does not exist, its fine move on
                continue


if __name__ == "__main__":
    bucket_name = 'geowave-rpms'
    cleaner = CleanUp(bucket_name)
    cleaner.clean_bucket()
    cleaner.clean_dirs()

