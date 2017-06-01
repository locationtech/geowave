#!/usr/bin/python
import boto3

"""
Developer: Ahmed Kamel
Company: Digital Globe | Radiant Blue
Contact: ahmed.kamel@digitalglobe.com
Description:
This script is used to access the geowave-rpms s3 container and clean up the dev dir
"""

class CleanS3Bucket():
    def __init__(self, bucket_name = None):
        self.objs_in_dev_noarch = []
        self.objs_in_dev_tarball = []
        self.objs_in_dev_srpms = []
        self.objs_in_dev_jar = []

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
            if obj.key.startswith('dev/noarch') and 'repodata' not in obj.key:
                self.objs_in_dev_noarch.append(obj)
            if obj.key.startswith('dev/TARBALL') and 'repodata' not in obj.key:
                self.objs_in_dev_tarball.append(obj)
            if obj.key.startswith('dev/SRPMS') and 'repodata' not in obj.key:
                self.objs_in_dev_srpms.append(obj)
            if obj.key.startswith('dev-jars/'):
                self.objs_in_dev_jar.append(obj)

    def clean_bucket(self):
        max_number_of_objs = 50

        #Sort the lists in order from oldest to newest
        ordered_objs_in_dev_noarch = sorted(self.objs_in_dev_noarch, key = lambda k: k.last_modified, reverse=True)
        ordered_objs_in_dev_tarball = sorted(self.objs_in_dev_tarball, key = lambda k: k.last_modified, reverse=True)
        ordered_objs_in_dev_srpms = sorted(self.objs_in_dev_srpms, key = lambda k: k.last_modified, reverse=True)
        ordered_objs_in_dev_jar = sorted(self.objs_in_dev_jar, key = lambda k: k.last_modified, reverse=True)
        
        print("Deleting the followings items from the geowave-rpms bucket:")
        if len(ordered_objs_in_dev_noarch) > max_number_of_objs:  
            for obj in ordered_objs_in_dev_noarch[max_number_of_objs:]:
                print(obj.key)
                #obj.delete()
        if len(ordered_objs_in_dev_tarball) > max_number_of_objs:  
            for obj in ordered_objs_in_dev_tarball[max_number_of_objs:]:
                print(obj.key)
                #obj.delete()
        if len(ordered_objs_in_dev_srpms) > max_number_of_objs:  
            for obj in ordered_objs_in_dev_srpms[max_number_of_objs:]:
                print(obj.key)
                #obj.delete
        if len(ordered_objs_in_dev_jar) > max_number_of_objs:  
            for obj in ordered_objs_in_dev_jar[max_number_of_objs:]:
                print(obj.key)
                #obj.delete

if __name__ == "__main__":
    bucket_name = 'geowave-rpms'
    cleaner = CleanS3Bucket(bucket_name)
    cleaner.query_s3_bucket()
    cleaner.clean_bucket()

