import boto3

"""
Developer: Ahmed Kamel
Company: Digital Globe | Radiant Blue
Contact: ahmed.kamel@digitalglobe.com
Description:
This script is used to access the geowave-rpms s3 container and clean up the dev dir
"""
objs_in_dev = []
objs_in_dev_jar = []

def query_s3_bucket():
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
        if obj.key.startswith('dev/') and 'repodata' not in obj.key:
            objs_in_dev.append(obj)
        if obj.key.startswith('dev-jars/'):
            objs_in_dev_jar.append(obj)

def clean_bucket():
    max_number_of_obj = 50

    #Sort the lists in order from oldest to newest
    ordered_objs_in_dev = sorted(objs_in_dev, key = lambda k: k.last_modified, reverse=True)
    ordered_objs_in_dev_jar = sorted(objs_in_dev_jar, key = lambda k: k.last_modified, reverse=True)
    for obj in objs_in_dev:
        print(obj.key, ,'t', obj.last_modified)
    
    for obj in objs_in_dev_jar:
        print(obj.key, '\t', obj.last_modified)


def run():
    pass

if __name__ == "__main__":
    bucket_name = 'geowave-rpms'
    query_s3_bucket(bucket_name)
    clean_bucket()


