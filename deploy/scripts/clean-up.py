#!/usr/bin/python
import re, os, sys
from collections import OrderedDict
import argparse

"""
Developer: Ahmed Kamel
Company: Digital Globe | Radiant Blue
Contact: ahmed.kamel@digitalglobe.com
Description:
This script is used to access the geowave-rpms s3 container and clean up the dev dir
"""

class CleanUp():
    def __init__(self, workspace_path):
        self.workspace_path = None
        if workspace_path.startswith(os.sep):
            self.workspace_path = workspace_path
        else:
            print("ERROR: Path provided for workspace is invalid. Please ensure it is an absolute path")
            sys.exit(1)

        # Information for builds to keep
        self.remove_builds_before = None
        self.num_of_builds_to_keep = 3
        
        # Variables for cleaning local workspace
        self.dev_path = os.path.join(os.sep, 'var','www','geowave-efs','html','repos','snapshots','geowave', 'dev')
        self.dev_jar_path = os.path.join(os.sep, 'var','www','geowave-efs','html','repos','snapshots','geowave', 'dev-jars')

    def find_build_type(self):
        build_type_file = os.path.join(self.workspace_path, 'deploy', 'target', 'build-type.txt')
        build_type = ""
        if os.path.isfile(build_type_file):
            fileptr = open(build_type_file, 'r')
            build_type = fileptr.readline().rstrip()
            fileptr.close()
        else:
            print("WARNING: \"{}\" file now found. Script will not clean clean".format(build_type_file)) 
            build_type = None
        return build_type
        

    @staticmethod
    def find_date(fname):
        if isinstance(fname, str):
            reg = re.search('(?P<date>\d{12})',fname)
        #Only return a number if date was found
        if reg:
            return int(reg.group('date'))
        else:
            return None

    def gen_list_of_dates(self, ordered_objs):
        list_of_dates = []
        for item in ordered_objs:
            date = self.find_date(item)
            if date:
                list_of_dates.append(date)
        return list(OrderedDict.fromkeys(list_of_dates))
    
    def delete_files(self, path):
        """
        Helper function for clean_dirs that does the actual deleting.
        """
        for f in os.listdir(path):
            if f.startswith('geowave-repo'):
                continue

            if f.startswith('geowave'):
                if self.remove_builds_before > self.find_date(f):
                    file_path = os.path.join(path,f)
                    print("Deleting file: {}".format(file_path))
                    os.remove(file_path)

    def clean_dirs(self):
        """
        This function is used to clean up the local space of previous build artifacts.
        """
        #Set the remove_builds_before variable to know which builds to delete
        noarch_files_list = os.listdir(os.path.join(self.dev_path, 'noarch'))
        list_of_dates = self.gen_list_of_dates(noarch_files_list)
        list_of_dates = sorted(list_of_dates, reverse=True)
        self.remove_builds_before = list_of_dates[self.num_of_builds_to_keep - 1]

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
    parser = argparse.ArgumentParser()
    parser.add_argument('workspace', type=str, help='The path to the jenkins workspace. Must be absolute path.')
    args = parser.parse_args()

    cleaner = CleanUp(args.workspace)
    build_type = cleaner.find_build_type()
    if build_type == 'dev':
        #cleaner.clean_bucket()
        cleaner.clean_dirs()
    elif build_type == 'release':
        print("Build type detected as release. Not doing clean up.") 
    

