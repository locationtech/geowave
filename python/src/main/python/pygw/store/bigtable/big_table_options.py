#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg
from pygw.store import DataStoreOptions

class BigTableOptions(DataStoreOptions):
    """
    BigTable data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.bigtable.config.BigTableOptions())

    def set_scan_cache_size(self, scan_cache_size):
        """
        Sets the scan cache size of the BigTable instance.

        Args:
            scan_cache_size (int): The scan cache size to use.
        """
        self._java_ref.setScanCacheSize(scan_cache_size)

    def get_scan_cache_size(self):
        """
        Returns:
            The scan cache size of the BigTable instance.
        """
        return self._java_ref.getScanCacheSize()

    def set_project_id(self, project_id):
        """
        Sets the project id of the BigTable data store.

        Args:
            project_id (str): The project ID to use.
        """
        self._java_ref.setProjectId(project_id)

    def get_project_id(self):
        """
        Returns:
            The project ID of the data store.
        """
        return self._java_ref.getProjectId()

    def set_instance_id(self, instance_id):
        """
        Sets the instance id of the BigTable data store.

        Args:
            instance_id (str): The instance ID to use.
        """
        self._java_ref.setInstanceId(instance_id)

    def get_instance_id(self):
        """
        Returns:
            The instance ID of the data store.
        """
        return self._java_ref.getInstanceId()
