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

class HBaseOptions(DataStoreOptions):
    """
    HBase data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.hbase.config.HBaseRequiredOptions())

    def set_zookeeper(self, zookeeper):
        """
        Sets the list of Zookeper servers that the HBase instance uses as a comma-separated
        string.

        Args:
            zookeeper (str): A comma-separated list of Zookeeper servers.
        """
        self._java_ref.setZookeeper(zookeeper)

    def get_zookeeper(self):
        """
        Returns:
            A comma-separated list of Zookeper servers.
        """
        return self._java_ref.getZookeeper()

    def set_scan_cache_size(self, scan_cache_size):
        """
        Sets the scan cache size of the HBase instance.

        Args:
            scan_cache_size (int): The scan cache size to use.
        """
        self._base_options.setScanCacheSize(scan_cache_size)

    def get_scan_cache_size(self):
        """
        Returns:
            The scan cache size of the HBase instance.
        """
        return self._base_options.getScanCacheSize()

    def set_verify_coprocessors(self, verify_coprocessors):
        """
        Sets whether or not to verify coprocessors when performing operations.

        Args:
            verify_coprocessors (bool): Whether or not to verify coprocessors.
        """
        self._base_options.setVerifyCoprocessors(verify_coprocessors)

    def is_verify_coprocessors(self):
        """
        Returns:
            True if coprocessors will be verified, False otherwise.
        """
        return self._base_options.isVerifyCoprocessors()

    def set_coprocessor_jar(self, coprocessor_jar):
        """
        Sets the path (HDFS URL) to the jar containing coprocessor classes.

        Args:
            coprocessor_jar (str): The path to the coprocessor jar.
        """
        self._base_options.setCoprocessorJar(coprocessor_jar)

    def get_coprocessor_jar(self):
        """
        Returns:
            The HDFS URL of the coprocessor jar.
        """
        return self._base_options.getCoprocessorJar()
