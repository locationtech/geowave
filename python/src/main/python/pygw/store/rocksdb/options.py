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

class RocksDBOptions(DataStoreOptions):
    """
    RocksDB data store options.
    """

    def __init__(self):
        """
        Initializes the RocksDB options class.
        """
        super().__init__(geowave_pkg.datastore.rocksdb.config.RocksDBOptions())

    def set_directory(self, directory):
        """
        Sets the directory of the RocksDB database.

        Args:
            directory (str): The directory for the database.
        """
        self._java_ref.setDirectory(directory)

    def get_directory(self):
        """
        Returns:
            The directory for the RocksDB database.
        """
        return self._java_ref.getDirectory()

    def set_compact_on_write(self, compact_on_write):
        """
        Sets whether or not to perform compaction on write.

        Args:
            compact_on_write (bool): Whether or not to perform compaction on write.
        """
        self._java_ref.setCompactOnWrite(compact_on_write)

    def is_compact_on_write(self):
        """
        Returns:
            True if compaction on write is enabled, False otherwise.
        """
        return self._java_ref.isCompactOnWrite()

    def set_batch_write_size(self, batch_write_size):
        """
        Sets the number of entries to be gathered before performing a batch write
        operation on the data store.

        Args:
            batch_write_size (int): The number of entries to write in batch write operations.
        """
        self._java_ref.setBatchWriteSize(batch_write_size)

    def get_batch_write_size(self):
        """
        Returns:
            The number of entries to write in batch write operations.
        """
        return self._java_ref.getBatchWriteSize()
