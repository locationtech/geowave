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

class RedisOptions(DataStoreOptions):
    """
    Redis data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.redis.config.RedisOptions())

    def set_address(self, address):
        """
        Sets the address of the Redis data store.

        Args:
            address (str): The address of the Redis data store.
        """
        self._java_ref.setAddress(address)

    def get_address(self):
        """
        Returns:
            The address of the Redis data store.
        """
        return self._java_ref.getAddress()

    def set_compression(self, compression):
        """
        Sets the compression to use for the Redis data store. Valid options are `SNAPPY`,
        `L4Z`, or `NONE`.

        Args:
            compression (str): The compressioin to use.
        """
        converter = geowave_pkg.datastore.redis.config.RedisOptions.CompressionConverter()
        self._java_ref.setCompression(converter.convert(compression))

    def get_compression(self):
        """
        Returns:
            The compression used by the data store.
        """
        return self._java_ref.getCompression().name()
