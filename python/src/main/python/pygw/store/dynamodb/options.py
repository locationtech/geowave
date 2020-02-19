#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg
from pygw.config import java_pkg
from pygw.store import DataStoreOptions

class DynamoDBOptions(DataStoreOptions):
    """
    DynamoDB data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.dynamodb.config.DynamoDBOptions())

    def set_region(self, region):
        """
        Sets the AWS region of the DynamoDB data store. For example `us-east-1`
        (specify either endpoint or region not both).

        Args:
            region (str): The AWS region to use.
        """
        if region is None:
            j_region = None
        else:
            j_region = java_pkg.com.amazonaws.regions.Regions.fromName(region)
        self._java_ref.setRegion(j_region)

    def get_region(self):
        """
        Returns:
            The AWS regiion of the DynamoDB data store.
        """
        j_region = self._java_ref.getRegion()
        if j_region is None:
            return j_region
        return j_region.getName()

    def set_endpoint(self, endpoint):
        """
        Sets the endpoint to connect to (specify either endpoint or region not both).

        Args:
            endpoint (str): The endpoint to connect to.
        """
        self._java_ref.setEndpoint(endpoint)

    def get_endpoint(self):
        """
        Returns:
            The endpoint to connect to.
        """
        return self._java_ref.getEndpoint()

    def set_write_capacity(self, write_capacity):
        """
        Sets the write capacity of the DynamoDB data store.

        Args:
            write_capacity (int): The write capacity.
        """
        self._java_ref.setWriteCapacity(write_capacity)

    def get_write_capacity(self):
        """
        Returns:
            The write capacity of the data store.
        """
        return self._java_ref.getWriteCapacity()

    def set_read_capacity(self, read_capacity):
        """
        Sets the read capacity of the DynamoDB data store.

        Args:
            read_capacity (int): The read capacity.
        """
        self._java_ref.setReadCapacity(read_capacity)

    def get_read_capacity(self):
        """
        Returns:
            The read capacity of the data store.
        """
        return self._java_ref.getReadCapacity()

    def set_enable_cache_response_metadata(self, enable_cache_response_metadata):
        """
        Sets whether or not to cache response metadata.

        Args:
            enable_cache_response_metadata (bool): Whether or not to cache response metadata.
        """
        self._java_ref.setEnableCacheResponseMetadata(enable_cache_response_metadata)

    def is_enable_cache_response_metadata(self):
        """
        Returns:
            True if response metadata will be cached, False otherwise.
        """
        return self._java_ref.isEnableCacheResponseMetadata()

    def set_protocol(self, protocol):
        """
        Sets the protocol of the connection to use. Either 'http' or 'https'.

        Args:
            protocol (str): The protocol to use.
        """
        if protocol is None:
            j_protocol = None
        else:
            j_protocol = java_pkg.com.amazonaws.Protocol.valueOf(protocol.upper())
        self._java_ref.setProtocol(j_protocol)

    def get_protocol(self):
        """
        Returns:
            The protocol of the connection to the data store.
        """
        j_protocol = self._java_ref.getProtocol()
        if j_protocol is None:
            return j_protocol
        return j_protocol.name()

    def set_max_connections(self, max_connections):
        """
        Sets the maximum number of connections to the data store.

        Args:
            max_connections (int): The maximum number of connections.
        """
        self._java_ref.setMaxConnections(max_connections)

    def get_max_connections(self):
        """
        Returns:
            The maximum number of connections.
        """
        return self._java_ref.getMaxConnections()
