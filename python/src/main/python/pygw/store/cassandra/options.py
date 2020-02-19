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

class CassandraOptions(DataStoreOptions):
    """
    Cassandra data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.cassandra.config.CassandraRequiredOptions())

    def set_contact_point(self, contact_point):
        """
        Sets a single contact point or a comma delimited set of contact points to
        connect to the Cassandra cluster.

        Args:
            contact_point (str): The contact point(s) to connect to.
        """
        self._java_ref.setContactPoint(contact_point)

    def get_contact_point(self):
        """
        Returns:
            The contact points of the Cassandra cluster.
        """
        return self._java_ref.getContactPoint()

    def set_batch_write_size(self, batch_write_size):
        """
        Sets the number of entries to be gathered before performing a batch write
        operation on the data store.

        Args:
            batch_write_size (int): The number of entries to write in batch write operations.
        """
        self._base_options.setBatchWriteSize(batch_write_size)

    def get_batch_write_size(self):
        """
        Returns:
            The number of entries to write in batch write operations.
        """
        return self._base_options.getBatchWriteSize()

    def set_durable_writes(self, durable_writes):
        """
        Sets whether or not to write to commit log for durability, configured only
        on creation of new keyspace.

        Args:
            durable_writes (bool): Whether or not to enable durable writes.
        """
        self._base_options.setDurableWrites(durable_writes)

    def is_durable_writes(self):
        """
        Returns:
            True if durable writes are enabled, False otherwise.
        """
        return self._base_options.isDurableWrites()

    def set_replication_factor(self, replication_factor):
        """
        Sets the number of replicas to use when creating a new keyspace.

        Args:
            replication_factor (int): The number of replicas.
        """
        self._base_options.setReplicationFactor(replication_factor)

    def get_replication_factor(self):
        """
        Returns:
            The number of replicas to use when creating a new keyspace.
        """
        return self._base_options.getReplicationFactor()
