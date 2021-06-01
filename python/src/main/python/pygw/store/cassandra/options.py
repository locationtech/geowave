#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from pygw.config import geowave_pkg, java_gateway
from pygw.store import DataStoreOptions
from py4j.java_collections import MapConverter


class CassandraOptions(DataStoreOptions):
    """
    Cassandra data store options.
    """

    def __init__(self):
        super().__init__(geowave_pkg.datastore.cassandra.config.CassandraRequiredOptions())

    def set_contact_points(self, contact_points):
        """
        Sets a single contact point or a comma delimited set of contact points to
        connect to the Cassandra cluster.

        Args:
            contact_point (str): The contact point(s) to connect to.
        """
        self._java_ref.setContactPoints(contact_points)

    def get_contact_points(self):
        """
        Returns:
            The contact points of the Cassandra cluster.
        """
        return self._java_ref.getContactPoints()

    def set_datacenter(self, datacenter):
        """
        Sets the local datacenter.

        Args:
            datacenter (str): The datacenter to connect to.
        """
        self._java_ref.setDatacenter(datacenter)

    def get_datacenter(self):
        """
        Returns:
            The local datacenter of the Cassandra cluster.
        """
        return self._java_ref.getDatacenter()

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

    def set_gc_grace_seconds(self, gc_grace_seconds):
        """
        Sets the gc_grace_seconds for each table created. Defaults to 10 days and major
        compaction should be triggered at least as often.

        Args:
            gc_grace_seconds (int): The gc_grace_seconds to set on the table.
        """
        self._base_options.setGcGraceSeconds(gc_grace_seconds)

    def get_gc_grace_seconds(self):
        """
        Returns:
            The gc_grace_seconds applied to new tables.
        """
        return self._base_options.getGcGraceSeconds()

    def set_table_options(self, table_options):
        """
        Sets additional table options for each new table created.

        Args:
            table_options (dictionary): The table options to apply to each new table.
        """
        self._base_options.setTableOptions(MapConverter().convert(table_options, java_gateway._gateway_client))

    def get_table_options(self):
        """
        Returns:
            The table options that are applied to each new table.
        """
        return self._base_options.getTableOptions()

    def set_compaction_strategy(self, compaction_strategy):
        """
        Set the compaction strategy applied to each new Cassandra table. Available options
        are LeveledCompactionStrategy, SizeTieredCompactionStrategy, or TimeWindowCompactionStrategy.

        Args:
            compaction_strategy (str): The compaction strategy to apply to each new table. Available
            options are LeveledCompactionStrategy, SizeTieredCompactionStrategy, or TimeWindowCompactionStrategy.
        """
        self._base_options.setCompactionStrategyStr(compaction_strategy)

    def get_compaction_strategy(self):
        """
        Returns:
            The compaction strategy applied to each new Cassandra table.
        """
        return self._base_options.getCompactionStrategyStr()