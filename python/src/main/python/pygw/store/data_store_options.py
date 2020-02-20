#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject

class DataStoreOptions(GeoWaveObject):
    """
    Base options for all data store types.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)
        self._base_options = java_ref.getStoreOptions()

    def set_geowave_namespace(self, namespace):
        """
        Sets the GeoWave namespace for the data store.

        Args:
            namespace (str): The namespace to use.
        """
        self._java_ref.setGeoWaveNamespace(namespace)

    def get_geowave_namespace(self):
        """
        Returns:
            The GeoWave namespace for the data store.
        """
        return self._java_ref.getGeoWaveNamespace()

    def set_persist_data_statistics(self, persist_data_statistics):
        """
        Sets whether or not to persist data statistics.

        Args:
            persist_data_statistics (bool): Whether or not to persist data statistics.
        """
        self._base_options.setPersistDataStatistics(persist_data_statistics)

    def is_persist_data_statistics(self):
        """
        Returns:
            True if data statistics are persisted, False otherwise.
        """
        return self._base_options.isPersistDataStatistics()

    def set_secondary_indexing(self, secondary_indexing):
        """
        Sets whether or not to enable secondary indexing on the data store.

        Args:
            secondary_indexing (bool): Whether or not to enable secondary indexing.
        """
        self._base_options.setSecondaryIndexing(secondary_indexing)

    def is_secondary_indexing(self):
        """
        Returns:
            True if secondary indexing is enabled, False otherwise.
        """
        return self._base_options.isSecondaryIndexing()

    def set_enable_block_cache(self, enable_block_cache):
        """
        Sets whether or not to enable the block cache.

        Args:
            enable_block_cache (bool): Whether or not to enable block cache.
        """
        self._base_options.setEnableBlockCache(enable_block_cache)

    def is_enable_block_cache(self):
        """
        Returns:
            True if block cache is enabled, False otherwise.
        """
        return self._base_options.isEnableBlockCache()

    def set_server_side_library_enabled(self, enable_server_side_library):
        """
        Sets whether or not to enable server side processing when it is available.

        Args:
            enable_server_side_library (bool): Whether or not to enable server side processing.
        """
        self._base_options.setServerSideLibraryEnabled(enable_server_side_library)

    def is_server_side_library_enabled(self):
        """
        Returns:
            True if server side processing is enabled, False otherwise.
        """
        return self._base_options.isServerSideLibraryEnabled()

    def set_max_range_decomposition(self, max_range_decomposition):
        """
        Sets the maximum number of ranges that a query can be decomposed into.

        Args:
            max_range_decomposition (int): The maximum range decomposition.
        """
        self._base_options.setMaxRangeDecomposition(max_range_decomposition)

    def get_max_range_decomposition(self):
        """
        Returns:
            The maximum range decomposition for queries.
        """
        return self._base_options.getMaxRangeDecomposition()

    def set_aggregation_max_range_decomposition(self, max_range_decomposition):
        """
        Sets the maximum number of ranges that an aggregation query can be decomposed into.

        Args:
            max_range_decomposition (int): The maximum range decomposition.
        """
        self._base_options.setAggregationMaxRangeDecomposition(max_range_decomposition)

    def get_aggregation_max_range_decomposition(self):
        """
        Returns:
            The maximum range decomposition for aggregations.
        """
        return self._base_options.getAggregationMaxRangeDecomposition()

    def set_enable_visibility(self, enable_visibility):
        """
        Sets whether or not to enable visibility on the data store.

        Args:
            enable_visibility (bool): Whether or not to enable visibility.
        """
        self._base_options.setEnableVisibility(enable_visibility)

    def is_visibility_enabled(self):
        """
        Returns:
            True if visibility is enabled, False otherwise.
        """
        return self._base_options.isVisibilityEnabled()
