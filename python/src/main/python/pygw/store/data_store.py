#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from pygw.base import GeoWaveObject
from pygw.base import CloseableIterator
from pygw.base import Writer
from pygw.base import DataTypeAdapter
from pygw.config import java_gateway
from pygw.config import geowave_pkg
from pygw.query import Query
from pygw.query import AggregationQuery
from pygw.index import Index
from pygw.query.statistics.statistic_query import StatisticQuery
from pygw.statistics.bin_constraints import BinConstraints
from pygw.statistics.statistic import Statistic
from pygw.statistics.statistic_mappings import map_statistic
from pygw.statistics.statistic_type import StatisticType
from pygw.statistics.statistic_value import StatisticValueTransformer
from pygw.statistics.transformers import BinnedStatisticTransformer


class DataStore(GeoWaveObject):
    """
    This class models the DataStore interface methods.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)

    def ingest(self, url, *indices, ingest_options=None):
        """
        Ingest from URL.

        If this is a directory, this method will recursively search for valid files to
        ingest in the directory. This will iterate through registered IngestFormatPlugins to find one
        that works for a given file.

        Args:
            url (str): The URL for data to read and ingest into this data store.
            *indices (pygw.index.index.Index): Index to ingest into.
            ingest_options: Options for ingest (Not yet supported).
        """
        # TODO: Ingest Options
        if ingest_options:
            raise NotImplementedError()

        assert isinstance(url, str)

        j_index_arr = GeoWaveObject.to_java_array(geowave_pkg.core.store.api.Index, indices)
        java_url = java_gateway.jvm.java.net.URL(url)
        self._java_ref.ingest(java_url, ingest_options, j_index_arr)

    def query(self, q):
        """
        Returns all data in this data store that matches the query parameter. All data that matches the
        query will be returned as an instance of the native data type. The Iterator must be closed when
        it is no longer needed - this wraps the underlying scanner implementation and closes underlying
        resources.

        Args:
            q (pygw.query.query.Query): The query to preform.
        Returns:
            A closeable iterable of results.  The `pygw.base.closeable_iterator.CloseableIterator.close` method should
            be called on the iterator when it is done being used.
        """
        assert isinstance(q, Query)
        j_query = q._java_ref
        return iter(CloseableIterator(self._java_ref.query(j_query), q.java_transformer))

    def aggregate(self, q):
        """
        Perform an aggregation on the data and just return the aggregated result. The query criteria is
        very similar to querying the individual entries except in this case it defines the input to the
        aggregation function, and the aggregation function produces a single result. Examples of this
        might be simply counting matched entries, producing a bounding box or other range/extent for
        matched entries, or producing a histogram.

        Args:
            q (pygw.query.AggregationQuery): The query to preform.
        Returns:
            The single result of the aggregation.
        """
        assert isinstance(q, AggregationQuery)
        j_query = q._java_ref
        return q.java_transformer.transform(self._java_ref.aggregate(j_query))

    def get_types(self):
        """
        Get all the data type adapters that have been used within this data store.

        Returns:
            List of `pygw.base.data_type_adapter.DataTypeAdapter` used in the data store.
        """
        j_adapter_arr = self._java_ref.getTypes()
        return [DataTypeAdapter(j_adpt) for j_adpt in j_adapter_arr]

    def add_empty_statistic(self, *statistic):
        j_stat_array = GeoWaveObject.to_java_array(geowave_pkg.core.store.api.Statistic, statistic)
        self._java_ref.addEmptyStatistic(j_stat_array)

    def add_statistic(self, *statistic):
        j_stat_array = GeoWaveObject.to_java_array(geowave_pkg.core.store.api.Statistic, statistic)
        self._java_ref.addStatistic(j_stat_array)

    def remove_statistic(self, *statistic):
        j_stat_array = GeoWaveObject.to_java_array(geowave_pkg.core.store.api.Statistic, statistic)
        self._java_ref.removeStatistic(j_stat_array)

    def recalc_statistic(self, *statistic):
        j_stat_array = GeoWaveObject.to_java_array(geowave_pkg.core.store.api.Statistic, statistic)
        self._java_ref.recalcStatistic(j_stat_array)

    def get_data_type_statistics(self, type_name):
        return map(map_statistic, self._java_ref.getDataTypeStatistics(type_name))

    def get_data_type_statistic(self, statistic_type, type_name, tag):
        if not isinstance(statistic_type, StatisticType):
            raise AttributeError('Invalid statistic type, should be of class StatisticType')
        return map_statistic(self._java_ref.getDataTypeStatistic(statistic_type.java_ref(), type_name, tag))

    def get_index_statistics(self, index_name):
        return map(map_statistic, self._java_ref.getIndexStatistics(index_name))

    def get_index_statistic(self, statistic_type, index_name, tag):
        if not isinstance(statistic_type, StatisticType):
            raise AttributeError('Invalid statistic type, should be of class StatisticType')
        return map_statistic(self._java_ref.getIndexStatistic(statistic_type.java_ref(), index_name, tag))

    def get_field_statistics(self, type_name, field_name):
        return map(map_statistic, self._java_ref.getFieldStatistics(type_name, field_name))

    def get_field_statistic(self, statistic_type, type_name, field_name, tag):
        if not isinstance(statistic_type, StatisticType):
            raise AttributeError('Invalid statistic type, should be of class StatisticType')
        return map_statistic(self._java_ref.getFieldStatistic(statistic_type.java_ref(), type_name, field_name, tag))

    def get_statistic_value(self, statistic, bin_constraints=None):
        if not isinstance(statistic, Statistic):
            raise AttributeError('Invalid statistic')
        if bin_constraints is None:
            value = self._java_ref.getStatisticValue(statistic.java_ref())
        else:
            if not isinstance(bin_constraints, BinConstraints):
                raise AttributeError('Invalid bin constraints')
            value = self._java_ref.getStatisticValue(statistic.java_ref(), bin_constraints.java_ref())
        return statistic.java_transformer.transform(value)

    def get_binned_statistic_values(self, statistic, bin_constraints=None):
        if not isinstance(statistic, Statistic):
            raise AttributeError('Invalid statistic')
        if bin_constraints is None:
            j_result_iter = self._java_ref.getBinnedStatisticValues(statistic.java_ref())
        else:
            if not isinstance(bin_constraints, BinConstraints):
                raise AttributeError('Invalid bin constraints')
            j_result_iter = self._java_ref.getBinnedStatisticValues(statistic.java_ref(), bin_constraints.java_ref())
        return iter(
            CloseableIterator(
                j_result_iter,
                BinnedStatisticTransformer(statistic.java_transformer)
            ))

    def query_statistics(self, query):
        if not isinstance(query, StatisticQuery):
            raise AttributeError('Invalid statistic query')
        return iter(CloseableIterator(self._java_ref.queryStatistics(query.java_ref()), StatisticValueTransformer()))

    def aggregate_statistics(self, query):
        if not isinstance(query, StatisticQuery):
            raise AttributeError('Invalid statistic query')
        return StatisticValueTransformer().transform(self._java_ref.aggregateStatistics(query.java_ref()))

    def get_indices(self, type_name=None):
        """
        Get the indices that have been registered with this data store for a given type.

        Gets all registered indices if `type_name` is None.

        Args:
            type_name (str): The name of the type.
        Returns:
            List of `pygw.index.index.Index` in the data store.
        """
        if type_name:
            j_indices = self._java_ref.getIndices(type_name)
        else:
            j_indices = self._java_ref.getIndices()
        return [Index(j_index) for j_index in j_indices]

    def copy_to(self, other, q=None):
        """
        Copy data from this data store to another.

        All data is copied if `q` is None, else only the data queried by `q`.

        Args:
            other (pygw.store.data_store.DataStore): The data store to copy to.
            q (pygw.query.query.Query): Query filter for data to be copied.
        """
        assert isinstance(other, DataStore)

        if q:
            assert isinstance(q, Query)
            q = q._java_ref

        self._java_ref.copyTo(other._java_ref, q)

    def add_index(self, type_name, *indices):
        """
        Add new indices for the given type. If there is data in other indices for this type, for
        consistency it will need to copy all of the data into the new indices, which could be a long
        process for lots of data.

        Args:
            type_name (str): Name of data type to register indices to.
            *indices (pygw.index.index.Index): Index to add.
        """
        assert isinstance(type_name, str)

        j_index_arr = GeoWaveObject.to_java_array(geowave_pkg.core.store.api.Index, indices)
        self._java_ref.addIndex(type_name, j_index_arr)

    def remove_index(self, index_name, type_name=None):
        """
        Remove an index for a given data type.

        If `type_name` is None, the specified index is removed for all types.

        Args:
            index_name (str): Name of the index to be removed.
            type_name (str): Name of data type to remove.
        Raises:
            Exception: If the index was the last index of a type.
        """
        if type_name:
            self._java_ref.removeIndex(index_name, type_name)
        else:
            self._java_ref.removeIndex(index_name)

    def remove_type(self, type_name):
        """
        Remove all data and statistics associated with the given type.

        Args:
            type_name (str): Name of the data type.
        """
        assert isinstance(type_name, str)

        self._java_ref.removeType(type_name)

    def delete(self, q):
        """
        Delete all data in this data store that matches the query parameter.

        Args:
            q (pygw.query.query.Query): The query criteria to use for deletion.
        Returns:
            True on success, False on fail.
        """
        assert isinstance(q, Query)

        return self._java_ref.delete(q._java_ref)

    def delete_all(self):
        """
        Delete ALL data and ALL metadata for this datastore.

        Returns:
            True on success, False on fail.
        """

        return self._java_ref.deleteAll()

    def add_type(self, type_adapter, *initial_indices):
        """
        Add this type to the data store. This only needs to be called one time per type.

        Args:
            type_adapter (pygw.base.data_type_adapter.DataTypeAdapter): The data type adapter to add to the data store.
            *initial_indices (pygw.index.index.Index): The initial indices for this type.
        """
        assert isinstance(type_adapter, DataTypeAdapter)

        j_index_arr = GeoWaveObject.to_java_array(geowave_pkg.core.store.api.Index, initial_indices)
        self._java_ref.addType(type_adapter._java_ref, j_index_arr)

    def create_writer(self, type_adapter_name):
        """
        Returns an index writer to perform batched write operations for the given data type name.

        Assumes the type has already been used previously or added using `add_type` and assumes one or
        more indices have been provided for this type.

        Args:
            type_adapter_name (str): The name of the type to write to.
        Returns:
            A `pygw.base.writer.Writer`, which can be used to write entries into the data store of the given type.
        """
        j_writer = self._java_ref.createWriter(type_adapter_name)

        if j_writer is None:
            return None

        return Writer(j_writer)
