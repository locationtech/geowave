#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject
from pygw.base import CloseableIterator
from pygw.base import Writer
from pygw.base import DataTypeAdapter
from pygw.config import java_gateway
from pygw.config import geowave_pkg
from pygw.query import Query
from pygw.index import Index

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
        if ingest_options: raise NotImplementedError()

        assert isinstance(url, str)

        n = len(indices)
        j_index_class = geowave_pkg.core.store.api.Index
        j_index_arr = java_gateway.new_array(j_index_class, n)
        for idx, name in enumerate(indices):
                j_index_arr[idx] = name._java_ref
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
            A closeable iterable of results.  The `pygw.base.closeable_iterator.CloseableIterator.close` method should be called
            on the iterator when it is done being used.
        """
        assert isinstance(q, Query)
        j_query = q._java_ref
        return iter(CloseableIterator(self._java_ref.query(j_query), q.result_transformer))

    def aggregate(self, q):
        raise NotImplementedError

    def get_types(self):
        """
        Get all the data type adapters that have been used within this data store.

        Returns:
            List of `pygw.base.data_type_adapter.DataTypeAdapter` used in the data store.
        """
        j_adapter_arr = self._java_ref.getTypes()
        return [DataTypeAdapter(j_adpt) for j_adpt in j_adapter_arr]

    def query_statistics(self, q):
        raise NotImplementedError

    def aggregate_statistics(self, q):
        raise NotImplementedError

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
        assert isinstance(type_name,str)

        n = len(indices)
        j_index_class = geowave_pkg.core.store.api.Index
        j_index_arr = java_gateway.new_array(j_index_class,n)
        for idx, py_obj in enumerate(indices):
                j_index_arr[idx] = py_obj._java_ref

        self._java_ref.addIndex(type_name,j_index_arr)

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
            self._java_ref.removeIndex(index_name,type_name)
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
        assert isinstance(type_adapter,DataTypeAdapter)

        n = len(initial_indices)
        j_index_class = geowave_pkg.core.store.api.Index
        j_index_arr = java_gateway.new_array(j_index_class,n)
        for idx, py_obj in enumerate(initial_indices):
                j_index_arr[idx] = py_obj._java_ref

        self._java_ref.addType(type_adapter._java_ref,j_index_arr)

    def create_writer(self, type_adapter_name):
        """
        Returns an index writer to perform batched write operations for the given data type name.

        Assumes the type has already been used previously or added using `add_type` and assumes one or
        more indices have been provided for this type.

        Args:
            type_name (str): The name of the type to write to.
        Returns:
            A `pygw.base.writer.Writer`, which can be used to write entries into the data store of the given type.
        """
        j_writer = self._java_ref.createWriter(type_adapter_name)

        if j_writer is None:
            return None

        return Writer(j_writer)
