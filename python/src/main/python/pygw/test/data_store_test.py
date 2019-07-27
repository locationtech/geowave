#
# Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

import pytest
import os

from pygw.store import DataStoreFactory
from pygw.store.rocksdb import RocksDBOptions
from pygw.index import SpatialIndexBuilder
from pygw.query import VectorQueryBuilder

from .conftest import TEST_DIR
from .conftest import POINT_TYPE_ADAPTER
from .conftest import TEST_DATA
from .conftest import write_test_data
from .conftest import results_as_list

# Test Additions #
def test_add_type(test_ds):
    # given
    index = SpatialIndexBuilder().create_index()
    adapter = POINT_TYPE_ADAPTER

    # when
    test_ds.add_type(adapter, index)
    indices = test_ds.get_indices()
    types = test_ds.get_types()

    # then
    assert len(indices) == 1
    assert indices[0].get_name() == index.get_name()
    assert indices[0].get_index_strategy() == index.get_index_strategy()
    assert indices[0].get_index_model() == index.get_index_model()
    assert len(types) == 1
    assert types[0].get_type_name() == adapter.get_type_name()


def test_add_existing_type(test_ds):
    # given
    index = SpatialIndexBuilder().create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    test_ds.add_type(adapter, index)
    indices = test_ds.get_indices(adapter.get_type_name())

    # then
    assert len(indices) == 1
    assert indices[0].get_name() == index.get_name()
    assert indices[0].get_index_strategy() == index.get_index_strategy()
    assert indices[0].get_index_model() == index.get_index_model()


# Test Removing #
def test_remove_index(test_ds):
    # given
    index = SpatialIndexBuilder().set_name_override("idx1").create_index()
    index2 = SpatialIndexBuilder().set_name_override("idx2").create_index()

    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    test_ds.add_type(adapter, index2)

    # when
    test_ds.remove_index(adapter.get_type_name(), index.get_name())
    indices = test_ds.get_indices()

    # then
    assert len(indices) == 1
    assert indices[0].get_name() == index2.get_name()
    assert indices[0].get_index_strategy() == index2.get_index_strategy()
    assert indices[0].get_index_model() == index2.get_index_model()


def test_remove_index_last(test_ds):
    with pytest.raises(Exception) as exec:
        # given
        index = SpatialIndexBuilder().create_index()
        adapter = POINT_TYPE_ADAPTER
        test_ds.add_type(adapter, index)

        # when
        test_ds.remove_index(index.get_name())

    # then
    assert 'Adapters require at least one index' in str(exec.value)


def test_remove_index_non_exist(test_ds):
    # given
    index = SpatialIndexBuilder().create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    test_ds.remove_index("Corgi")

    # then
    assert len(test_ds.get_indices()) == 1


def test_remove_type(test_ds):
    # given
    index = SpatialIndexBuilder().create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    write_test_data(test_ds, index)

    # when
    test_ds.remove_type(adapter.get_type_name())
    query = VectorQueryBuilder().build()
    res = results_as_list(test_ds.query(query))

    # then
    assert len(test_ds.get_indices(adapter.get_type_name())) == 0
    assert len(test_ds.get_indices()) == 1
    assert len(res) == 0


# Test Deleting #
def test_delete(test_ds):
    # given
    index = SpatialIndexBuilder().set_name_override("idx1").create_index()
    index2 = SpatialIndexBuilder().set_name_override("idx2").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    test_ds.add_type(adapter, index2)
    write_test_data(test_ds, index, index2)

    # when
    qbldr = VectorQueryBuilder()
    constraints_factory = qbldr.constraints_factory()
    # filter encompasses 10 features (1, 1) - (10, 10)
    constraints = constraints_factory.cql_constraints("BBOX(the_geom, 0.5, 0.5, 10.5, 10.5)")
    qbldr.constraints(constraints)
    test_ds.delete(qbldr.build())

    query = VectorQueryBuilder().build()
    res = results_as_list(test_ds.query(query))

    # then
    assert len(test_ds.get_indices()) == 2
    assert len(test_ds.get_types()) == 1
    assert len(res) == (len(TEST_DATA) - 10)

# Test Delete All #
def test_delete_all(test_ds):
    # given
    index = SpatialIndexBuilder().set_name_override("idx1").create_index()
    index2 = SpatialIndexBuilder().set_name_override("idx2").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    test_ds.add_type(adapter, index2)
    write_test_data(test_ds, index, index2)

    # when
    test_ds.delete_all()

    query = VectorQueryBuilder().build()
    res = results_as_list(test_ds.query(query))

    # then
    assert len(test_ds.get_indices()) == 0
    assert len(test_ds.get_types()) == 0
    assert len(res) == 0


# Test Copy #
def test_copy(test_ds):
    # given
    options = RocksDBOptions()
    options.set_geowave_namespace("geowave.tests")
    options.set_directory(os.path.join(TEST_DIR, "datastore2"))
    ds2 = DataStoreFactory.create_data_store(options)

    adapter = POINT_TYPE_ADAPTER
    index = SpatialIndexBuilder().create_index()
    test_ds.add_type(adapter, index)

    write_test_data(test_ds, index)

    # when
    test_ds.copy_to(ds2)

    indices = ds2.get_indices()
    types = ds2.get_types()
    query = VectorQueryBuilder().build()
    res = results_as_list(ds2.query(query))

    # then
    assert len(test_ds.get_indices()) == 1
    assert len(indices) == 1
    assert indices[0].get_name() == index.get_name()
    assert indices[0].get_index_strategy() == index.get_index_strategy()
    assert indices[0].get_index_model() == index.get_index_model()
    assert len(types) == 1
    assert types[0].get_type_name() == adapter.get_type_name()
    assert len(res) == len(TEST_DATA)

    ds2.delete_all()

def test_copy_by_query(test_ds):
    # given
    options = RocksDBOptions()
    options.set_geowave_namespace("geowave.tests")
    options.set_directory(os.path.join(TEST_DIR, "datastore2"))
    ds2 = DataStoreFactory.create_data_store(options)

    adapter = POINT_TYPE_ADAPTER
    index = SpatialIndexBuilder().create_index()
    test_ds.add_type(adapter, index)

    write_test_data(test_ds, index)

    # when
    qbldr = VectorQueryBuilder()
    constraints_factory = qbldr.constraints_factory()
    # filter encompasses 10 features (1, 1) - (10, 10)
    constraints = constraints_factory.cql_constraints("BBOX(the_geom, 0.5, 0.5, 10.5, 10.5)")
    qbldr.all_indices().constraints(constraints)
    test_ds.copy_to(ds2, qbldr.build())

    indices = ds2.get_indices()
    types = ds2.get_types()
    query = VectorQueryBuilder().build()
    res = results_as_list(ds2.query(query))

    # then
    assert len(test_ds.get_indices()) == 1
    assert len(indices) == 1
    assert indices[0].get_name() == index.get_name()
    assert indices[0].get_index_strategy() == index.get_index_strategy()
    assert indices[0].get_index_model() == index.get_index_model()
    assert len(types) == 1
    assert types[0].get_type_name() == adapter.get_type_name()
    assert len(res) == 10

    ds2.delete_all()

# Test Writer #
def test_create_writer(test_ds):
    # given
    adapter = POINT_TYPE_ADAPTER
    index = SpatialIndexBuilder().create_index()
    test_ds.add_type(adapter, index)

    # when
    writer = test_ds.create_writer(adapter.get_type_name())

    # then
    assert writer is not None


def test_create_writer_null(test_ds):
    # when
    writer = test_ds.create_writer("Corgi")

    # then
    assert writer is None


def test_create_writer_null_other(test_ds):
    # given
    adapter = POINT_TYPE_ADAPTER
    index = SpatialIndexBuilder().create_index()
    test_ds.add_type(adapter, index)
    test_ds.create_writer(adapter.get_type_name())

    # when
    writer = test_ds.create_writer("Corgi")

    # then
    assert writer is None


def test_write(test_ds):
    # given
    adapter = POINT_TYPE_ADAPTER
    index = SpatialIndexBuilder().create_index()
    test_ds.add_type(adapter, index)

    # when
    write_test_data(test_ds, index)

    query = VectorQueryBuilder().build()

    res = results_as_list(test_ds.query(query))

    # then
    assert len(res) == len(TEST_DATA)
