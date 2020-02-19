#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

import pytest
import os

from pygw.store import DataStoreFactory
from pygw.store.accumulo import AccumuloOptions
from pygw.store.bigtable import BigTableOptions
from pygw.store.cassandra import CassandraOptions
from pygw.store.dynamodb import DynamoDBOptions
from pygw.store.hbase import HBaseOptions
from pygw.store.kudu import KuduOptions
from pygw.store.redis import RedisOptions
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
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    index2 = SpatialIndexBuilder().set_name("idx2").create_index()

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
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    index2 = SpatialIndexBuilder().set_name("idx2").create_index()
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
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    index2 = SpatialIndexBuilder().set_name("idx2").create_index()
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

def _test_base_options(options, server_side_possible=True):
    options.set_geowave_namespace("test_namespace")
    assert options.get_geowave_namespace() == "test_namespace"

    persist_data_statistics = not options.is_persist_data_statistics()
    options.set_persist_data_statistics(persist_data_statistics)
    assert options.is_persist_data_statistics() == persist_data_statistics

    secondary_indexing = not options.is_secondary_indexing()
    options.set_secondary_indexing(secondary_indexing)
    assert options.is_secondary_indexing() == secondary_indexing

    block_cache = not options.is_enable_block_cache()
    options.set_enable_block_cache(block_cache)
    assert options.is_enable_block_cache() == block_cache

    server_side_library = not options.is_server_side_library_enabled()
    options.set_secondary_indexing(False)
    options.set_server_side_library_enabled(server_side_library)
    if server_side_possible:
        assert options.is_server_side_library_enabled() == server_side_library
    else:
        assert options.is_server_side_library_enabled() == False

    options.set_max_range_decomposition(42)
    assert options.get_max_range_decomposition() == 42

    options.set_aggregation_max_range_decomposition(43)
    assert options.get_aggregation_max_range_decomposition() == 43

    visibility = not options.is_visibility_enabled()
    options.set_enable_visibility(visibility)
    assert options.is_visibility_enabled() == visibility


def test_accumulo_options():
    options = AccumuloOptions()
    options.set_zookeeper("test_zookeeper")
    assert options.get_zookeeper() == "test_zookeeper"
    options.set_instance("test_instance")
    assert options.get_instance() == "test_instance"
    options.set_user("test_user")
    assert options.get_user() == "test_user"
    options.set_password("test_password")
    assert options.get_password() == "test_password"
    locality_groups = not options.is_use_locality_groups()
    options.set_use_locality_groups(locality_groups)
    assert options.is_use_locality_groups() == locality_groups
    _test_base_options(options)

def test_bigtable_options():
    options = BigTableOptions()
    options.set_scan_cache_size(42)
    assert options.get_scan_cache_size() == 42
    options.set_project_id("test_project_id")
    assert options.get_project_id() == "test_project_id"
    options.set_instance_id("test_instance_id")
    assert options.get_instance_id() == "test_instance_id"
    _test_base_options(options)

def test_cassandra_options():
    options = CassandraOptions()
    options.set_contact_point("test_contact_point")
    assert options.get_contact_point() == "test_contact_point"
    options.set_batch_write_size(42)
    assert options.get_batch_write_size() == 42
    durable_writes = not options.is_durable_writes()
    options.set_durable_writes(durable_writes)
    assert options.is_durable_writes() == durable_writes
    options.set_replication_factor(43)
    assert options.get_replication_factor() == 43
    _test_base_options(options, False)

def test_dynamodb_options():
    options = DynamoDBOptions()
    options.set_region("us-east-1")
    assert options.get_region() == "us-east-1"
    options.set_region(None)
    assert options.get_region() is None
    options.set_endpoint("test_endpoint")
    assert options.get_endpoint() == "test_endpoint"
    options.set_write_capacity(42)
    assert options.get_write_capacity() == 42
    options.set_read_capacity(43)
    assert options.get_read_capacity() == 43
    enable_cache_response_metadata = not options.is_enable_cache_response_metadata()
    options.set_enable_cache_response_metadata(enable_cache_response_metadata)
    assert options.is_enable_cache_response_metadata() == enable_cache_response_metadata
    options.set_protocol("HTTP")
    assert options.get_protocol() == "HTTP"
    options.set_protocol(None)
    assert options.get_protocol() is None
    options.set_max_connections(44)
    assert options.get_max_connections() == 44
    _test_base_options(options, False)

def test_hbase_options():
    options = HBaseOptions()
    options.set_zookeeper("test_zookeeper")
    assert options.get_zookeeper() == "test_zookeeper"
    options.set_scan_cache_size(42)
    assert options.get_scan_cache_size() == 42
    options.set_server_side_library_enabled(True)
    verify_coprocessors = not options.is_verify_coprocessors()
    options.set_verify_coprocessors(verify_coprocessors)
    assert options.is_verify_coprocessors() == verify_coprocessors
    options.set_coprocessor_jar("test_jar")
    assert options.get_coprocessor_jar() == "test_jar"
    _test_base_options(options)

def test_kudu_options():
    options = KuduOptions()
    options.set_kudu_master("test_master")
    assert options.get_kudu_master() == "test_master"
    _test_base_options(options, False)

def test_redis_options():
    options = RedisOptions()
    options.set_address("test_address")
    assert options.get_address() == "test_address"
    options.set_compression("L4Z")
    assert options.get_compression() == "L4Z"
    _test_base_options(options, False)

def test_rocksdb_options():
    options = RocksDBOptions()
    options.set_directory("test_directory")
    assert options.get_directory() == "test_directory"
    compact_on_wriite = not options.is_compact_on_write()
    options.set_compact_on_write(compact_on_wriite)
    assert options.is_compact_on_write() == compact_on_wriite
    options.set_batch_write_size(42)
    assert options.get_batch_write_size() == 42
    _test_base_options(options, False)
