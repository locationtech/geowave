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
from datetime import datetime

from shapely.geometry import Polygon

from pygw.store import DataStoreFactory
from pygw.store.rocksdb import RocksDBOptions
from pygw.index import SpatialIndexBuilder
from pygw.index import SpatialTemporalIndexBuilder
from pygw.query import VectorQueryBuilder
from pygw.query import FilterFactory

from .conftest import TEST_DIR
from .conftest import POINT_TYPE_ADAPTER
from .conftest import TEST_DATA
from .conftest import write_test_data
from .conftest import results_as_list

# Test Deleting #
def test_cql_query(test_ds):
    # given
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    write_test_data(test_ds, index)

    # when
    qbldr = VectorQueryBuilder()
    constraints_factory = qbldr.constraints_factory()
    # filter encompasses 10 features (1, 1) - (10, 10)
    constraints = constraints_factory.cql_constraints("BBOX(the_geom, 0.5, 0.5, 10.5, 10.5)")
    qbldr.constraints(constraints)
    res = results_as_list(test_ds.query(qbldr.build()))

    # then
    assert len(res) == 10

def test_query_spatial(test_ds):
    # given
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    write_test_data(test_ds, index)

    # when
    qbldr = VectorQueryBuilder()
    stcb = qbldr.constraints_factory().spatial_temporal_constraints()
    # polygon encompasses 10 features (1, 1) - (10, 10)
    stcb.spatial_constraints(Polygon([[0.5, 0.5], [0.5, 10.5], [10.5, 10.5], [10.5, 0.5], [0.5, 0.5]]))
    stcb.spatial_constraints_compare_operation("CONTAINS")
    qbldr.constraints(stcb.build())
    res = results_as_list(test_ds.query(qbldr.build()))

    # then
    assert len(res) == 10

def test_query_temporal(test_ds):
    # given
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    write_test_data(test_ds, index)

    # when
    qbldr = VectorQueryBuilder()
    stcb = qbldr.constraints_factory().spatial_temporal_constraints()
    # time range encompasses 10 features (1, 1) - (10, 10)
    stcb.add_time_range(datetime.fromtimestamp(1), datetime.fromtimestamp(11))
    qbldr.constraints(stcb.build())
    res = results_as_list(test_ds.query(qbldr.build()))

    for feature in res:
        print(feature.get_id())
        print(feature.get_default_geometry())
    # then
    assert len(res) == 10

def test_query_spatial_temporal(test_ds):
    # given
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    write_test_data(test_ds, index)

    # when
    qbldr = VectorQueryBuilder()
    stcb = qbldr.constraints_factory().spatial_temporal_constraints()
    # polygon encompasses 10 features (1, 1) - (10, 10)
    stcb.spatial_constraints(Polygon([[0.5, 0.5], [0.5, 10.5], [10.5, 10.5], [10.5, 0.5], [0.5, 0.5]]))
    stcb.spatial_constraints_compare_operation("CONTAINS")
    # time range encompasses 10 features (5, 5) - (14, 14)
    stcb.add_time_range(datetime.fromtimestamp(5), datetime.fromtimestamp(15))
    qbldr.constraints(stcb.build())
    res = results_as_list(test_ds.query(qbldr.build()))

    # then
    assert len(res) == 6

def test_query_filter(test_ds):
    # given
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    write_test_data(test_ds, index)

    # when
    qbldr = VectorQueryBuilder()
    filter_factory = FilterFactory()
    # filter encompasses 10 features (1, 1) - (10, 10)
    bbox_filter = filter_factory.bbox(filter_factory.property("the_geom"), 0.5, 0.5, 10.5, 10.5, "EPSG:4326")
    constraints = qbldr.constraints_factory().filter_constraints(bbox_filter)
    qbldr.constraints(constraints)
    res = results_as_list(test_ds.query(qbldr.build()))

    # then
    assert len(res) == 10
