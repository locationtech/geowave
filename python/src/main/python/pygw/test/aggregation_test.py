#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from datetime import datetime

from pygw.index import SpatialIndexBuilder
from pygw.query import VectorAggregationQueryBuilder

from .conftest import POINT_TYPE_ADAPTER, POINT_GEOMETRY_FIELD, POINT_TIME_FIELD, POINT_TYPE_NAME, POINT_NUMBER_FIELD
from .conftest import write_test_data
from ..base import Envelope, Interval


def setup_query_builder(test_ds):
    # given
    index = SpatialIndexBuilder().set_name("idx1").create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    write_test_data(test_ds, index)

    qbldr = VectorAggregationQueryBuilder()
    constraints_factory = qbldr.constraints_factory()
    # filter encompasses 10 features (1, 1) - (10, 10)
    constraints = constraints_factory.cql_constraints("BBOX(the_geom, 0.5, 0.5, 10.5, 10.5)")
    qbldr.constraints(constraints)
    return qbldr


def test_bbox_aggregation(test_ds):
    # given
    qbldr = setup_query_builder(test_ds)

    # when
    qbldr.bbox_of_results(POINT_TYPE_NAME)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert isinstance(res, Envelope)
    assert res.get_min_x() == 1.0
    assert res.get_min_y() == 1.0
    assert res.get_max_x() == 10.0
    assert res.get_max_y() == 10.0

    # when
    qbldr.bbox_of_results_for_geometry_field(POINT_TYPE_NAME, POINT_GEOMETRY_FIELD)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert isinstance(res, Envelope)
    assert res.get_min_x() == 1.0
    assert res.get_min_y() == 1.0
    assert res.get_max_x() == 10.0
    assert res.get_max_y() == 10.0


def test_time_range_aggregation(test_ds):
    # given
    qbldr = setup_query_builder(test_ds)

    # when
    qbldr.time_range_of_results(POINT_TYPE_NAME)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert isinstance(res, Interval)
    assert res.get_start() == datetime.utcfromtimestamp(1)  # Start Date
    assert res.get_end() == datetime.utcfromtimestamp(10)  # End Date

    # when
    qbldr.time_range_of_results_for_time_field(POINT_TYPE_NAME, POINT_TIME_FIELD)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert isinstance(res, Interval)
    assert res.get_start() == datetime.utcfromtimestamp(1)  # Start Date
    assert res.get_end() == datetime.utcfromtimestamp(10)  # End Date


def test_math_aggregations(test_ds):
    # given
    qbldr = setup_query_builder(test_ds)

    # when
    qbldr.max(POINT_TYPE_NAME, POINT_NUMBER_FIELD)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert res == 10.0  # Maximum number

    # when
    qbldr.min(POINT_TYPE_NAME, POINT_NUMBER_FIELD)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert res == 1.0  # Minimum number

    # when
    qbldr.sum(POINT_TYPE_NAME, POINT_NUMBER_FIELD)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert res == 55.0  # 1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10


def test_count_aggregation(test_ds):
    # given
    qbldr = setup_query_builder(test_ds)

    # when
    qbldr.count(POINT_TYPE_NAME)
    res = test_ds.aggregate(qbldr.build())

    # then
    assert res == 10
