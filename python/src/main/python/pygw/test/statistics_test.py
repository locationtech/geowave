#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from datetime import datetime
from functools import reduce

from pygw.index import SpatialIndexBuilder

from .conftest import POINT_TYPE_ADAPTER, POINT_TYPE_NAME, POINT_NUMBER_FIELD, POINT_GEOMETRY_FIELD, POINT_TIME_FIELD, \
    POINT_SHAPE_FIELD, POINT_COLOR_FIELD, results_as_list
from .conftest import write_test_data, write_test_data_offset
from ..base import Interval, Envelope

from shapely.geometry import Polygon
from ..base.range import Range
from ..query.statistics.statistic_query_builder import DataTypeStatisticQueryBuilder
from ..query.statistics.statistic_query_builder import FieldStatisticQueryBuilder
from ..query.statistics.statistic_query_builder import IndexStatisticQueryBuilder
from ..query.statistics.statistic_query_builder import StatisticQueryBuilder
from ..query.statistics.statistic_query import StatisticQuery
from ..statistics import DataTypeStatisticType, FieldStatisticType, IndexStatisticType, BinConstraints, StatisticValue
from ..statistics.binning_strategy import CompositeBinningStrategy, DataTypeBinningStrategy, \
    FieldValueBinningStrategy, NumericRangeFieldValueBinningStrategy, TimeRangeFieldValueBinningStrategy, \
    PartitionBinningStrategy, SpatialFieldValueBinningStrategy
from ..statistics.data_type import CountStatistic
from ..statistics.field import BloomFilterStatistic, BoundingBoxStatistic, CountMinSketchStatistic, \
    FixedBinNumericHistogramStatistic, HyperLogLogStatistic, NumericHistogramStatistic, NumericMeanStatistic, \
    NumericRangeStatistic, NumericStatsStatistic, TimeRangeStatistic
from ..statistics.field.bloom_filter_statistic import BloomFilter
from ..statistics.field.count_min_sketch_statistic import CountMinSketch
from ..statistics.field.fixed_bin_numeric_histogram_statistic import FixedBinNumericHistogram
from ..statistics.field.hyper_log_log_statistic import HyperLogLogPlus
from ..statistics.field.numeric_histogram_statistic import NumericHistogram
from ..statistics.field.numeric_stats_statistic import Stats
from ..statistics.index import DifferingVisibilityCountStatistic, DuplicateEntryCountStatistic, \
    FieldVisibilityCountStatistic, MaxDuplicatesStatistic, PartitionsStatistic, RowRangeHistogramStatistic, \
    IndexMetaDataSetStatistic

INTERNAL_TAG = 'internal'
TEST_TAG = 'test'
TEST_INDEX = 'spatial_index'


def test_statistic_query(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    binning_strategy = FieldValueBinningStrategy([POINT_COLOR_FIELD, POINT_SHAPE_FIELD])
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # when
    stat_query_builder = StatisticQueryBuilder.new_builder(CountStatistic.STATS_TYPE)
    stat_query_builder.type_name(POINT_TYPE_NAME)
    stat_query_builder.tag(TEST_TAG)
    stat_query_builder.bin_constraints(BinConstraints.of_prefix(b'BLUE'))
    result = results_as_list(test_ds.query_statistics(stat_query_builder.build()))

    # then
    assert len(result) == 4
    merged = None
    for stat_value in result:
        if merged is None:
            merged = stat_value
        else:
            merged.merge(stat_value)
        assert isinstance(stat_value, StatisticValue)
        assert stat_value.get_value() == 30
        assert isinstance(stat_value.get_statistic(), CountStatistic)
    assert merged.get_value() == 120

    # when
    aggregated_result = test_ds.aggregate_statistics(stat_query_builder.build())

    # then
    assert isinstance(aggregated_result, StatisticValue)
    assert aggregated_result.get_value() == 120


def test_query_transform(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)
    numeric_range_stat = NumericRangeStatistic()
    numeric_range_stat.set_tag(TEST_TAG)
    numeric_range_stat.set_type_name(POINT_TYPE_NAME)
    numeric_range_stat.set_field_name(POINT_NUMBER_FIELD)
    test_ds.add_statistic(numeric_range_stat)
    write_test_data(test_ds, index)

    # when
    stat_query_builder = StatisticQueryBuilder.new_builder(NumericRangeStatistic.STATS_TYPE)
    stat_query_builder.type_name(POINT_TYPE_NAME)
    stat_query_builder.field_name(POINT_NUMBER_FIELD)
    stat_query_builder.tag(TEST_TAG)
    result = results_as_list(test_ds.query_statistics(stat_query_builder.build()))

    # then
    assert len(result) == 1
    assert isinstance(result[0], StatisticValue)
    value = result[0].get_value()
    assert isinstance(value, Range)
    assert value.get_minimum() == -180
    assert value.get_maximum() == 179

    # when
    aggregated_result = test_ds.aggregate_statistics(stat_query_builder.build())

    # then
    assert isinstance(aggregated_result, StatisticValue)
    value = aggregated_result.get_value()
    assert isinstance(value, Range)
    assert value.get_minimum() == -180
    assert value.get_maximum() == 179


def test_statistic_query_builders():
    # exercise all methods to make sure their java connections are valid
    index_query_builder = StatisticQueryBuilder.new_builder(DifferingVisibilityCountStatistic.STATS_TYPE)
    assert isinstance(index_query_builder, IndexStatisticQueryBuilder)
    index_query_builder.index_name('idx')
    index_query_builder.tag('test')
    index_query_builder.add_authorization('auth1')
    index_query_builder.authorizations(['auth1', 'auth2'])
    index_query_builder.internal()
    query = index_query_builder.build()
    assert isinstance(query, StatisticQuery)

    data_type_query_builder = StatisticQueryBuilder.new_builder(CountStatistic.STATS_TYPE)
    assert isinstance(data_type_query_builder, DataTypeStatisticQueryBuilder)
    data_type_query_builder.type_name(POINT_TYPE_NAME)
    query = data_type_query_builder.build()
    assert isinstance(query, StatisticQuery)

    field_query_builder = StatisticQueryBuilder.new_builder(BloomFilterStatistic.STATS_TYPE)
    assert isinstance(field_query_builder, FieldStatisticQueryBuilder)
    field_query_builder.type_name(POINT_TYPE_NAME)
    field_query_builder.field_name(POINT_NUMBER_FIELD)
    query = field_query_builder.build()
    assert isinstance(query, StatisticQuery)

    statistic_query_builder = StatisticQueryBuilder.count()
    assert isinstance(statistic_query_builder, DataTypeStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.bloom_filter()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.bbox()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.count_min_sketch()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.fixed_bin_numeric_histogram()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.hyper_log_log()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.numeric_histogram()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.numeric_mean()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.numeric_range()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.differing_visibility_count()
    assert isinstance(statistic_query_builder, IndexStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.duplicate_entry_count()
    assert isinstance(statistic_query_builder, IndexStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.field_visibility_count()
    assert isinstance(statistic_query_builder, IndexStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.index_meta_data_set()
    assert isinstance(statistic_query_builder, IndexStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.max_duplicates()
    assert isinstance(statistic_query_builder, IndexStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.partitions()
    assert isinstance(statistic_query_builder, IndexStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.row_range_histogram()
    assert isinstance(statistic_query_builder, IndexStatisticQueryBuilder)

    statistic_query_builder = StatisticQueryBuilder.numeric_stats()
    assert isinstance(statistic_query_builder, FieldStatisticQueryBuilder)


def test_count_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    assert isinstance(count_stat, CountStatistic)
    assert count_stat.get_tag() == TEST_TAG
    assert count_stat.get_type_name() == POINT_TYPE_NAME
    assert count_stat.get_description() is not None
    assert count_stat.get_binning_strategy() is None
    assert isinstance(count_stat.get_statistic_type(), DataTypeStatisticType)
    assert count_stat.get_statistic_type().get_string() == 'COUNT'
    assert test_ds.get_statistic_value(count_stat) == 360

    # test alternate constructors
    count_stat = CountStatistic(POINT_TYPE_NAME)
    count_stat.set_internal()
    assert count_stat.get_type_name() == POINT_TYPE_NAME
    assert count_stat.get_tag() == INTERNAL_TAG


def test_bloom_filter_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    bloom_filter_stat = BloomFilterStatistic()
    bloom_filter_stat.set_tag(TEST_TAG)
    bloom_filter_stat.set_type_name(POINT_TYPE_NAME)
    bloom_filter_stat.set_field_name(POINT_NUMBER_FIELD)
    test_ds.add_statistic(bloom_filter_stat)
    write_test_data(test_ds, index)

    # then
    bloom_filter_stat = test_ds.get_field_statistic(
        BloomFilterStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(bloom_filter_stat, BloomFilterStatistic)
    assert bloom_filter_stat.get_tag() == TEST_TAG
    assert bloom_filter_stat.get_type_name() == POINT_TYPE_NAME
    assert bloom_filter_stat.get_field_name() == POINT_NUMBER_FIELD
    assert bloom_filter_stat.get_description() is not None
    assert bloom_filter_stat.get_binning_strategy() is None
    assert isinstance(bloom_filter_stat.get_statistic_type(), FieldStatisticType)
    assert bloom_filter_stat.get_statistic_type().get_string() == 'BLOOM_FILTER'
    bloom_filter = test_ds.get_statistic_value(bloom_filter_stat)
    assert isinstance(bloom_filter, BloomFilter)
    assert bloom_filter.might_contain('-180.0')
    assert bloom_filter.might_contain('179.0')
    assert not bloom_filter.might_contain('garbage')
    assert bloom_filter.approximate_element_count() > 300
    assert bloom_filter.expected_fpp() is not None

    # test alternate constructors
    bloom_filter_stat = BloomFilterStatistic(POINT_TYPE_NAME, POINT_NUMBER_FIELD)
    bloom_filter_stat.set_internal()
    assert bloom_filter_stat.get_type_name() == POINT_TYPE_NAME
    assert bloom_filter_stat.get_field_name() == POINT_NUMBER_FIELD
    assert bloom_filter_stat.get_tag() == INTERNAL_TAG


def test_bounding_box_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    bounding_box_stat = BoundingBoxStatistic()
    bounding_box_stat.set_tag(TEST_TAG)
    bounding_box_stat.set_type_name(POINT_TYPE_NAME)
    bounding_box_stat.set_field_name(POINT_GEOMETRY_FIELD)
    test_ds.add_statistic(bounding_box_stat)
    write_test_data_offset(test_ds, index)

    # then
    bounding_box_stat = test_ds.get_field_statistic(
        BoundingBoxStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_GEOMETRY_FIELD,
        TEST_TAG)
    assert isinstance(bounding_box_stat, BoundingBoxStatistic)
    assert bounding_box_stat.get_tag() == TEST_TAG
    assert bounding_box_stat.get_type_name() == POINT_TYPE_NAME
    assert bounding_box_stat.get_field_name() == POINT_GEOMETRY_FIELD
    assert bounding_box_stat.get_description() is not None
    assert bounding_box_stat.get_binning_strategy() is None
    assert isinstance(bounding_box_stat.get_statistic_type(), FieldStatisticType)
    assert bounding_box_stat.get_statistic_type().get_string() == 'BOUNDING_BOX'
    bounding_box = test_ds.get_statistic_value(bounding_box_stat)
    assert isinstance(bounding_box, Envelope)
    assert bounding_box.get_min_x() == -179.5
    assert bounding_box.get_min_y() == -89.5
    assert bounding_box.get_max_x() == 179.5
    assert bounding_box.get_max_y() == 89.5

    # test alternate constructors
    bounding_box_stat = BoundingBoxStatistic(POINT_TYPE_NAME, POINT_GEOMETRY_FIELD)
    bounding_box_stat.set_internal()
    assert bounding_box_stat.get_type_name() == POINT_TYPE_NAME
    assert bounding_box_stat.get_field_name() == POINT_GEOMETRY_FIELD
    assert bounding_box_stat.get_tag() == INTERNAL_TAG


def test_count_min_sketch_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    count_min_sketch_stat = CountMinSketchStatistic()
    count_min_sketch_stat.set_tag(TEST_TAG)
    count_min_sketch_stat.set_type_name(POINT_TYPE_NAME)
    count_min_sketch_stat.set_field_name(POINT_NUMBER_FIELD)
    count_min_sketch_stat.set_error_factor(0.002)
    count_min_sketch_stat.set_probability_of_correctness(0.8)
    test_ds.add_statistic(count_min_sketch_stat)
    write_test_data(test_ds, index)

    # then
    count_min_sketch_stat = test_ds.get_field_statistic(
        CountMinSketchStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(count_min_sketch_stat, CountMinSketchStatistic)
    assert count_min_sketch_stat.get_tag() == TEST_TAG
    assert count_min_sketch_stat.get_type_name() == POINT_TYPE_NAME
    assert count_min_sketch_stat.get_field_name() == POINT_NUMBER_FIELD
    assert count_min_sketch_stat.get_error_factor() == 0.002
    assert count_min_sketch_stat.get_probability_of_correctness() == 0.8
    assert count_min_sketch_stat.get_description() is not None
    assert count_min_sketch_stat.get_binning_strategy() is None
    assert isinstance(count_min_sketch_stat.get_statistic_type(), FieldStatisticType)
    assert count_min_sketch_stat.get_statistic_type().get_string() == 'COUNT_MIN_SKETCH'
    count_min_sketch = test_ds.get_statistic_value(count_min_sketch_stat)
    assert isinstance(count_min_sketch, CountMinSketch)
    assert count_min_sketch.get_relative_error() == 0.002
    assert count_min_sketch.get_confidence() > 0.5
    assert count_min_sketch.estimate_count('90.0') == 1

    # test alternate constructors
    count_min_sketch = CountMinSketchStatistic(POINT_TYPE_NAME, POINT_GEOMETRY_FIELD, 0.002, 0.8)
    count_min_sketch.set_internal()
    assert count_min_sketch.get_type_name() == POINT_TYPE_NAME
    assert count_min_sketch.get_field_name() == POINT_GEOMETRY_FIELD
    assert count_min_sketch.get_error_factor() == 0.002
    assert count_min_sketch.get_probability_of_correctness() == 0.8
    assert count_min_sketch.get_tag() == INTERNAL_TAG


def test_fixed_bin_numeric_histogram(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    fixed_bin_stat = FixedBinNumericHistogramStatistic()
    fixed_bin_stat.set_tag(TEST_TAG)
    fixed_bin_stat.set_type_name(POINT_TYPE_NAME)
    fixed_bin_stat.set_field_name(POINT_NUMBER_FIELD)
    fixed_bin_stat.set_num_bins(128)
    fixed_bin_stat.set_min_value(-180.0)
    fixed_bin_stat.set_max_value(180.0)
    test_ds.add_statistic(fixed_bin_stat)
    write_test_data(test_ds, index)

    # then
    fixed_bin_stat = test_ds.get_field_statistic(
        FixedBinNumericHistogramStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(fixed_bin_stat, FixedBinNumericHistogramStatistic)
    assert fixed_bin_stat.get_tag() == TEST_TAG
    assert fixed_bin_stat.get_type_name() == POINT_TYPE_NAME
    assert fixed_bin_stat.get_field_name() == POINT_NUMBER_FIELD
    assert fixed_bin_stat.get_num_bins() == 128
    assert fixed_bin_stat.get_min_value() == -180
    assert fixed_bin_stat.get_max_value() == 180
    assert fixed_bin_stat.get_description() is not None
    assert fixed_bin_stat.get_binning_strategy() is None
    assert isinstance(fixed_bin_stat.get_statistic_type(), FieldStatisticType)
    assert fixed_bin_stat.get_statistic_type().get_string() == 'FIXED_BIN_NUMERIC_HISTOGRAM'
    histogram = test_ds.get_statistic_value(fixed_bin_stat)
    assert isinstance(histogram, FixedBinNumericHistogram)
    assert histogram.get_num_bins() == 128
    assert histogram.get_min_value() == -180
    assert histogram.get_max_value() == 180
    assert histogram.cdf(0) == 0.5
    assert len(histogram.bin_quantiles(0)) == 0
    assert histogram.quantile(0.0) == -180
    assert 180 < histogram.quantile(1.0) < 185
    assert histogram.sum(0) == 180
    assert histogram.percent_population_over_range(-90, 0) == 0.25
    assert histogram.total_sample_size() == 360
    assert histogram.get_total_count() == 360
    assert len(histogram.count(0)) == 0

    # test alternate constructors
    fixed_bin_stat = FixedBinNumericHistogramStatistic(POINT_TYPE_NAME, POINT_NUMBER_FIELD, 128, -180, 180)
    fixed_bin_stat.set_internal()
    assert fixed_bin_stat.get_type_name() == POINT_TYPE_NAME
    assert fixed_bin_stat.get_field_name() == POINT_NUMBER_FIELD
    assert fixed_bin_stat.get_num_bins() == 128
    assert fixed_bin_stat.get_min_value() == -180
    assert fixed_bin_stat.get_max_value() == 180
    assert fixed_bin_stat.get_tag() == INTERNAL_TAG


def test_hyper_log_log_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    hyper_log_log_stat = HyperLogLogStatistic()
    hyper_log_log_stat.set_tag(TEST_TAG)
    hyper_log_log_stat.set_type_name(POINT_TYPE_NAME)
    hyper_log_log_stat.set_field_name(POINT_NUMBER_FIELD)
    hyper_log_log_stat.set_precision(24)
    test_ds.add_statistic(hyper_log_log_stat)
    write_test_data(test_ds, index)

    # then
    hyper_log_log_stat = test_ds.get_field_statistic(
        HyperLogLogStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(hyper_log_log_stat, HyperLogLogStatistic)
    assert hyper_log_log_stat.get_tag() == TEST_TAG
    assert hyper_log_log_stat.get_type_name() == POINT_TYPE_NAME
    assert hyper_log_log_stat.get_field_name() == POINT_NUMBER_FIELD
    assert hyper_log_log_stat.get_precision() == 24
    assert hyper_log_log_stat.get_description() is not None
    assert hyper_log_log_stat.get_binning_strategy() is None
    assert isinstance(hyper_log_log_stat.get_statistic_type(), FieldStatisticType)
    assert hyper_log_log_stat.get_statistic_type().get_string() == 'HYPER_LOG_LOG'
    hyper_log_log = test_ds.get_statistic_value(hyper_log_log_stat)
    assert isinstance(hyper_log_log, HyperLogLogPlus)
    assert 350 < hyper_log_log.cardinality() < 370

    # test alternate constructors
    fixed_bin_stat = HyperLogLogStatistic(POINT_TYPE_NAME, POINT_NUMBER_FIELD, 31)
    fixed_bin_stat.set_internal()
    assert fixed_bin_stat.get_type_name() == POINT_TYPE_NAME
    assert fixed_bin_stat.get_field_name() == POINT_NUMBER_FIELD
    assert fixed_bin_stat.get_precision() == 31
    assert fixed_bin_stat.get_tag() == INTERNAL_TAG


def test_numeric_histogram_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    numeric_histogram_stat = NumericHistogramStatistic()
    numeric_histogram_stat.set_tag(TEST_TAG)
    numeric_histogram_stat.set_type_name(POINT_TYPE_NAME)
    numeric_histogram_stat.set_field_name(POINT_NUMBER_FIELD)
    numeric_histogram_stat.set_compression(80)
    test_ds.add_statistic(numeric_histogram_stat)
    write_test_data(test_ds, index)

    # then
    numeric_histogram_stat = test_ds.get_field_statistic(
        NumericHistogramStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(numeric_histogram_stat, NumericHistogramStatistic)
    assert numeric_histogram_stat.get_tag() == TEST_TAG
    assert numeric_histogram_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_histogram_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_histogram_stat.get_compression() == 80
    assert numeric_histogram_stat.get_description() is not None
    assert numeric_histogram_stat.get_binning_strategy() is None
    assert isinstance(numeric_histogram_stat.get_statistic_type(), FieldStatisticType)
    assert numeric_histogram_stat.get_statistic_type().get_string() == 'NUMERIC_HISTOGRAM'
    histogram = test_ds.get_statistic_value(numeric_histogram_stat)
    assert isinstance(histogram, NumericHistogram)
    assert -182 < histogram.get_min_value() < -178
    assert 178 < histogram.get_max_value() < 182
    assert 0.48 < histogram.cdf(0) < 0.52
    assert -182 < histogram.quantile(0.0) < -178
    assert 178 < histogram.quantile(1.0) < 182
    assert 178 < histogram.sum(0) < 182
    assert histogram.get_total_count() == 360

    # test alternate constructors
    numeric_histogram_stat = NumericHistogramStatistic(POINT_TYPE_NAME, POINT_NUMBER_FIELD, 80)
    numeric_histogram_stat.set_internal()
    assert numeric_histogram_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_histogram_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_histogram_stat.get_compression() == 80
    assert numeric_histogram_stat.get_tag() == INTERNAL_TAG


def test_numeric_mean_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    numeric_mean_stat = NumericMeanStatistic()
    numeric_mean_stat.set_tag(TEST_TAG)
    numeric_mean_stat.set_type_name(POINT_TYPE_NAME)
    numeric_mean_stat.set_field_name(POINT_NUMBER_FIELD)
    test_ds.add_statistic(numeric_mean_stat)
    write_test_data(test_ds, index)

    # then
    numeric_mean_stat = test_ds.get_field_statistic(
        NumericMeanStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(numeric_mean_stat, NumericMeanStatistic)
    assert numeric_mean_stat.get_tag() == TEST_TAG
    assert numeric_mean_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_mean_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_mean_stat.get_description() is not None
    assert numeric_mean_stat.get_binning_strategy() is None
    assert isinstance(numeric_mean_stat.get_statistic_type(), FieldStatisticType)
    assert numeric_mean_stat.get_statistic_type().get_string() == 'NUMERIC_MEAN'
    assert test_ds.get_statistic_value(numeric_mean_stat) == -0.5  # Mean of values -180 to 179

    # test alternate constructors
    numeric_mean_stat = NumericMeanStatistic(POINT_TYPE_NAME, POINT_NUMBER_FIELD)
    numeric_mean_stat.set_internal()
    assert numeric_mean_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_mean_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_mean_stat.get_tag() == INTERNAL_TAG


def test_numeric_range_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    numeric_range_stat = NumericRangeStatistic()
    numeric_range_stat.set_tag(TEST_TAG)
    numeric_range_stat.set_type_name(POINT_TYPE_NAME)
    numeric_range_stat.set_field_name(POINT_NUMBER_FIELD)
    test_ds.add_statistic(numeric_range_stat)
    write_test_data(test_ds, index)

    # then
    numeric_range_stat = test_ds.get_field_statistic(
        NumericRangeStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(numeric_range_stat, NumericRangeStatistic)
    assert numeric_range_stat.get_tag() == TEST_TAG
    assert numeric_range_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_range_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_range_stat.get_description() is not None
    assert numeric_range_stat.get_binning_strategy() is None
    assert isinstance(numeric_range_stat.get_statistic_type(), FieldStatisticType)
    assert numeric_range_stat.get_statistic_type().get_string() == 'NUMERIC_RANGE'
    numeric_range = test_ds.get_statistic_value(numeric_range_stat)
    assert isinstance(numeric_range, Range)
    assert numeric_range.get_minimum() == -180
    assert numeric_range.get_maximum() == 179

    # test alternate constructors
    numeric_range_stat = NumericRangeStatistic(POINT_TYPE_NAME, POINT_NUMBER_FIELD)
    numeric_range_stat.set_internal()
    assert numeric_range_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_range_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_range_stat.get_tag() == INTERNAL_TAG


def test_numeric_stats_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    numeric_stats_stat = NumericStatsStatistic()
    numeric_stats_stat.set_tag(TEST_TAG)
    numeric_stats_stat.set_type_name(POINT_TYPE_NAME)
    numeric_stats_stat.set_field_name(POINT_NUMBER_FIELD)
    test_ds.add_statistic(numeric_stats_stat)
    write_test_data(test_ds, index)

    # then
    numeric_stats_stat = test_ds.get_field_statistic(
        NumericStatsStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_NUMBER_FIELD,
        TEST_TAG)
    assert isinstance(numeric_stats_stat, NumericStatsStatistic)
    assert numeric_stats_stat.get_tag() == TEST_TAG
    assert numeric_stats_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_stats_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_stats_stat.get_description() is not None
    assert numeric_stats_stat.get_binning_strategy() is None
    assert isinstance(numeric_stats_stat.get_statistic_type(), FieldStatisticType)
    assert numeric_stats_stat.get_statistic_type().get_string() == 'NUMERIC_STATS'
    stats = test_ds.get_statistic_value(numeric_stats_stat)
    assert isinstance(stats, Stats)
    assert stats.count() == 360
    assert stats.mean() == -0.5
    assert stats.sum() == -180
    assert 10799 < stats.population_variance() < 10801
    assert 103 < stats.population_standard_deviation() < 105
    assert 10829 < stats.sample_variance() < 10831
    assert 103 < stats.sample_standard_deviation() < 105
    assert stats.min() == -180
    assert stats.max() == 179

    # test alternate constructors
    numeric_stats_stat = NumericStatsStatistic(POINT_TYPE_NAME, POINT_NUMBER_FIELD)
    numeric_stats_stat.set_internal()
    assert numeric_stats_stat.get_type_name() == POINT_TYPE_NAME
    assert numeric_stats_stat.get_field_name() == POINT_NUMBER_FIELD
    assert numeric_stats_stat.get_tag() == INTERNAL_TAG


def test_time_range_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    time_range_stat = TimeRangeStatistic()
    time_range_stat.set_tag(TEST_TAG)
    time_range_stat.set_type_name(POINT_TYPE_NAME)
    time_range_stat.set_field_name(POINT_TIME_FIELD)
    test_ds.add_statistic(time_range_stat)
    write_test_data(test_ds, index)

    # then
    time_range_stat = test_ds.get_field_statistic(
        TimeRangeStatistic.STATS_TYPE,
        POINT_TYPE_NAME,
        POINT_TIME_FIELD,
        TEST_TAG)
    assert isinstance(time_range_stat, TimeRangeStatistic)
    assert time_range_stat.get_tag() == TEST_TAG
    assert time_range_stat.get_type_name() == POINT_TYPE_NAME
    assert time_range_stat.get_field_name() == POINT_TIME_FIELD
    assert time_range_stat.get_description() is not None
    assert time_range_stat.get_binning_strategy() is None
    assert isinstance(time_range_stat.get_statistic_type(), FieldStatisticType)
    assert time_range_stat.get_statistic_type().get_string() == 'TIME_RANGE'
    time_range = test_ds.get_statistic_value(time_range_stat)
    assert isinstance(time_range, Interval)
    assert time_range.get_start() == datetime.utcfromtimestamp(-180)
    assert time_range.get_end() == datetime.utcfromtimestamp(179)

    # test alternate constructors
    time_range_stat = TimeRangeStatistic(POINT_TYPE_NAME, POINT_TIME_FIELD)
    time_range_stat.set_internal()
    assert time_range_stat.get_type_name() == POINT_TYPE_NAME
    assert time_range_stat.get_field_name() == POINT_TIME_FIELD
    assert time_range_stat.get_tag() == INTERNAL_TAG


def test_differing_visibility_count_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    differing_visibility_stat = DifferingVisibilityCountStatistic()
    differing_visibility_stat.set_tag(TEST_TAG)
    differing_visibility_stat.set_index_name(TEST_INDEX)
    test_ds.add_statistic(differing_visibility_stat)
    write_test_data(test_ds, index)

    # then
    differing_visibility_stat = test_ds.get_index_statistic(
        DifferingVisibilityCountStatistic.STATS_TYPE,
        TEST_INDEX,
        TEST_TAG)
    assert isinstance(differing_visibility_stat, DifferingVisibilityCountStatistic)
    assert differing_visibility_stat.get_tag() == TEST_TAG
    assert differing_visibility_stat.get_index_name() == TEST_INDEX
    assert differing_visibility_stat.get_description() is not None
    assert differing_visibility_stat.get_binning_strategy() is None
    assert isinstance(differing_visibility_stat.get_statistic_type(), IndexStatisticType)
    assert differing_visibility_stat.get_statistic_type().get_string() == 'DIFFERING_VISIBILITY_COUNT'
    assert test_ds.get_statistic_value(differing_visibility_stat) == 0

    # test alternate constructors
    differing_visibility_stat = DifferingVisibilityCountStatistic(TEST_INDEX)
    differing_visibility_stat.set_internal()
    assert differing_visibility_stat.get_index_name() == TEST_INDEX
    assert differing_visibility_stat.get_tag() == INTERNAL_TAG


def test_duplicate_entry_count_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    duplicate_entry_stat = DuplicateEntryCountStatistic()
    duplicate_entry_stat.set_tag(TEST_TAG)
    duplicate_entry_stat.set_index_name(TEST_INDEX)
    test_ds.add_statistic(duplicate_entry_stat)
    write_test_data(test_ds, index)

    # then
    duplicate_entry_stat = test_ds.get_index_statistic(
        DuplicateEntryCountStatistic.STATS_TYPE,
        TEST_INDEX,
        TEST_TAG)
    assert isinstance(duplicate_entry_stat, DuplicateEntryCountStatistic)
    assert duplicate_entry_stat.get_tag() == TEST_TAG
    assert duplicate_entry_stat.get_index_name() == TEST_INDEX
    assert duplicate_entry_stat.get_description() is not None
    assert duplicate_entry_stat.get_binning_strategy() is None
    assert isinstance(duplicate_entry_stat.get_statistic_type(), IndexStatisticType)
    assert duplicate_entry_stat.get_statistic_type().get_string() == 'DUPLICATE_ENTRY_COUNT'
    assert test_ds.get_statistic_value(duplicate_entry_stat) == 0

    # test alternate constructors
    duplicate_entry_stat = DuplicateEntryCountStatistic(TEST_INDEX)
    duplicate_entry_stat.set_internal()
    assert duplicate_entry_stat.get_index_name() == TEST_INDEX
    assert duplicate_entry_stat.get_tag() == INTERNAL_TAG


def test_field_visibility_count_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    field_visibility_stat = FieldVisibilityCountStatistic()
    field_visibility_stat.set_tag(TEST_TAG)
    field_visibility_stat.set_index_name(TEST_INDEX)
    test_ds.add_statistic(field_visibility_stat)
    write_test_data(test_ds, index)

    # then
    field_visibility_stat = test_ds.get_index_statistic(
        FieldVisibilityCountStatistic.STATS_TYPE,
        TEST_INDEX,
        TEST_TAG)
    assert isinstance(field_visibility_stat, FieldVisibilityCountStatistic)
    assert field_visibility_stat.get_tag() == TEST_TAG
    assert field_visibility_stat.get_index_name() == TEST_INDEX
    assert field_visibility_stat.get_description() is not None
    assert field_visibility_stat.get_binning_strategy() is None
    assert isinstance(field_visibility_stat.get_statistic_type(), IndexStatisticType)
    assert field_visibility_stat.get_statistic_type().get_string() == 'FIELD_VISIBILITY_COUNT'
    visibility_counts = test_ds.get_statistic_value(field_visibility_stat)
    assert isinstance(visibility_counts, dict)
    assert len(visibility_counts) == 1
    assert b'' in visibility_counts
    assert visibility_counts[b''] == 360

    # test alternate constructors
    field_visibility_stat = FieldVisibilityCountStatistic(TEST_INDEX)
    field_visibility_stat.set_internal()
    assert field_visibility_stat.get_index_name() == TEST_INDEX
    assert field_visibility_stat.get_tag() == INTERNAL_TAG


def test_index_meta_data_set_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    index_meta_data_set_stat = IndexMetaDataSetStatistic()
    index_meta_data_set_stat.set_tag(TEST_TAG)
    index_meta_data_set_stat.set_index_name(TEST_INDEX)
    test_ds.add_statistic(index_meta_data_set_stat)
    write_test_data(test_ds, index)

    # then
    index_meta_data_set_stat = test_ds.get_index_statistic(
        IndexMetaDataSetStatistic.STATS_TYPE,
        TEST_INDEX,
        TEST_TAG)
    assert isinstance(index_meta_data_set_stat, IndexMetaDataSetStatistic)
    assert index_meta_data_set_stat.get_tag() == TEST_TAG
    assert index_meta_data_set_stat.get_index_name() == TEST_INDEX
    assert index_meta_data_set_stat.get_description() is not None
    assert index_meta_data_set_stat.get_binning_strategy() is None
    assert isinstance(index_meta_data_set_stat.get_statistic_type(), IndexStatisticType)
    assert index_meta_data_set_stat.get_statistic_type().get_string() == 'INDEX_METADATA'
    assert len(test_ds.get_statistic_value(index_meta_data_set_stat)) == 0

    # test alternate constructors
    index_meta_data_set_stat = IndexMetaDataSetStatistic(TEST_INDEX)
    index_meta_data_set_stat.set_internal()
    assert index_meta_data_set_stat.get_index_name() == TEST_INDEX
    assert index_meta_data_set_stat.get_tag() == INTERNAL_TAG


def test_max_duplicates_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    max_duplicates_stat = MaxDuplicatesStatistic()
    max_duplicates_stat.set_tag(TEST_TAG)
    max_duplicates_stat.set_index_name(TEST_INDEX)
    test_ds.add_statistic(max_duplicates_stat)
    write_test_data(test_ds, index)

    # then
    max_duplicates_stat = test_ds.get_index_statistic(
        MaxDuplicatesStatistic.STATS_TYPE,
        TEST_INDEX,
        TEST_TAG)
    assert isinstance(max_duplicates_stat, MaxDuplicatesStatistic)
    assert max_duplicates_stat.get_tag() == TEST_TAG
    assert max_duplicates_stat.get_index_name() == TEST_INDEX
    assert max_duplicates_stat.get_description() is not None
    assert max_duplicates_stat.get_binning_strategy() is None
    assert isinstance(max_duplicates_stat.get_statistic_type(), IndexStatisticType)
    assert max_duplicates_stat.get_statistic_type().get_string() == 'MAX_DUPLICATES'
    assert test_ds.get_statistic_value(max_duplicates_stat) == 0

    # test alternate constructors
    max_duplicates_stat = MaxDuplicatesStatistic(TEST_INDEX)
    max_duplicates_stat.set_internal()
    assert max_duplicates_stat.get_index_name() == TEST_INDEX
    assert max_duplicates_stat.get_tag() == INTERNAL_TAG


def test_partitions_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    partitions_stat = PartitionsStatistic()
    partitions_stat.set_tag(TEST_TAG)
    partitions_stat.set_index_name(TEST_INDEX)
    test_ds.add_statistic(partitions_stat)
    write_test_data(test_ds, index)

    # then
    partitions_stat = test_ds.get_index_statistic(
        PartitionsStatistic.STATS_TYPE,
        TEST_INDEX,
        TEST_TAG)
    assert isinstance(partitions_stat, PartitionsStatistic)
    assert partitions_stat.get_tag() == TEST_TAG
    assert partitions_stat.get_index_name() == TEST_INDEX
    assert partitions_stat.get_description() is not None
    assert partitions_stat.get_binning_strategy() is None
    assert isinstance(partitions_stat.get_statistic_type(), IndexStatisticType)
    assert partitions_stat.get_statistic_type().get_string() == 'PARTITIONS'
    partitions = test_ds.get_statistic_value(partitions_stat)
    assert isinstance(partitions, set)
    assert len(partitions) >= 1
    for item in partitions:
        assert isinstance(item, bytes)

    # test alternate constructors
    partitions_stat = PartitionsStatistic(TEST_INDEX)
    partitions_stat.set_internal()
    assert partitions_stat.get_index_name() == TEST_INDEX
    assert partitions_stat.get_tag() == INTERNAL_TAG


def test_row_range_histogram_statistic(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    row_range_histogram_stat = RowRangeHistogramStatistic()
    row_range_histogram_stat.set_tag(TEST_TAG)
    row_range_histogram_stat.set_index_name(TEST_INDEX)
    test_ds.add_statistic(row_range_histogram_stat)
    write_test_data(test_ds, index)

    # then
    row_range_histogram_stat = test_ds.get_index_statistic(
        RowRangeHistogramStatistic.STATS_TYPE,
        TEST_INDEX,
        TEST_TAG)
    assert isinstance(row_range_histogram_stat, RowRangeHistogramStatistic)
    assert row_range_histogram_stat.get_tag() == TEST_TAG
    assert row_range_histogram_stat.get_index_name() == TEST_INDEX
    assert row_range_histogram_stat.get_description() is not None
    assert row_range_histogram_stat.get_binning_strategy() is None
    assert isinstance(row_range_histogram_stat.get_statistic_type(), IndexStatisticType)
    assert row_range_histogram_stat.get_statistic_type().get_string() == 'ROW_RANGE_HISTOGRAM'
    histogram = test_ds.get_statistic_value(row_range_histogram_stat)
    assert isinstance(histogram, NumericHistogram)
    assert histogram.get_min_value() == float('inf')
    assert histogram.get_max_value() == float('-inf')
    assert histogram.sum(histogram.quantile(1.0)) == 360
    assert histogram.get_total_count() == 360

    # test alternate constructors
    row_range_histogram_stat = RowRangeHistogramStatistic(TEST_INDEX)
    row_range_histogram_stat.set_internal()
    assert row_range_histogram_stat.get_index_name() == TEST_INDEX
    assert row_range_histogram_stat.get_tag() == INTERNAL_TAG


def test_composite_binning_strategy(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    binning_strategy = CompositeBinningStrategy([
        FieldValueBinningStrategy([POINT_COLOR_FIELD]),
        FieldValueBinningStrategy([POINT_SHAPE_FIELD]),
    ])
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    binning_strategy = count_stat.get_binning_strategy()
    assert isinstance(binning_strategy, CompositeBinningStrategy)
    assert binning_strategy.get_strategy_name() == 'COMPOSITE'
    assert binning_strategy.get_description() is not None
    assert test_ds.get_statistic_value(count_stat) == 360
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat))
    # There should be one bin for every color/shape combination
    assert len(binned_values) == 12
    for b, v in binned_values:
        assert isinstance(b, bytes)
        # colors and shapes are evenly distributed, so each bin should have 30
        assert v == 30


def test_data_type_binning_strategy(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    binning_strategy = DataTypeBinningStrategy()
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    binning_strategy = count_stat.get_binning_strategy()
    assert isinstance(binning_strategy, DataTypeBinningStrategy)
    assert binning_strategy.get_strategy_name() == 'DATA_TYPE'
    assert binning_strategy.get_description() is not None
    assert test_ds.get_statistic_value(count_stat) == 360
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat))
    # The only bin is for our point adapter
    assert len(binned_values) == 1
    assert binning_strategy.bin_to_string(binned_values[0][0]) == POINT_TYPE_NAME
    assert binned_values[0][1] == 360

    # test bin constraint
    bin_constraint = BinConstraints.of(POINT_TYPE_NAME.encode())
    assert isinstance(bin_constraint, BinConstraints)
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat, bin_constraint))
    # There should be one bin for every 5 values
    assert len(binned_values) == 1
    assert binning_strategy.bin_to_string(binned_values[0][0]) == POINT_TYPE_NAME
    assert binned_values[0][1] == 360


def test_field_value_binning_strategy(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    binning_strategy = FieldValueBinningStrategy([POINT_COLOR_FIELD, POINT_SHAPE_FIELD])
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    binning_strategy = count_stat.get_binning_strategy()
    assert isinstance(binning_strategy, FieldValueBinningStrategy)
    assert binning_strategy.get_strategy_name() == 'FIELD_VALUE'
    assert binning_strategy.get_description() is not None
    assert test_ds.get_statistic_value(count_stat) == 360
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat))
    # There should be one bin for every color/shape combination
    assert len(binned_values) == 12
    for b, v in binned_values:
        assert isinstance(b, bytes)
        # colors and shapes are evenly distributed, so each bin should have 30
        assert v == 30

    # test bin prefix constraint
    bin_constraint = BinConstraints.of_prefix(b'BLUE')
    assert isinstance(bin_constraint, BinConstraints)
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat, bin_constraint))
    # There should be 4 bins, one for each shape
    assert len(binned_values) == 4
    for b, v in binned_values:
        assert isinstance(b, bytes)
        # each bin should have 30
        assert v == 30


def test_numeric_range_field_value_binning_strategy(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    binning_strategy = NumericRangeFieldValueBinningStrategy(fields=[POINT_NUMBER_FIELD], interval=5)
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    binning_strategy = count_stat.get_binning_strategy()
    assert isinstance(binning_strategy, NumericRangeFieldValueBinningStrategy)
    assert binning_strategy.get_strategy_name() == 'NUMERIC_RANGE'
    assert binning_strategy.get_description() is not None
    assert test_ds.get_statistic_value(count_stat) == 360
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat))
    # There should be one bin for every 5 values
    assert len(binned_values) == 72
    for b, v in binned_values:
        assert isinstance(b, bytes)
        assert v == 5

    # test numeric range constraint
    bin_constraint = BinConstraints.of_object(Range(0, 180))
    assert isinstance(bin_constraint, BinConstraints)
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat, bin_constraint))
    # There should be one bin for every 5 values
    assert len(binned_values) == 36
    for b, v in binned_values:
        assert isinstance(b, bytes)
        assert v == 5


def test_time_range_field_value_binning_strategy(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    binning_strategy = TimeRangeFieldValueBinningStrategy(fields=[POINT_TIME_FIELD], periodicity='minute')
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    binning_strategy = count_stat.get_binning_strategy()
    assert isinstance(binning_strategy, TimeRangeFieldValueBinningStrategy)
    assert binning_strategy.get_strategy_name() == 'TIME_RANGE'
    assert binning_strategy.get_description() is not None
    assert test_ds.get_statistic_value(count_stat) == 360
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat))
    # Each value is 1 second apart, so there should be 6 bins with a periodicity of minute
    assert len(binned_values) == 6
    for b, v in binned_values:
        assert isinstance(b, bytes)
        assert v == 60

    # test time range constraint
    bin_constraint = BinConstraints.of_object(Interval(datetime.utcfromtimestamp(0), datetime.utcfromtimestamp(180)))
    assert isinstance(bin_constraint, BinConstraints)
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat, bin_constraint))
    # Each value is 1 second apart, so there should be 3 bins with a periodicity of minute
    assert len(binned_values) == 3
    for b, v in binned_values:
        assert isinstance(b, bytes)
        assert v == 60


def test_spatial_field_value_binning_strategy(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    binning_strategy = SpatialFieldValueBinningStrategy(fields=[POINT_GEOMETRY_FIELD], type='GEOHASH', precision=1)
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data_offset(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    binning_strategy = count_stat.get_binning_strategy()
    assert isinstance(binning_strategy, SpatialFieldValueBinningStrategy)
    assert binning_strategy.get_strategy_name() == 'SPATIAL'
    assert binning_strategy.get_description() is not None
    assert test_ds.get_statistic_value(count_stat) == 360
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat))
    assert len(binned_values) == 8
    for b, v in binned_values:
        assert isinstance(b, bytes)
        assert v == 45

    # test polygon constraint
    bin_constraint = BinConstraints.of_object(Polygon([[0.5, 0.5], [0.5, 45.5], [45.5, 45.5], [45.5, 0.5], [0.5, 0.5]]))
    assert isinstance(bin_constraint, BinConstraints)
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat, bin_constraint))
    # There is a bin every 45 degrees so there should be 2 bins
    assert len(binned_values) == 2
    for b, v in binned_values:
        assert isinstance(b, bytes)
        assert v == 45

    bin_constraint = BinConstraints.of_object(Envelope(min_x=1, min_y=1, max_x=91, max_y=90))
    assert isinstance(bin_constraint, BinConstraints)
    assert test_ds.get_statistic_value(count_stat, bin_constraint) == 135



def test_partition_binning_strategy(test_ds):
    # given
    index = SpatialIndexBuilder().set_name(TEST_INDEX).create_index()
    adapter = POINT_TYPE_ADAPTER
    test_ds.add_type(adapter, index)

    # when
    binning_strategy = PartitionBinningStrategy()
    count_stat = CountStatistic()
    count_stat.set_tag(TEST_TAG)
    count_stat.set_type_name(POINT_TYPE_NAME)
    count_stat.set_binning_strategy(binning_strategy)
    test_ds.add_statistic(count_stat)
    write_test_data(test_ds, index)

    # then
    count_stat = test_ds.get_data_type_statistic(CountStatistic.STATS_TYPE, POINT_TYPE_NAME, TEST_TAG)
    binning_strategy = count_stat.get_binning_strategy()
    assert isinstance(binning_strategy, PartitionBinningStrategy)
    assert binning_strategy.get_strategy_name() == 'PARTITION'
    assert binning_strategy.get_description() is not None
    assert test_ds.get_statistic_value(count_stat) == 360
    binned_values = results_as_list(test_ds.get_binned_statistic_values(count_stat))
    # No real assumptions can be made about how the data is partitioned, but there will be at least 1 and all values
    # should sum to 360
    assert len(binned_values) >= 1
    assert reduce(lambda a, b: a + b, map(lambda a: a[1], binned_values)) == 360
