#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from .statistic import Statistic
from .data_type.count_statistic import CountStatistic
from .field.bloom_filter_statistic import BloomFilterStatistic
from .field.bounding_box_statistic import BoundingBoxStatistic
from .field.count_min_sketch_statistic import CountMinSketchStatistic
from .field.fixed_bin_numeric_histogram_statistic import FixedBinNumericHistogramStatistic
from .field.hyper_log_log_statistic import HyperLogLogStatistic
from .field.numeric_histogram_statistic import NumericHistogramStatistic
from .field.numeric_mean_statistic import NumericMeanStatistic
from .field.numeric_range_statistic import NumericRangeStatistic
from .field.numeric_stats_statistic import NumericStatsStatistic
from .field.time_range_statistic import TimeRangeStatistic
from .index.differing_visibility_count_statistic import DifferingVisibilityCountStatistic
from .index.duplicate_entry_count_statistic import DuplicateEntryCountStatistic
from .index.field_visibility_count_statistic import FieldVisibilityCountStatistic
from .index.index_meta_data_set_statistic import IndexMetaDataSetStatistic
from .index.max_duplicates_statistic import MaxDuplicatesStatistic
from .index.partitions_statistic import PartitionsStatistic
from .index.row_range_histogram_statistic import RowRangeHistogramStatistic


__statistic_mappings = {
    CountStatistic.STATS_TYPE.get_string(): CountStatistic,
    BloomFilterStatistic.STATS_TYPE.get_string(): BloomFilterStatistic,
    BoundingBoxStatistic.STATS_TYPE.get_string(): BoundingBoxStatistic,
    CountMinSketchStatistic.STATS_TYPE.get_string(): CountMinSketchStatistic,
    FixedBinNumericHistogramStatistic.STATS_TYPE.get_string(): FixedBinNumericHistogramStatistic,
    HyperLogLogStatistic.STATS_TYPE.get_string(): HyperLogLogStatistic,
    NumericHistogramStatistic.STATS_TYPE.get_string(): NumericHistogramStatistic,
    NumericMeanStatistic.STATS_TYPE.get_string(): NumericMeanStatistic,
    NumericRangeStatistic.STATS_TYPE.get_string(): NumericRangeStatistic,
    NumericStatsStatistic.STATS_TYPE.get_string(): NumericStatsStatistic,
    TimeRangeStatistic.STATS_TYPE.get_string(): TimeRangeStatistic,
    DifferingVisibilityCountStatistic.STATS_TYPE.get_string(): DifferingVisibilityCountStatistic,
    DuplicateEntryCountStatistic.STATS_TYPE.get_string(): DuplicateEntryCountStatistic,
    FieldVisibilityCountStatistic.STATS_TYPE.get_string(): FieldVisibilityCountStatistic,
    IndexMetaDataSetStatistic.STATS_TYPE.get_string(): IndexMetaDataSetStatistic,
    MaxDuplicatesStatistic.STATS_TYPE.get_string(): MaxDuplicatesStatistic,
    PartitionsStatistic.STATS_TYPE.get_string(): PartitionsStatistic,
    RowRangeHistogramStatistic.STATS_TYPE.get_string(): RowRangeHistogramStatistic,
}


def map_statistic(j_statistic):
    if j_statistic is None:
        return None
    stat_type = j_statistic.getStatisticType().getString()
    if stat_type in __statistic_mappings:
        return __statistic_mappings[stat_type](java_ref=j_statistic)
    return Statistic(j_statistic)
