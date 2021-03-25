#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.config import geowave_pkg
from ..statistic import FieldStatistic
from ..statistic_type import FieldStatisticType
from ...base import GeoWaveObject
from ...base.java_transformer import JavaTransformer
from ...base.type_conversions import PrimitiveDoubleArrayType, PrimitiveLongArrayType


class FixedBinNumericHistogramStatistic(FieldStatistic):
    """
    Fixed number of bins for a histogram. Unless configured, the range will expand dynamically,  redistributing the
    data as necessary into the wider bins.

    The advantage of constraining the range of the statistic is to ignore values outside the range, such as erroneous
    values. Erroneous values force extremes in the histogram. For example, if the expected range of values falls
    between 0 and 1 and a value of 10000 occurs, then a single bin contains the entire population between 0 and 1, a
    single bin represents the single value of 10000.  If there are extremes in the data, then use
    NumericHistogramStatistic instead.
    """
    STATS_TYPE = FieldStatisticType(
        geowave_pkg.core.store.statistics.field.FixedBinNumericHistogramStatistic.STATS_TYPE)

    def __init__(self, type_name=None, field_name=None, bins=1024, min_value=None, max_value=None, java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.store.statistics.field.FixedBinNumericHistogramStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.field.FixedBinNumericHistogramStatistic(
                    type_name, field_name, bins, float(min_value), float(max_value))
        super().__init__(java_ref, FixedBinNumericHistogramTransformer())

    def set_num_bins(self, num_bins):
        self._java_ref.setNumBins(num_bins)

    def get_num_bins(self):
        return self._java_ref.getNumBins()

    def set_min_value(self, min_value):
        self._java_ref.setMinValue(float(min_value))

    def get_min_value(self):
        return self._java_ref.getMinValue()

    def set_max_value(self, max_value):
        self._java_ref.setMaxValue(float(max_value))

    def get_max_value(self):
        return self._java_ref.getMaxValue()


class FixedBinNumericHistogramTransformer(JavaTransformer):
    def transform(self, j_object):
        return FixedBinNumericHistogram(j_object)


class FixedBinNumericHistogram(GeoWaveObject):
    def bin_quantiles(self, bins):
        return PrimitiveDoubleArrayType().from_java(self._java_ref.quantile(int(bins)))

    def quantile(self, percentage):
        return self._java_ref.quantile(float(percentage))

    def cdf(self, val):
        return self._java_ref.cdf(float(val))

    def sum(self, val, inclusive=True):
        return self._java_ref.sum(float(val), inclusive)

    def percent_population_over_range(self, start, stop):
        return self._java_ref.percentPopulationOverRange(float(start), float(stop))

    def total_sample_size(self):
        return self._java_ref.totalSampleSize()

    def count(self, bins):
        return PrimitiveLongArrayType().from_java(self._java_ref.count(bins))

    def get_total_count(self):
        return self._java_ref.getTotalCount()

    def get_num_bins(self):
        return self._java_ref.getNumBins()

    def get_min_value(self):
        return self._java_ref.getMinValue()

    def get_max_value(self):
        return self._java_ref.getMaxValue()
