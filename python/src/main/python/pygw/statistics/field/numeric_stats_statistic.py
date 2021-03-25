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


class NumericStatsStatistic(FieldStatistic):
    """
    Tracks the min, max, count, mean, sum, variance and standard deviation of a numeric attribute.
    """
    STATS_TYPE = FieldStatisticType(geowave_pkg.core.store.statistics.field.NumericStatsStatistic.STATS_TYPE)

    def __init__(self, type_name=None, field_name=None, java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.store.statistics.field.NumericStatsStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.field.NumericStatsStatistic(type_name, field_name)
        super().__init__(java_ref, StatsTransformer())


class StatsTransformer(JavaTransformer):
    def transform(self, j_object):
        return Stats(j_object)


class Stats(GeoWaveObject):
    def count(self):
        return self._java_ref.count()

    def mean(self):
        return self._java_ref.mean()

    def sum(self):
        return self._java_ref.sum()

    def population_variance(self):
        return self._java_ref.populationVariance()

    def population_standard_deviation(self):
        return self._java_ref.populationStandardDeviation()

    def sample_variance(self):
        return self._java_ref.sampleVariance()

    def sample_standard_deviation(self):
        return self._java_ref.sampleStandardDeviation()

    def min(self):
        return self._java_ref.min()

    def max(self):
        return self._java_ref.max()
