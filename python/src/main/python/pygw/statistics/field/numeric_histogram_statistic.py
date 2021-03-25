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


class NumericHistogramStatistic(FieldStatistic):
    """
    Dynamic histogram provide very high accuracy for CDF and quantiles over the a numeric attribute.
    """
    STATS_TYPE = FieldStatisticType(geowave_pkg.core.store.statistics.field.NumericHistogramStatistic.STATS_TYPE)

    def __init__(self, type_name=None, field_name=None, compression=100, java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.store.statistics.field.NumericHistogramStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.field.NumericHistogramStatistic(
                    type_name, field_name, float(compression))
        super().__init__(java_ref, NumericHistogramTransformer())

    def set_compression(self, compression):
        self._java_ref.setCompression(float(compression))

    def get_compression(self):
        return self._java_ref.getCompression()


class NumericHistogramTransformer(JavaTransformer):
    def transform(self, j_object):
        return NumericHistogram(j_object)


class NumericHistogram(GeoWaveObject):
    def quantile(self, value):
        return self._java_ref.quantile(float(value))

    def cdf(self, value):
        return self._java_ref.cdf(float(value))

    def sum(self, value, inclusive=True):
        return self._java_ref.sum(float(value), inclusive)

    def get_min_value(self):
        return self._java_ref.getMinValue()

    def get_max_value(self):
        return self._java_ref.getMaxValue()

    def get_total_count(self):
        return self._java_ref.getTotalCount()
