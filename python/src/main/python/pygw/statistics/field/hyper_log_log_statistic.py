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


class HyperLogLogStatistic(FieldStatistic):
    """
    Provides an estimated cardinality of the number of unique values for an attribute.
    """
    STATS_TYPE = FieldStatisticType(geowave_pkg.core.store.statistics.field.HyperLogLogStatistic.STATS_TYPE)

    def __init__(self, type_name=None, field_name=None, precision=16, java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.store.statistics.field.HyperLogLogStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.field.HyperLogLogStatistic(type_name, field_name, precision)
        super().__init__(java_ref, HyperLogLogTransformer())

    def set_precision(self, precision):
        self._java_ref.setPrecision(precision)

    def get_precision(self):
        return self._java_ref.getPrecision()


class HyperLogLogTransformer(JavaTransformer):
    def transform(self, j_object):
        return HyperLogLogPlus(j_object)


class HyperLogLogPlus(GeoWaveObject):
    def cardinality(self):
        return self._java_ref.cardinality()
