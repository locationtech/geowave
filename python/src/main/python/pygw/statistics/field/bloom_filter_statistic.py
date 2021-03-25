#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base import GeoWaveObject
from pygw.config import geowave_pkg
from ..statistic import FieldStatistic
from ..statistic_type import FieldStatisticType
from ...base.java_transformer import JavaTransformer


class BloomFilterStatistic(FieldStatistic):
    """
    Applies a bloom filter to field values useful for quickly determining set membership. False positives are possible
    but false negatives are not possible. In other words, a value can be determined to be possibly in the set or
    definitely not in the set.
    """
    STATS_TYPE = FieldStatisticType(geowave_pkg.core.store.statistics.field.BloomFilterStatistic.STATS_TYPE)

    def __init__(self, type_name=None, field_name=None, java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.store.statistics.field.BloomFilterStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.field.BloomFilterStatistic(type_name, field_name)
        super().__init__(java_ref, BloomFilterTransformer())

    def set_expected_insertions(self, expected_insertions):
        self._java_ref.setExpectedInsertions(int(expected_insertions))

    def get_expected_insertions(self):
        return self._java_ref.getExpectedInsertions()

    def set_desired_false_positive_probability(self, probability):
        self._java_ref.setDesiredFalsePositiveProbability(float(probability))

    def get_desired_false_positive_probability(self):
        return self._java_ref.getDesiredFalsePositiveProbability()


class BloomFilterTransformer(JavaTransformer):
    def transform(self, j_object):
        return BloomFilter(j_object)


class BloomFilter(GeoWaveObject):
    def might_contain(self, value):
        return self._java_ref.mightContain(value)

    def expected_fpp(self):
        return self._java_ref.expectedFpp()

    def approximate_element_count(self):
        return self._java_ref.approximateElementCount()
