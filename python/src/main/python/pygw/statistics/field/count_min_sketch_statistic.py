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


class CountMinSketchStatistic(FieldStatistic):
    """
    Maintains an estimate of how may of each attribute value occurs in a set of data.
    """
    STATS_TYPE = FieldStatisticType(geowave_pkg.core.store.statistics.field.CountMinSketchStatistic.STATS_TYPE)

    def __init__(
            self,
            type_name=None,
            field_name=None,
            error_factor=0.001,
            probability_of_correctness=0.98,
            java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.store.statistics.field.CountMinSketchStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.field.CountMinSketchStatistic(
                    type_name, field_name, float(error_factor), float(probability_of_correctness))
        super().__init__(java_ref, CountMinSketchTransformer())

    def set_error_factor(self, error_factor):
        self._java_ref.setErrorFactor(float(error_factor))

    def get_error_factor(self):
        return self._java_ref.getErrorFactor()

    def set_probability_of_correctness(self, probability_of_correctness):
        self._java_ref.setProbabilityOfCorrectness(float(probability_of_correctness))

    def get_probability_of_correctness(self):
        return self._java_ref.getProbabilityOfCorrectness()


class CountMinSketchTransformer(JavaTransformer):
    def transform(self, j_object):
        return CountMinSketch(j_object)


class CountMinSketch(GeoWaveObject):
    def get_relative_error(self):
        return self._java_ref.getRelativeError()

    def get_confidence(self):
        return self._java_ref.getConfidence()

    def estimate_count(self, item):
        return self._java_ref.estimateCount(item)
