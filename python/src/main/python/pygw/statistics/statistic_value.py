#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base import GeoWaveObject
from .statistic_mappings import map_statistic
from ..base.java_transformer import JavaTransformer


class StatisticValue(GeoWaveObject):
    """
    Base GeoWave statistic value.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)

    def get_statistic(self):
        """
        Get the parent statistic.

        Returns:
            The statistic associated with this value
        """
        return map_statistic(self._java_ref.getStatistic())

    def get_bin(self):
        """
        Gets the bin for this value. If the underlying statistic does not use a binning strategy, an empty byte array
        will be returned.

        Returns:
            The bin for this value.
        """
        return self._java_ref.getBin()

    def get_value(self):
        """
        Gets the raw statistic value.

        Returns:
            The raw statistic value.
        """
        statistic = self.get_statistic()
        return statistic.java_transformer.transform(self._java_ref.getValue())

    def merge(self, other):
        """
        Merge another statistic value of the same type into this statistic value.

        Args:
            other (StatisticValue): The other value to merge into this one.
        """
        self._java_ref.merge(other.java_ref())


class StatisticValueTransformer(JavaTransformer):
    def transform(self, j_object):
        return StatisticValue(j_object)
