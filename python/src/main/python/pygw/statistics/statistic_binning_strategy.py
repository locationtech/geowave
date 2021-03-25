#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base import GeoWaveObject
from ..config import geowave_pkg


class StatisticBinningStrategy(GeoWaveObject):
    """
    Base statistic binning strategy class.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)

    def get_strategy_name(self):
        """
        Gets the name of the binning strategy.

        Returns:
            The name of the binning strategy.
        """
        return self._java_ref.getStrategyName()

    def get_description(self):
        """
        Gets a description of the binning strategy.

        Returns:
            A description of the binning strategy.
        """
        return self._java_ref.getDescription()

    def bin_to_string(self, stat_bin):
        """
        Convert a bin to a readable string.

        Args:
            stat_bin (bytes): The bin to convert to string.
        Returns:
            A string that represents the bin.
        """
        j_byte_array = geowave_pkg.core.index.ByteArray(stat_bin)
        return self._java_ref.binToString(j_byte_array)
