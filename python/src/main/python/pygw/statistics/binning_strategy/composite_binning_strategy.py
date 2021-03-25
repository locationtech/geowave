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
from ..statistic_binning_strategy import StatisticBinningStrategy


class CompositeBinningStrategy(StatisticBinningStrategy):
    """
    Statistic binning strategy that combines two or more other binning strategies.
    """

    def __init__(self, binning_strategies=None, java_ref=None):
        if java_ref is None:
            j_binning_strategies = GeoWaveObject.to_java_array(
                geowave_pkg.core.store.api.StatisticBinningStrategy, binning_strategies)
            java_ref = geowave_pkg.core.store.statistics.binning.CompositeBinningStrategy(j_binning_strategies)
        super().__init__(java_ref)
