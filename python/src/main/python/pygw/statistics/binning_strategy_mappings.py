#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.config import geowave_pkg
from .statistic_binning_strategy import StatisticBinningStrategy
from .binning_strategy.composite_binning_strategy import CompositeBinningStrategy
from .binning_strategy.data_type_binning_strategy import DataTypeBinningStrategy
from .binning_strategy.field_value_binning_strategy import FieldValueBinningStrategy
from .binning_strategy.numeric_range_field_value_binning_strategy import NumericRangeFieldValueBinningStrategy
from .binning_strategy.partition_binning_strategy import PartitionBinningStrategy
from .binning_strategy.spatial_field_value_binning_strategy import SpatialFieldValueBinningStrategy
from .binning_strategy.time_range_field_value_binning_strategy import TimeRangeFieldValueBinningStrategy

__binning_strategy_mappings = {
    geowave_pkg.core.store.statistics.binning.CompositeBinningStrategy.NAME:
        CompositeBinningStrategy,
    geowave_pkg.core.store.statistics.binning.DataTypeBinningStrategy.NAME:
        DataTypeBinningStrategy,
    geowave_pkg.core.store.statistics.binning.PartitionBinningStrategy.NAME:
        PartitionBinningStrategy,
    geowave_pkg.core.store.statistics.binning.NumericRangeFieldValueBinningStrategy.NAME:
        NumericRangeFieldValueBinningStrategy,
    geowave_pkg.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy.NAME:
        SpatialFieldValueBinningStrategy,
    geowave_pkg.core.geotime.store.statistics.binning.TimeRangeFieldValueBinningStrategy.NAME:
        TimeRangeFieldValueBinningStrategy,
    geowave_pkg.core.store.statistics.binning.FieldValueBinningStrategy.NAME:
        FieldValueBinningStrategy,
}


def map_binning_strategy(j_binning_strategy):
    if j_binning_strategy is None:
        return None
    strategy_name = j_binning_strategy.getStrategyName()
    if strategy_name in __binning_strategy_mappings:
        return __binning_strategy_mappings[strategy_name](java_ref=j_binning_strategy)
    return StatisticBinningStrategy(j_binning_strategy)
