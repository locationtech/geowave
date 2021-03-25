#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base.type_conversions import StringArrayType
from pygw.config import geowave_pkg
from .field_value_binning_strategy import FieldValueBinningStrategy


class TimeRangeFieldValueBinningStrategy(FieldValueBinningStrategy):
    """
    Statistic binning strategy that bins statistic values by the temporal representation of the value of a given field.
    It bins time values by a temporal periodicity (any time unit), default to daily. A statistic using this binning
    strategy can be constrained using an Interval as a constraint).
    """

    def __init__(self, fields=None, time_zone="GMT", periodicity="day", java_ref=None):
        if java_ref is None:
            j_periodicity = geowave_pkg.core.geotime.index.dimension.TemporalBinningStrategy.Unit.fromString(
                periodicity)
            java_ref = geowave_pkg.core.geotime.store.statistics.binning.TimeRangeFieldValueBinningStrategy(
                time_zone, j_periodicity, StringArrayType().to_java(fields))
        super().__init__(fields, java_ref)
