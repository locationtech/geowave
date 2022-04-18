#
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.config import geowave_pkg
from ..statistic import FieldStatistic
from ..statistic_type import FieldStatisticType
from ...base.interval import IntervalTransformer


class TimeRangeStatistic(FieldStatistic):
    """
    Tracks the time range of a temporal field.
    """
    STATS_TYPE = FieldStatisticType(geowave_pkg.core.geotime.store.statistics.TimeRangeStatistic.STATS_TYPE)

    def __init__(self, type_name=None, field_name=None, java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.geotime.store.statistics.TimeRangeStatistic()
            else:
                java_ref = geowave_pkg.core.geotime.store.statistics.TimeRangeStatistic(type_name, field_name)
        super().__init__(java_ref, IntervalTransformer())
