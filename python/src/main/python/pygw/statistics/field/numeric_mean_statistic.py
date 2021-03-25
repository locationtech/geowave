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


class NumericMeanStatistic(FieldStatistic):
    """
    Computes the mean of a numeric field.
    """
    STATS_TYPE = FieldStatisticType(geowave_pkg.core.store.statistics.field.NumericMeanStatistic.STATS_TYPE)

    def __init__(self, type_name=None, field_name=None, java_ref=None):
        if java_ref is None:
            if type_name is None and field_name is None:
                java_ref = geowave_pkg.core.store.statistics.field.NumericMeanStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.field.NumericMeanStatistic(type_name, field_name)
        super().__init__(java_ref)
