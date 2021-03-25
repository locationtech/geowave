#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.config import geowave_pkg
from ..field.numeric_histogram_statistic import NumericHistogramTransformer
from ..statistic import IndexStatistic
from ..statistic_type import IndexStatisticType


class RowRangeHistogramStatistic(IndexStatistic):
    """
    Provides a histogram of the row ranges used by the given index.
    """
    STATS_TYPE = IndexStatisticType(geowave_pkg.core.store.statistics.index.RowRangeHistogramStatistic.STATS_TYPE)

    def __init__(self, index_name=None, java_ref=None):
        if java_ref is None:
            if index_name is None:
                java_ref = geowave_pkg.core.store.statistics.index.RowRangeHistogramStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.index.RowRangeHistogramStatistic(index_name)
        super().__init__(java_ref, NumericHistogramTransformer())
