#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg

from ..query_builder import QueryBuilder

class VectorStatisticsQueryBuilder(QueryBuilder):
    """
    A builder for creating statistics queries for vector data.
    """

    def __init__(self):
        j_stats_qbuilder = geowave_pkg.core.geotime.store.query.api.VectorStatisticsQueryBuilder.newBuilder()
        super().__init__(j_stats_qbuilder)

    def bbox(self):
        # TODO: support FeatureBoundingBoxStatistics builder
        return self._java_ref.bbox()

    def time_range(self):
        # TODO: support FeatureTimeRangeStatistics builder
        return self._java_ref.timeRange()
