#
# Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg

from ..query_builder import QueryBuilder
from .noop_transformer import NoOpTransformer

class VectorStatisticsQueryBuilder(QueryBuilder):
    """
    A builder for creating statistics queries for vector data.
    """

    def __init__(self):
        j_stats_qbuilder = geowave_pkg.core.geotime.store.query.api.VectorStatisticsQueryBuilder.newBuilder().factory()
        # this will at least work perfectly fine for counts, ideally other results get transformed to more appropriate python objects
        super().__init__(j_stats_qbuilder, NoOpTransformer())

    def bbox(self):
        # TODO: support FeatureBoundingBoxStatistics builder
        return self._java_ref.bbox().build()

    def time_range(self):
        # TODO: support FeatureTimeRangeStatistics builder
        return self._java_ref.timeRange().build()

    def count(self):
        return self._java_ref.count().build()

    def rowHistogram(self):
        return self._java_ref.rowHistogram().build()

    def partitions(self):
        return self._java_ref.partitions().build()
