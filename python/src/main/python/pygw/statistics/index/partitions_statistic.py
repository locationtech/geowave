#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.config import geowave_pkg
from ..statistic import IndexStatistic
from ..statistic_type import IndexStatisticType
from ...base.java_transformer import JavaTransformer
from ...base.type_conversions import PrimitiveByteArrayType


class PartitionsStatistic(IndexStatistic):
    """
    This class is responsible for maintaining all unique Partition IDs that are being used within a data set.
    """
    STATS_TYPE = IndexStatisticType(geowave_pkg.core.store.statistics.index.PartitionsStatistic.STATS_TYPE)

    def __init__(self, index_name=None, java_ref=None):
        if java_ref is None:
            if index_name is None:
                java_ref = geowave_pkg.core.store.statistics.index.PartitionsStatistic()
            else:
                java_ref = geowave_pkg.core.store.statistics.index.PartitionsStatistic(index_name)
        super().__init__(java_ref, PartitionsTransformer())


class PartitionsTransformer(JavaTransformer):
    def transform(self, j_object):
        partitions = set()
        byte_array_type = PrimitiveByteArrayType()
        for item in j_object:
            partitions.add(byte_array_type.from_java(item.getBytes()))
        return partitions
