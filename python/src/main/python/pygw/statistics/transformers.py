#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base.java_transformer import JavaTransformer
from pygw.base.type_conversions import PrimitiveByteArrayType


class BinnedStatisticTransformer(JavaTransformer):
    """
    Transforms a binned statistic value into a tuple.
    """

    def __init__(self, statistic_value_transformer):
        self._statistic_value_transformer = statistic_value_transformer
        self._byte_array_type = PrimitiveByteArrayType()
        super().__init__()

    def transform(self, j_object):
        """
        Transform the given bin/value pair into a tuple.

        Args:
            j_object (Java Pair): The bin/value pair.
        Returns:
            A tuple of bin bytes and statistic value.
        """
        return self._byte_array_type.from_java(j_object.getKey().getBytes()),\
            self._statistic_value_transformer.transform(j_object.getValue())
