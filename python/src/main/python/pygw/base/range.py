#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from .geowave_object import GeoWaveObject
from .java_transformer import JavaTransformer
from ..config import java_pkg


class Range(GeoWaveObject):
    def __init__(self, minimum=0, maximum=0, java_ref=None):
        if java_ref is None:
            java_ref = java_pkg.org.apache.commons.lang3.Range.between(float(minimum), float(maximum))
        super().__init__(java_ref)

    def get_minimum(self):
        return self._java_ref.getMinimum()

    def get_maximum(self):
        return self._java_ref.getMaximum()


class RangeTransformer(JavaTransformer):
    """
    Transforms Java Ranges into a pygw Range.
    """

    def transform(self, j_object):
        """
        Transform the given Java Range into a tuple.

        Args:
            j_object (Java Range): A Range Java object.
        Returns:
            A pygw implementation of Range.
        """
        return Range(java_ref=j_object)
