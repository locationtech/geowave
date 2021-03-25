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


class Envelope(GeoWaveObject):
    def __init__(self, min_x=0, min_y=0, max_x=0, max_y=0, java_ref=None):
        if java_ref is None:
            java_ref = java_pkg.org.locationtech.jts.geom.Envelope(
                float(min_x), float(max_x), float(min_y), float(max_y))
        super().__init__(java_ref)

    def get_min_x(self):
        return self._java_ref.getMinX()

    def get_max_x(self):
        return self._java_ref.getMaxX()

    def get_min_y(self):
        return self._java_ref.getMinY()

    def get_max_y(self):
        return self._java_ref.getMaxY()


class EnvelopeTransformer(JavaTransformer):
    """
    Transforms Java Envelopes into a pygw Envelope.
    """

    def transform(self, j_object):
        """
        Transform the given Java Envelope into a pygw Envelope.

        Args:
            j_object (Java Envelope): An Envelope Java object.
        Returns:
            A pygw implementation of Envelope.
        """
        return Envelope(java_ref=j_object)
