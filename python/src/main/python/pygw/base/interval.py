#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from datetime import datetime, timezone

from .geowave_object import GeoWaveObject
from .java_transformer import JavaTransformer
from ..config import java_pkg


class Interval(GeoWaveObject):
    def __init__(self, start=datetime.now(), end=datetime.now(), java_ref=None):
        if java_ref is None:
            j_start = java_pkg.java.time.Instant.ofEpochSecond(int(start.replace(tzinfo=timezone.utc).timestamp()))
            j_end = java_pkg.java.time.Instant.ofEpochSecond(int(end.replace(tzinfo=timezone.utc).timestamp()))
            java_ref = java_pkg.org.threeten.extra.Interval.of(j_start, j_end)
        super().__init__(java_ref)

    def get_start(self):
        return datetime.utcfromtimestamp(self._java_ref.getStart().getEpochSecond())

    def get_end(self):
        return datetime.utcfromtimestamp(self._java_ref.getEnd().getEpochSecond())


class IntervalTransformer(JavaTransformer):
    """
    Transforms Java Intervals into a pygw Interval.
    """

    def transform(self, j_object):
        """
        Transform the given Java Interval into a pygw Interval.

        Args:
            j_object (Java Interval): An Interval Java object.
        Returns:
            A pygw implementation of Interval.
        """
        return Interval(java_ref=j_object)
