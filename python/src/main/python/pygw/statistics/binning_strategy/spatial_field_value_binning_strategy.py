#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base.type_conversions import StringArrayType
from pygw.config import geowave_pkg
from .field_value_binning_strategy import FieldValueBinningStrategy


class SpatialFieldValueBinningStrategy(FieldValueBinningStrategy):
    """
    Statistic binning strategy that bins statistic values by the spatial representation of the value of a given field.
    It bins spatial values by a hashing function, default to Google's S2. A statistic using this binning
    strategy can be constrained using a geometry or envelope as a constraint).
    """

    def __init__(self, fields=None, type="S2", precision=8, complex_geometry="USE_CENTROID_ONLY", java_ref=None):
        if java_ref is None:
            j_type = geowave_pkg.core.geotime.binning.SpatialBinningType.fromString(
                type)
            j_complex_geometry = geowave_pkg.core.geotime.binning.ComplexGeometryBinningOption.fromString(
                complex_geometry)
            java_ref = geowave_pkg.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy(
                j_type, precision, j_complex_geometry, StringArrayType().to_java(fields))
        super().__init__(fields, java_ref)
