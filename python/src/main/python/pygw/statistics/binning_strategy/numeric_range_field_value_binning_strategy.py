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


class NumericRangeFieldValueBinningStrategy(FieldValueBinningStrategy):
    """
    Statistic binning strategy that bins statistic values by the numeric representation of the value of a given field.
    By default it will truncate decimal places and will bin by the integer. However, an "offset" and "interval" can be
    provided to bin numbers at any regular step-sized increment from an origin value. A statistic using this binning
    strategy can be constrained using numeric ranges (A Range can be used as a constraint).
    """

    def __init__(self, fields=None, interval=1, offset=0, java_ref=None):
        if java_ref is None:
            java_ref = geowave_pkg.core.store.statistics.binning.NumericRangeFieldValueBinningStrategy(
                float(interval), float(offset), StringArrayType().to_java(fields))
        super().__init__(fields, java_ref)
