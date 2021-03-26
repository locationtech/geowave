#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base import GeoWaveObject
from pygw.base.type_conversions import PrimitiveByteArrayType, GeometryType
from pygw.config import geowave_pkg, java_gateway
from shapely.geometry.base import BaseGeometry


class BinConstraints(GeoWaveObject):
    """
    Bin constraints for statistic queries.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)

    @staticmethod
    def _convert_byte_arrays(*byte_arrays):
        n = len(byte_arrays)
        j_arr = java_gateway.new_array(geowave_pkg.core.index.ByteArray, n)
        byte_array_type = PrimitiveByteArrayType()
        for idx, byte_array in enumerate(byte_arrays):
            j_arr[idx] = geowave_pkg.core.index.ByteArray(byte_array_type.to_java(byte_array))

        return j_arr

    @staticmethod
    def all_bins():
        """
        Unconstrained, a query will return all of the bins.

        Returns:
            BinConstraints that represent all bins.
        """
        j_constraints = geowave_pkg.core.store.api.BinConstraints.allBins()
        return BinConstraints(j_constraints)

    @staticmethod
    def of(*exact_match_bins):
        """
        Sets the bins of the query explicitly. If a queried statistic uses a binning strategy, only values contained in
        one of the given bins will be return.

        Args:
            exact_match_bins (bytes): The bins to match.
        Returns:
            A bin constraint representing exact matches of the provided bins.
        """
        j_constraints = geowave_pkg.core.store.api.BinConstraints.of(
            BinConstraints._convert_byte_arrays(*exact_match_bins))
        return BinConstraints(j_constraints)

    @staticmethod
    def of_prefix(*prefix_bins):
        """
        Sets the bins of the query by prefix. If a queried statistic uses a binning strategy, only values matching the
        bin prefix will be returned.

        Args:
            prefix_bins (bytes): The prefixes used to match the bins.
        Returns:
            A bin constraint representing the set of bin prefixes.
        """
        j_constraints = geowave_pkg.core.store.api.BinConstraints.ofPrefix(
            BinConstraints._convert_byte_arrays(*prefix_bins))
        return BinConstraints(j_constraints)

    @staticmethod
    def of_object(binning_strategy_constraint):
        """
        Sets the bins of the query using an object type that is supported by the binning strategy. The result will be
        constrained to only statistics that use binning strategies that support this type of constraint and the
        resulting bins will be constrained according to that strategy's usage of this object. For example, spatial
        binning strategies may use spatial Envelope as constraints, or another example might be a numeric field binning
        strategy using Range as constraints. If a queried statistic uses a binning strategy, only values
        contained in one of the given bins will be return.

        Args:
            binning_strategy_constraint (any): An object of any type supported by the binning strategy. It will be
            interpreted as appropriate by the binning strategy and binning strategies that do not support this object
            type will not return any results.
        Returns:
            Bin constraints representing the provided object.
        """
        if isinstance(binning_strategy_constraint, GeoWaveObject):
            binning_strategy_constraint = binning_strategy_constraint.java_ref()
        elif isinstance(binning_strategy_constraint, BaseGeometry):
            binning_strategy_constraint = GeometryType().to_java(binning_strategy_constraint)
        j_constraints = geowave_pkg.core.store.api.BinConstraints.ofObject(binning_strategy_constraint)
        return BinConstraints(j_constraints)
