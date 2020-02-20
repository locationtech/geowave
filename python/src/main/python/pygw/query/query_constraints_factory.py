#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from py4j.java_gateway import JavaClass

from pygw.base import GeoWaveObject
from pygw.base.type_conversions import PrimitiveByteArrayType
from pygw.config import geowave_pkg
from pygw.config import java_gateway

from .query_constraints import QueryConstraints

_pbat = PrimitiveByteArrayType()

class QueryConstraintsFactory(GeoWaveObject):
    """
    Base factory for constructing general query constraints to be used by the
    query builder.  Do not use this factory directly, instead get an instance of
    the factory from the `pygw.query.query_builder.QueryBuilder.constraints_factory` method of the query builder.
    """

    def data_ids(self, data_ids):
        """
        Constrain a query by data IDs.

        Args:
            data_ids (list of bytes): The data IDs to constrain by.
        Returns:
            A `pygw.query.query_constraints.QueryConstraints` with the given data ids.
        """
        byte_array_class = JavaClass("[B", java_gateway._gateway_client)
        j_data_ids = java_gateway.new_array(byte_array_class, len(data_ids))
        for idx, data_id in enumerate(data_ids):
            j_data_ids[idx] = _pbat.to_java(data_id)
        j_qc = self._java_ref.dataIds(j_data_ids)
        return QueryConstraints(j_qc)

    def data_ids_by_range(self, start_data_id_inclusive, end_data_id_inclusive):
        """
        Constrain a query using a range of data IDs, assuming big endian ordering.

        Args:
            start_data_id_inclusive (bytes): The start of data ID range (inclusive).
            end_data_id_inclusive (bytes): The end of data ID range (inclusive).
        Returns:
            A `pygw.query.query_constraints.QueryConstraints` with the given data ID range.
        """
        j_qc = self._java_ref.dataIdsByRange(_pbat.to_java(start_data_id_inclusive),
                                             _pbat.to_java(end_data_id_inclusive))
        return QueryConstraints(j_qc)

    def prefix(self, partition_key, sort_key_prefix):
        """
        Constrain a query by prefix.

        Args:
            partition_key (bytes): The partition key to constrain by.
            sort_key_prefix (bytes): The sort key prefix to constrain by.
        Returns:
            A `pygw.query.query_constraints.QueryConstraints` with the given prefix.
        """
        j_qc = self._java_ref.prefix(_pbat.to_java(partition_key),
                                     _pbat.to_java(sort_key_prefix))
        return QueryConstraints(j_qc)

    def coordinate_ranges(self, index_strategy, coordinate_ranges):
        # TODO: support Python variants of NumericIndexStrategy and MultiDimensionalCoordinateRangesArray
        raise NotImplementedError

    def constraints(self, constraints, compare_op=None):
        # TODO: support Python variants of Constraints
        raise NotImplementedError

    def no_constraints(self):
        """
        Use no query constraints, meaning wide open query (this is the default).

        Returns:
            A `pygw.query.query_constraints.QueryConstraints` with no constraints.
        """
        return QueryConstraints(self._java_ref.noConstraints())
