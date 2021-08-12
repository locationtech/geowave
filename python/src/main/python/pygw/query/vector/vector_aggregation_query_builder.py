#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from pygw.config import geowave_pkg
from ...base.envelope import EnvelopeTransformer
from ...base.interval import IntervalTransformer

from ...base.type_conversions import StringArrayType
from ..aggregation_query_builder import AggregationQueryBuilder
from .vector_query_constraints_factory import VectorQueryConstraintsFactory


class VectorAggregationQueryBuilder(AggregationQueryBuilder):
    """
    A builder for creating aggregation queries for vector data.
    """

    def __init__(self):
        j_agg_qbuilder = geowave_pkg.core.geotime.store.query.api.VectorAggregationQueryBuilder.newBuilder()
        super().__init__(j_agg_qbuilder)

    def constraints_factory(self):
        """
        Creates a constraints factory for vector queries.  The vector query constraint factory
        provides additional constraints specific to vector data.

        Returns:
            A `pygw.query.vector.VectorQueryConstraintsFactory`.
        """
        return VectorQueryConstraintsFactory(self._java_ref.constraintsFactory())

    def bbox_of_results(self, *type_names):
        """
        Convenience method for getting a bounding box of the results of a query. It uses the default geometry for a
        feature type which is also the indexed geometry.

        Args:
            type_names (str): The type names to get the bounding box of.
        Returns:
            This query builder.
        """
        self._java_transformer = EnvelopeTransformer()
        if type_names is None:
            self._java_ref.bboxOfResults()
        else:
            self._java_ref.bboxOfResults(StringArrayType().to_java(type_names))
        return self

    def bbox_of_results_for_geometry_field(self, type_name, geometry_field_name):
        """
        Convenience method for getting a bounding box of the results of a query.  This can be particularly useful if you
        want to calculate the bbox on a different field than the default/indexed Geometry.

        Args:
            type_name (str): The type to aggregate.
            geometry_field_name (str): The geometry field to get the bounding box of.
        Returns:
            This query builder.
        """
        self._java_transformer = EnvelopeTransformer()
        self._java_ref.bboxOfResultsForGeometryField(type_name, geometry_field_name)
        return self

    def time_range_of_results(self, *type_names):
        """
        Convenience method for getting a time range of the results of a query.  This uses inferred or hinted
        temporal attribute names.

        Args:
            type_names (str): The type names to get the time range of.
        Returns:
            This query builder.
        """
        self._java_transformer = IntervalTransformer()
        self._java_ref.timeRangeOfResults(StringArrayType().to_java(type_names))
        return self

    def time_range_of_results_for_time_field(self, type_name, time_field_name):
        """
        Convenience method for getting a time range of the results of a query.  This can be particularly useful if you
        want to calculate the time range on a specific time field.

        Args:
            type_name (str): The type to aggregate.
            time_field_name (str): The time field to get the range of.
        Returns:
            This query builder.
        """
        self._java_transformer = IntervalTransformer()
        self._java_ref.timeRangeOfResultsForTimeField(type_name, time_field_name)
        return self

    def min(self, type_name, numeric_field_name):
        """
        Convenience method for getting the minimum value of a numeric field from the results of a query.

        Args:
            type_name (str): The type to aggregate.
            numeric_field_name (str): The numeric field to get the minimum value of.
        Returns:
            This query builder.
        """
        j_field_parameter = geowave_pkg.core.store.query.aggregate.FieldNameParam(numeric_field_name)
        j_min_agg = geowave_pkg.core.store.query.aggregate.FieldMinAggregation(j_field_parameter)
        return self.aggregate(type_name, j_min_agg)

    def max(self, type_name, numeric_field_name):
        """
        Convenience method for getting the maximum value of a numeric field from the results of a query.

        Args:
            type_name (str): The type to aggregate.
            numeric_field_name (str): The numeric field to get the maximum value of.
        Returns:
            This query builder.
        """
        j_field_parameter = geowave_pkg.core.store.query.aggregate.FieldNameParam(numeric_field_name)
        j_max_agg = geowave_pkg.core.store.query.aggregate.FieldMaxAggregation(j_field_parameter)
        return self.aggregate(type_name, j_max_agg)

    def sum(self, type_name, numeric_field_name):
        """
        Convenience method for getting the sum of a numeric field from the results of a query.

        Args:
            type_name (str): The type to aggregate.
            numeric_field_name (str): The numeric field to get the sum of.
        Returns:
            This query builder.
        """
        j_field_parameter = geowave_pkg.core.store.query.aggregate.FieldNameParam(numeric_field_name)
        j_sum_agg = geowave_pkg.core.store.query.aggregate.FieldSumAggregation(j_field_parameter)
        return self.aggregate(type_name, j_sum_agg)
