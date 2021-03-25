#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from .base_query_builder import BaseQueryBuilder
from .aggregation_query import AggregationQuery
from ..base.type_conversions import StringArrayType


class AggregationQueryBuilder(BaseQueryBuilder):
    """
    A builder for creating aggregation queries. This class should not be used directly.  Instead, use one of the derived
    classes such as `pygw.query.vector.VectorAggregationQueryBuilder`.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)

    def count(self, *type_names):
        """
        This is a convenience method to set the count aggregation if no type names are given it is
        assumed to count every type.

        Args:
            type_names (str): The type names to count results.
        Returns:
            This query builder.
        """
        if type_names is None:
            self._java_ref.count()
        else:
            self._java_ref.count(StringArrayType().to_java(type_names))
        return self

    def aggregate(self, type_name, j_aggregation):
        """
        Provide the Java Aggregation function and the type name to apply the aggregation on.

        Args:
            type_name (str): The type name to aggregate.
            j_aggregation (Aggregation):  The Java aggregation function to
        Returns:
            This query builder.
        """

        return self._java_ref.aggregate(type_name, j_aggregation)

    def build(self):
        """
        Builds the configured aggregation query.

        Returns:
            The final constructed `pygw.query.AggregationQuery`.
        """
        return AggregationQuery(self._java_ref.build(), self._java_transformer)
