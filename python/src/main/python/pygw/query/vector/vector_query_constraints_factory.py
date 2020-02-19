#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from ..query_constraints_factory import QueryConstraintsFactory
from ..query_constraints import QueryConstraints
from .spatial_temporal_constraints_builder import SpatialTemporalConstraintsBuilder

class VectorQueryConstraintsFactory(QueryConstraintsFactory):
    """
    A query constraints factory with additional methods for creating spatial and/or
    temporal constraints for vector data.  Do not construct this class manually, instead,
    get the constraints factory by using the `constraints_factory()` method of the
    query builder.
    """

    def spatial_temporal_constraints(self):
        """
        Creates a spatial temporal constraints builder that can be used to construct
        spatial and/or temporal constraints.

        Returns:
            A new `pygw.query.vector.spatial_temporal_constraints_builder.SpatialTemporalConstraintsBuilder`.
        """
        return SpatialTemporalConstraintsBuilder(self._java_ref.spatialTemporalConstraints())

    def filter_constraints(self, filter):
        """
        Constrain a query using a filter created by pygw.query.FilterFactory.

        Args:
            filter (filter): The filter to constrain the query by.
        Returns:
            A `pygw.query.query_constraints.QueryConstraints` with the given filter.
        """
        return QueryConstraints(self._java_ref.filterConstraints(filter))

    def cql_constraints(self, cql_expression):
        """
        Constrain a query using a CQL expression.

        Args:
            cql_expression (str): The CQL expression to constrain the query by.
        Returns:
            A `pygw.query.query_constraints.QueryConstraints` with the given CQL expression.
        """
        return QueryConstraints(self._java_ref.cqlConstraints(cql_expression))
