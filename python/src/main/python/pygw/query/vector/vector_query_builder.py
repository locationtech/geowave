#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg

from ..query_builder import QueryBuilder
from .simple_feature_transformer import SimpleFeatureTransformer
from .vector_query_constraints_factory import VectorQueryConstraintsFactory

class VectorQueryBuilder(QueryBuilder):
    """
    A builder for creating queries for vector data.
    """

    def __init__(self):
        j_vector_qbuilder = geowave_pkg.core.geotime.store.query.api.VectorQueryBuilder.newBuilder()
        super().__init__(j_vector_qbuilder, SimpleFeatureTransformer())

    def constraints_factory(self):
        """
        Creates a constraints factory for vector queries.  The vector query constraint factory
        provides additional constraints specific to vector data.

        Returns:
            A `pygw.query.vector.vector_query_constraints_factory.VectorQueryConstraintsFactory`.
        """
        return VectorQueryConstraintsFactory(self._java_ref.constraintsFactory())
