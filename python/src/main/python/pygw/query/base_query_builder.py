#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from pygw.base import GeoWaveObject
from pygw.base.type_conversions import StringArrayType

from .query_constraints_factory import QueryConstraintsFactory
from .query_hint_key import QueryHintKey
from .query_constraints import QueryConstraints
from ..base.java_transformer import NoOpTransformer


class BaseQueryBuilder(GeoWaveObject):
    """
    A base class for building queries.  This class should not be used directly.  Instead, use one of the derived classes
    such as `pygw.query.vector.VectorQueryBuilder` or `pygw.query.vector.VectorAggregationQueryBuilder`.
    """

    def __init__(self, java_ref, java_transformer=NoOpTransformer()):
        self._java_transformer = java_transformer
        super().__init__(java_ref)

    def constraints_factory(self):
        """
        Creates a constraints factory for the type of query that is being built.

        Returns:
            An appropriate `pygw.query.QueryConstraintsFactory` for the query type.
        """
        return QueryConstraintsFactory(self._java_ref.constraints_factory())

    def all_indices(self):
        """
        Configure the query to allow the use of all indices when getting data.
        This is the default.

        Returns:
            This query builder.
        """
        self._java_ref.allIndices()
        return self

    def index_name(self, index_name):
        """
        Configure the query to get data from a specific index.

        Args:
            index_name (str): The name of the index to get data from.
        Returns:
            This query builder.
        """
        self._java_ref.indexName(index_name)
        return self

    def add_authorization(self, authorization):
        """
        Configure the query to get data using the given authorization.

        Args:
            authorization (str): The authorization to use in the query.
        Returns:
            This query builder.
        """
        self._java_ref.addAuthorization(authorization)
        return self

    def set_authorizations(self, authorizations):
        """
        Configure the query to get data using the given set of authorizations.

        Args:
            authorizations (list of str): The authorizations to use in the query.
        Returns:
            This query builder.
        """
        self._java_ref.setAuthorizations(StringArrayType().to_java(authorizations))
        return self

    def no_authorizations(self):
        """
        Configure the query to get data without using any authorizations.  This
        is the default.

        Returns:
            This query builder.
        """
        self._java_ref.noAuthorizations()
        return self

    def no_limit(self):
        """
        Configure the query to get all results that match the query constraints.
        This is the default.

        Returns:
            This query builder.
        """
        self._java_ref.noLimit()
        return self

    def limit(self, limit):
        """
        Configure the query to only return a limited number of results.

        Args:
            limit (int): The maximum number of results to get.
        Returns:
            This query builder.
        """
        self._java_ref.limit(limit)
        return self

    def add_hint(self, key, value):
        """
        Adds a hint to the query.  Available query hints are defined by the
        enumeration at `pygw.query.query_hint_key.QueryHintKey`.

        Args:
            key (pygw.query.query_hint_key.QueryHintKey): The key of the hint to set.
            value (any): The value to use for the hint.
        Returns:
            This query builder.
        """
        assert isinstance(key, QueryHintKey)
        self._java_ref.addHint(QueryHintKey.get_key(key), value)
        return self

    def no_hints(self):
        """
        Configure the query to use no query hints.  This is the default.

        Returns:
            This query builder.
        """
        self._java_ref.noHints()
        return self

    def constraints(self, constraints):
        """
        Configure the query to be constrained by the given query constraints.
        Constraints can be constructed using the factory provided by the
        `pygw.query.query_builder.QueryBuilder.constraints_factory` method.

        Args:
            constraints (pygw.query.query_constraints.QueryConstraints): The constraints to use.
        Returns:
            This query builder.
        """
        assert isinstance(constraints, QueryConstraints)
        self._java_ref.constraints(constraints._java_ref)
        return self

    def build(self):
        """
        Builds the configured query.

        Raises:
            NotImplementedError: This should be overridden by derived query builders.
        Returns:
            The constructed query.
        """
        raise NotImplementedError
