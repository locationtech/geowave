#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject
from pygw.base.type_conversions import StringArrayType

from .query import Query
from .query_hint_key import QueryHintKey
from .query_constraints import QueryConstraints

class QueryBuilder(GeoWaveObject):
    """
    Base query builder for constructing GeoWave queries.  This class should not
    be used directly.  Instead, use one of the derived classes such as
    `pygw.query.vector.vector_query_builder.VectorQueryBuilder`.
    """

    def __init__(self, java_ref, result_transformer):
        self._result_transformer = result_transformer
        super().__init__(java_ref)

    def constraints_factory():
        """
        Creates a constraints factory for the type of query that is being built.
        This should be overridden by all derived query builders.

        Raises:
            NotImplementedError: This should be overridden by derived query builders.
        Returns:
            An appropriate `pygw.query.query_constraints_factory.QueryConstraintsFactory` for the query type.
        """
        raise NotImplementedError

    def all_types(self):
        """
        Configure the query to get data from all types. This is the default.

        Returns:
            This query builder.
        """
        self._java_ref.allTypes()
        return self

    def add_type_name(self, type_name):
        """
        Configure the query to get data from a specific type.

        Args:
            type_name (str): The type to get data from.
        Returns:
            This query builder.
        """
        self._java_ref.addTypeName(type_name)
        return self

    def set_type_names(self, type_names):
        """
        Configure the query to get data from a set of types.

        Args:
            type_names (list of str): The types to get data from.
        Returns:
            This query builder.
        """
        self._java_ref.setTypeNames(StringArrayType().to_java(type_names))
        return self

    def subset_fields(self, type_name, field_names):
        """
        Configure the query to get a specific set of fields from a given type.

        Args:
            type_name (str): The type to get from.
            field_names (list of str): The fields to get.
        Returns:
            This query builder.
        """
        self._java_ref.subsetFields(type_name, StringArrayType().to_java(field_names))
        return self

    def all_fields(self):
        """
        Configure the query to get all fields from the given type(s). This is the
        default.

        Returns:
            This query builder.
        """
        self._java_ref.allFields()
        return self

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

        Returns:
            The final constructed `pygw.query.query.Query`.
        """
        return Query(self._java_ref.build(), self._result_transformer)
