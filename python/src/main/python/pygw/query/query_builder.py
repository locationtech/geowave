#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from pygw.base.type_conversions import StringArrayType

from .query import Query
from .base_query_builder import BaseQueryBuilder


class QueryBuilder(BaseQueryBuilder):
    """
    Base query builder for constructing GeoWave queries.  This class should not
    be used directly.  Instead, use one of the derived classes such as
    `pygw.query.vector.vector_query_builder.VectorQueryBuilder`.
    """

    def __init__(self, java_ref, java_transformer):
        super().__init__(java_ref, java_transformer)

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

    def build(self):
        """
        Builds the configured query.

        Returns:
            The final constructed `pygw.query.query.Query`.
        """
        return Query(self._java_ref.build(), self._java_transformer)
