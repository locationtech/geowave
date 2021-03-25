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
from pygw.config import geowave_pkg
from pygw.query.statistics.statistic_query import StatisticQuery
from pygw.statistics.bin_constraints import BinConstraints
from pygw.statistics.statistic_type import StatisticType, IndexStatisticType, DataTypeStatisticType


class StatisticQueryBuilder(GeoWaveObject):
    """
    A builder for creating statistics queries. This class should not be constructed directly, instead use one of the
    static methods to create an appropriate builder.
    """

    def __init__(self, java_ref, result_transformer):
        self._result_transformer = result_transformer
        super().__init__(java_ref)

    def tag(self, tag):
        """
        Sets the tag to query for.

        Args:
            tag (str): The tag to query for.
        Returns:
            This statistic query builder.
        """
        self._java_ref.tag(tag)
        return self

    def internal(self):
        """
        When set, only internal statistics will be queried.

        Returns:
            This statistic query builder.
        """
        self._java_ref.internal()
        return self

    def add_authorization(self, authorization):
        """
        Adds an authorization to the query.

        Args:
            authorization (str): The authorization to add.
        Returns:
            This statistic query builder.
        """
        self._java_ref.addAuthorization(authorization)
        return self

    def authorizations(self, authorizations):
        """
        Sets the set of authorizations to use for the query.

        Args:
            authorizations (array of str): The authorizations to use for the query.
        Returns:
            This statistic query builder.
        """
        self._java_ref.authorizations(StringArrayType().to_java(authorizations))
        return self

    def bin_constraints(self, bin_constraints):
        """
        Sets the constraints to use for the statistic query.  Only bins that match the given constraints will be
        returned.

        Args:
            bin_constraints (BinConstraints): The constraints to constrain the query by.
        Returns:
            This statistic query builder.
        """
        if not isinstance(bin_constraints, BinConstraints):
            raise AttributeError('Must be a BinConstraints instance.')
        self._java_ref.binConstraints(bin_constraints.java_ref())
        return self

    def build(self):
        """
        Build the statistic query.

        Returns:
            This constructed statistic query.
        """
        return StatisticQuery(self._java_ref.build(), self._result_transformer)

    @staticmethod
    def new_builder(statistic_type):
        """
        Create a statistic query builder for the given statistic type.

        Args:
            statistic_type (StatisticType): The statistic type for the query builder.
        Returns:
            A statistic query builder.
        """
        if not isinstance(statistic_type, StatisticType):
            raise AttributeError('Must be a StatisticType instance.')

        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.newBuilder(statistic_type.java_ref())
        if isinstance(statistic_type, IndexStatisticType):
            return IndexStatisticQueryBuilder(statistic_type, j_builder)
        if isinstance(statistic_type, DataTypeStatisticType):
            return DataTypeStatisticQueryBuilder(statistic_type, j_builder)
        return FieldStatisticQueryBuilder(statistic_type, j_builder)

    @staticmethod
    def differing_visibility_count():
        """
        Create a statistic query builder for a differing visibility count statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.differingVisibilityCount()
        return IndexStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def duplicate_entry_count():
        """
        Create a statistic query builder for a duplicate entry count statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.duplicateEntryCount()
        return IndexStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def field_visibility_count():
        """
        Create a statistic query builder for a field visibility count statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.fieldVisibilityCount()
        return IndexStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def index_meta_data_set():
        """
        Create a statistic query builder for an index meta data set statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.indexMetaDataSet()
        return IndexStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def max_duplicates():
        """
        Create a statistic query builder for a max duplicates statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.maxDuplicates()
        return IndexStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def partitions():
        """
        Create a statistic query builder for a partitions statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.partitions()
        return IndexStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def row_range_histogram():
        """
        Create a statistic query builder for a row range histogram statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.rowRangeHistogram()
        return IndexStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def count():
        """
        Create a statistic query builder for a count statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.count()
        return DataTypeStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def bbox():
        """
        Create a statistic query builder for a bounding box statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.geotime.store.statistics.SpatialTemporalStatisticQueryBuilder.bbox()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def bloom_filter():
        """
        Create a statistic query builder for a bloom filter statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.bloomFilter()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def time_range():
        """
        Create a statistic query builder for a time range statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.geotime.store.statistics.SpatialTemporalStatisticQueryBuilder.timeRange()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def count_min_sketch():
        """
        Create a statistic query builder for a count min sketch statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.countMinSketch()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def fixed_bin_numeric_histogram():
        """
        Create a statistic query builder for a fixed bin numeric histogram statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.fixedBinNumericHistogram()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def hyper_log_log():
        """
        Create a statistic query builder for a hyper log log statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.hyperLogLog()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def numeric_histogram():
        """
        Create a statistic query builder for a numeric histogram statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.numericHistogram()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def numeric_mean():
        """
        Create a statistic query builder for a numeric mean statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.numericMean()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def numeric_range():
        """
        Create a statistic query builder for a numeric range statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.numericRange()
        return FieldStatisticQueryBuilder(java_ref=j_builder)

    @staticmethod
    def numeric_stats():
        """
        Create a statistic query builder for a numeric stats statistic.

        Returns:
            A statistic query builder.
        """
        j_builder = geowave_pkg.core.store.api.StatisticQueryBuilder.numericStats()
        return FieldStatisticQueryBuilder(java_ref=j_builder)


class IndexStatisticQueryBuilder(StatisticQueryBuilder):
    """
    A builder for index statistic queries.
    """

    def __init__(self, statistic_type=None, java_ref=None):
        if java_ref is None:
            j_qbuilder = geowave_pkg.core.statistics.query.IndexStatisticQueryBuilder(statistic_type.java_ref())
        else:
            j_qbuilder = java_ref
        super().__init__(j_qbuilder, None)

    def index_name(self, index_name):
        """
        Set the index name to constrain the query by.

        Args:
            index_name (str): The index name to query.
        Returns:
            This statistic query builder.
        """
        self._java_ref.indexName(index_name)
        return self


class DataTypeStatisticQueryBuilder(StatisticQueryBuilder):
    """
    A builder for data type statistic queries.
    """

    def __init__(self, statistic_type=None, java_ref=None):
        if java_ref is None:
            j_qbuilder = geowave_pkg.core.statistics.query.DataTypeStatisticQueryBuilder(statistic_type.java_ref())
        else:
            j_qbuilder = java_ref
        super().__init__(j_qbuilder, None)

    def type_name(self, type_name):
        """
        Set the type name to constrain the query by.

        Args:
            type_name (str): The type name to query.
        Returns:
            This statistic query builder.
        """
        self._java_ref.typeName(type_name)
        return self


class FieldStatisticQueryBuilder(StatisticQueryBuilder):
    """
    A builder for field statistic queries.
    """

    def __init__(self, statistic_type=None, java_ref=None):
        if java_ref is None:
            j_qbuilder = geowave_pkg.core.statistics.query.FieldStatisticQueryBuilder(statistic_type.java_ref())
        else:
            j_qbuilder = java_ref
        super().__init__(j_qbuilder, None)

    def type_name(self, type_name):
        """
        Set the type name to constrain the query by.

        Args:
            type_name (str): The type name to query.
        Returns:
            This statistic query builder.
        """
        self._java_ref.typeName(type_name)
        return self

    def field_name(self, field_name):
        """
        Set the field name to constrain the query by.

        Args:
            field_name (str): The field name to query.
        Returns:
            This statistic query builder.
        """
        self._java_ref.fieldName(field_name)
        return self
