#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from pygw.base import GeoWaveObject
from .statistic_binning_strategy import StatisticBinningStrategy
from .statistic_type import DataTypeStatisticType, IndexStatisticType, FieldStatisticType
from .binning_strategy_mappings import map_binning_strategy
from ..base.java_transformer import NoOpTransformer


class Statistic(GeoWaveObject):
    """
    Base GeoWave statistic.
    """

    def __init__(self, java_ref, java_transformer=NoOpTransformer()):
        self.java_transformer = java_transformer
        super().__init__(java_ref)

    def get_statistic_type(self):
        """
        Get the statistic type associated with the statistic.

        Returns:
            The type of this statistic.
        """
        pass

    def get_description(self):
        """
        Gets a description of the statistic.

        Returns:
            A description of the statistic.
        """
        return self._java_ref.getDescription()

    def set_tag(self, tag):
        """
        Sets the tag of the statistic.

        Args:
            tag (str): The tag to use for the statistic
        """
        self._java_ref.setTag(tag)

    def get_tag(self):
        """
        Get the tag for the statistic.

        Returns:
            The tag for this statistic.
        """
        return self._java_ref.getTag()

    def set_internal(self):
        """
        Set the tag of this statistic to the default internal statistic tag.
        """
        self._java_ref.setInternal()

    def is_internal(self):
        """
        Checks if the statistic is an internal statistic.

        Returns:
            True if the statistic is internal.
        """
        return self._java_ref.isInternal()

    def set_binning_strategy(self, binning_strategy):
        """
        Sets the binning strategy of the statistic.

        Args:
            binning_strategy (StatisticBinningStrategy): The binning strategy to use for the statistic.
        """
        if not isinstance(binning_strategy, StatisticBinningStrategy):
            raise AttributeError('Expected an instance of StatisticBinningStrategy')
        self._java_ref.setBinningStrategy(binning_strategy.java_ref())

    def get_binning_strategy(self):
        """
        Gets the binning strategy used by the statistic.

        Returns:
            The binning strategy used by the statistic.
        """
        return map_binning_strategy(self._java_ref.getBinningStrategy())


class IndexStatistic(Statistic):

    def get_statistic_type(self):
        """
        Get the statistic type associated with the statistic.

        Returns:
            The type of this statistic.
        """
        return IndexStatisticType(self._java_ref.getStatisticType())

    def set_index_name(self, name):
        """
        Sets the index name of the statistic.

        Args:
            name (str): The index name to use for the statistic
        """
        self._java_ref.setIndexName(name)

    def get_index_name(self):
        """
        Get the index name associated with the statistic.

        Returns:
            The index name of this statistic.
        """
        return self._java_ref.getIndexName()


class DataTypeStatistic(Statistic):

    def get_statistic_type(self):
        """
        Get the statistic type associated with the statistic.

        Returns:
            The type of this statistic.
        """
        return DataTypeStatisticType(self._java_ref.getStatisticType())

    def set_type_name(self, name):
        """
        Sets the type name of the statistic.

        Args:
            name (str): The type name to use for the statistic
        """
        self._java_ref.setTypeName(name)

    def get_type_name(self):
        """
        Get the type name associated with the statistic.

        Returns:
            The type name of this statistic.
        """
        return self._java_ref.getTypeName()


class FieldStatistic(Statistic):

    def get_statistic_type(self):
        """
        Get the statistic type associated with the statistic.

        Returns:
            The type of this statistic.
        """
        return FieldStatisticType(self._java_ref.getStatisticType())

    def set_type_name(self, name):
        """
        Sets the type name of the statistic.

        Args:
            name (str): The type name to use for the statistic
        """
        self._java_ref.setTypeName(name)

    def get_type_name(self):
        """
        Get the type name associated with the statistic.

        Returns:
            The type name of this statistic.
        """
        return self._java_ref.getTypeName()

    def set_field_name(self, field_name):
        """
        Sets the field name of the statistic.

        Args:
            field_name (str): The field name to use for the statistic
        """
        self._java_ref.setFieldName(field_name)

    def get_field_name(self):
        """
        Get the field name associated with the statistic.

        Returns:
            The field name of this statistic.
        """
        return self._java_ref.getFieldName()
