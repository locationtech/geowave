#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import java_pkg
from pygw.base import GeoWaveObject

from .simple_feature_type import SimpleFeatureType
from .simple_feature import SimpleFeature

class SimpleFeatureBuilder(GeoWaveObject):
    """
    Builds SimpleFeature instances for a given SimpleFeatureType.
    """

    def __init__(self, feature_type):
        """
        Constructs a new simple feature builder using the given feature type.

        Args:
            feature_type (pygw.geotools.simple_feature_type.SimpleFeatureType): The feature type of the feature.
        """
        assert isinstance(feature_type, SimpleFeatureType)
        self._feature_type = feature_type
        j_builder = java_pkg.org.geotools.feature.simple.SimpleFeatureBuilder(feature_type._java_ref)
        super().__init__(j_builder)

    def set_attr(self, descriptor, value):
        """
        Sets an attribute of the feature to the given value.

        Args:
            descriptor (str): The name or index of the attribute to set.
            value (any): The value of the attribute.
        Returns:
            This feature builder.
        """
        attr = self._feature_type.get_attribute(descriptor)
        if attr is None:
            raise SimpleFeatureBuilder.NoSuchAttributeInTypeError("No matching attribute for {}".format(descriptor))
        j_value = attr.to_java(value)
        self._java_ref.set(descriptor, j_value)
        return self

    def build(self, id):
        """
        Constructs the configured feature.

        Args:
            id (str): The feature ID to use.
        Returns:
            The constructed `pygw.geotools.simple_feature.SimpleFeature`.
        """
        j_feature = self._java_ref.buildFeature(str(id))
        return SimpleFeature(self._feature_type, j_feature)

    class NoSuchAttributeInTypeError(Exception):
        """
        Error that is raised when attempting to set an attribute using a descriptor
        that does not match any of the attributes in the feature type.
        """
        pass
