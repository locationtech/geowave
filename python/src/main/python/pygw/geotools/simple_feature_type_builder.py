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
from .attribute_descriptor import AttributeDescriptor

class SimpleFeatureTypeBuilder(GeoWaveObject):
    """
    Builds `pygw.geotools.simple_feature_type.SimpleFeatureType` instances.
    """

    def __init__(self):
        self.attributes = []
        super().__init__(java_pkg.org.geotools.feature.simple.SimpleFeatureTypeBuilder())

    def set_name(self, name):
        """
        Sets the name of the feature type.

        Args:
            name (str): The name to use.
        Returns:
            This feature type builder.
        """
        self._java_ref.setName(name)
        return self

    def set_namespace_uri(self, namespace_uri):
        """
        Sets the namespace URI of the feature type.

        Args:
            namespace_uri (str): The namespace URI to use.
        Returns:
            This feature type builder.
        """
        self._java_ref.setNamespaceURI(namespace_uri)
        return self

    def set_srs(self, srs):
        """
        Sets the spatial reference system of the feature type.

        Args:
            srs (str): The spatial reference system to use.
        Returns:
            This feature type builder.
        """
        self._java_ref.setSRS(srs)
        return self

    def add(self, attribute_descriptor):
        """
        Adds an attribute to the feature type.

        Args:
            attribute_descriptor (pygw.geotools.attribute_descriptor.AttributeDescriptor): The attribute to add.
        Returns:
            This feature type builder.
        """
        if isinstance(attribute_descriptor, AttributeDescriptor):
            self.attributes.append(attribute_descriptor)
            self._java_ref.add(attribute_descriptor._java_ref)
            return self
        else:
            raise ValueError("attribute_descriptor should be of type AttributeDescriptor")

    def build_feature_type(self):
        """
        Builds the configured feature type.

        Returns:
            A `pygw.geotools.simple_feature_type.SimpleFeatureType` with the given configuration.
        """
        return SimpleFeatureType(self._java_ref.buildFeatureType(), self.attributes)
