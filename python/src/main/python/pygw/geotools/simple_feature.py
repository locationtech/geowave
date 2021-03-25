#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from pygw.base import GeoWaveObject
from pygw.base.type_conversions import GeometryType
from . import AttributeDescriptor

from .simple_feature_type import SimpleFeatureType
from ..base.java_transformer import JavaTransformer


class SimpleFeature(GeoWaveObject):
    """
    A Simple (Vector) Feature.
    """

    def __init__(self, feature_type, java_ref):
        assert isinstance(feature_type, SimpleFeatureType)
        self._feature_type = feature_type
        super().__init__(java_ref)

    def get_id(self):
        """
        Returns:
            The ID of the feature.
        """
        return self._java_ref.getID()

    def get_type(self):
        """
        Returns:
            The `pygw.geotools.simple_feature_type.SimpleFeatureType` of the feature.
        """
        return self._feature_type

    def get_feature_type(self):
        """
        Identical to `get_type`.

        Returns:
            The `pygw.geotools.simple_feature_type.SimpleFeatureType` of the feature.
        """
        return self.get_type()

    def get_attributes(self):
        """
        Gets all of the attribute values of the feature in a single list.

        Returns:
            A list containing all attribute values of the feature.
        """
        j_values = self._java_ref.getAttributes()
        descriptors = self._feature_type.get_attribute_descriptors()
        values = []
        for i in range(len(j_values)):
            values.append(descriptors[i].from_java(j_values[i]))
        return values

    def get_attribute(self, attribute):
        """
        Gets a single attribute value by name or by index.

        Args:
            attribute (str or int): Name or index of the attribute to get.
        Returns:
            The value of the requested attribute.
        """
        j_value = self._java_ref.getAttribute(attribute)
        attr = self._feature_type.get_attribute(attribute)
        return attr.from_java(j_value)

    def get_attribute_count(self):
        """
        Returns:
            The number of attributes that the feature has.
        """
        return self._java_ref.getAttributeCount()

    def get_default_geometry(self):
        """
        Returns:
            The default geometry of the feature.
        """
        j_geom = self._java_ref.getDefaultGeometry()
        return GeometryType().from_java(j_geom)

    def to_dict(self, id_column="id"):
        """
        Convert this feature to a dictionary, including the feature ID.

        Args:
            id_column (str): The key for the feature ID. Default is "id".
        Returns:
            The attributes and feature ID of this feature in a dictionary.
        """
        j_values = self._java_ref.getAttributes()
        descriptors = self._feature_type.get_attribute_descriptors()
        feature_dict = {
            id_column: self._java_ref.getID()
        }
        for i in range(len(j_values)):
            feature_dict[descriptors[i].descriptor] = descriptors[i].from_java(j_values[i])
        return feature_dict


class SimpleFeatureTransformer(JavaTransformer):
    """
    Transforms Java SimpleFeatures into pgyw.geotools.SimpleFeatures.  In order to accomplish this, the pygw variant of
    the SimpleFeatureType has to be constructed from the feature.  In order to avoid doing this multiple times, there
    is a feature type cache that the transform function can pull from.
    """

    def __init__(self):
        self._feature_type_cache = {}

    def transform(self, j_object):
        """
        Transform the given Java SimpleFeature into a pygw.geotools.SimpleFeature.

        Args:
            j_object (Java SimpleFeature): A geotools SimpleFeature Java object.
        Returns:
            A `pygw.geotools.simple_feature.SimpleFeature`.
        """
        j_sft = j_object.getFeatureType()
        type_name = j_sft.getTypeName()
        if type_name in self._feature_type_cache:
            sft = self._feature_type_cache[type_name]
        else:
            j_attrs = j_sft.getAttributeDescriptors().iterator()
            descriptors = []
            while j_attrs.hasNext():
                descriptors.append(AttributeDescriptor.from_java_attribute_descriptor(j_attrs.next()))
            sft = SimpleFeatureType(j_sft, descriptors)
            self._feature_type_cache[type_name] = sft
        return SimpleFeature(sft, j_object)
