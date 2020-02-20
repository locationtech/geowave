#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject

class SimpleFeatureType(GeoWaveObject):
    """
    Defines a schema for vector features.
    """

    def __init__(self, java_ref, attributes):
        self.attribute_dict = {}
        for a in attributes:
            self.attribute_dict[a.descriptor] = a
        self.attribute_list = attributes
        super().__init__(java_ref)

    def get_type_name(self):
        """
        Returns:
            The name of the feature type.
        """
        return self._java_ref.getTypeName()

    def get_attribute(self, attribute):
        """
        Gets an attribute descriptor by index or by name.

        Args:
            attribute (str or int): Name or index of the descriptor to get.
        Returns:
            A `pygw.geotools.attribute_descriptor.AttributeDescriptor` for the attribute requested.
        """
        if isinstance(attribute, int):
            return self.attribute_list[attribute]
            pass
        elif isinstance(attribute, str) and attribute in self.attribute_dict:
            return self.attribute_dict[attribute]
            pass
        return None

    def get_attribute_descriptors(self):
        """
        Gets all of the attribute descriptors for this feature type.

        Returns:
            A list of `pygw.geotools.attribute_descriptor.AttributeDescriptor` for this feature type.
        """
        return self.attribute_list
