#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.geotools import SimpleFeature
from pygw.geotools import SimpleFeatureType
from pygw.geotools import AttributeDescriptor

from ..query_result_transformer import QueryResultTransformer

class SimpleFeatureTransformer(QueryResultTransformer):
    """
    Transforms Java SimpleFeature query results into pgyw.geotools.SimpleFeature
    results.  In order to accomplish this, the pygw variant of the SimpleFeatureType
    has to be constructed from the feature.  In order to avoid doing this for every
    result, there is a feature type cache that the transform function can pull from.
    """
    def __init__(self):
        self._feature_type_cache = {}

    def transform(self, j_result):
        """
        Transform the given Java SimpleFeature into a pygw.geotools.SimpleFeature.

        Args:
            j_result (Java SimpleFeature): A geotools SimpleFeature Java object.
        Returns:
            A `pygw.geotools.simple_feature.SimpleFeature`.
        """
        j_sft = j_result.getFeatureType()
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
        return SimpleFeature(sft, j_result)
