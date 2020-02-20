#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg
from pygw.base import DataTypeAdapter

from .simple_feature_type import SimpleFeatureType

class FeatureDataAdapter(DataTypeAdapter):
    """
    Data adapter for storing and retrieving SimpleFeatures from a GeoWave data store.
    """

    def __init__(self, feature_type):
        """
        Constructs a feature data adapter using the given feature type.

        Args:
            feature_type (pygw.geotools.simple_feature_type.SimpleFeatureType): The feature type of the data.
        """
        self.feature_type = feature_type
        assert isinstance(feature_type, SimpleFeatureType)
        j_feat_type = feature_type._java_ref
        j_feat_adapter = geowave_pkg.adapter.vector.FeatureDataAdapter(j_feat_type)
        super().__init__(j_feat_adapter)
