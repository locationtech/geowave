#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
This module contains classes that wrap the functionality of geotools SimpleFeatures and SimpleFeatureTypes.  These classes can be used to create feature types, features, and data adapters based on simple features.

It contains the following import shortcuts:
```python
from pygw.geotools import FeatureDataAdapter
from pygw.geotools import AttributeDescriptor
from pygw.geotools import SimpleFeature
from pygw.geotools import SimpleFeatureBuilder
from pygw.geotools import SimpleFeatureType
from pygw.geotools import SimpleFeatureTypeBuilder
```
"""

from .feature_data_adapter import FeatureDataAdapter
from .attribute_descriptor import AttributeDescriptor
from .simple_feature_type import SimpleFeatureType
from .simple_feature_type_builder import SimpleFeatureTypeBuilder
from .simple_feature import SimpleFeature
from .simple_feature_builder import SimpleFeatureBuilder
