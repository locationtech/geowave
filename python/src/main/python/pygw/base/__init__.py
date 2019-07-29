#
# Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
This module contains classes that are used by multiple other modules.

It contains the following import shortcuts:
```python
from pygw.base import GeoWaveObject
from pygw.base import CloseableIterator
from pygw.base import DataTypeAdapter
from pygw.base import Writer
```
"""

from .geowave_object import GeoWaveObject
from .closeable_iterator import CloseableIterator
from .data_type_adapter import DataTypeAdapter
from .writer import Writer
