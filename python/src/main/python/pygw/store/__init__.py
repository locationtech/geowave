#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
This module contains classes that can be used to establish connections to the various GeoWave backends.  Each store type has a submodule which contains a class that can be used to connect to that store type.  For example `from pygw.store.accumulo import AccumuloOptions`.  The `DataStore` object can be constructed by passing the options object to the `DataStoreFactory.create_data_store(<options>)` method.

This module contains the following import shortcuts:
```python
from pygw.store import DataStore
from pygw.store import DataStoreOptions
from pygw.store import DataStoreFactory
```
"""

from .data_store import DataStore
from .data_store_options import DataStoreOptions
from .data_store_factory import DataStoreFactory
