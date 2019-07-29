#
# Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
This module contains all of the classes used in the creation of a connection to a GeoWave data store.

In general you would construct an instance of `pygw.store.data_store_options.DataStoreOptions` through one of the backend submodules such as `pygw.store.accumulo` or `pygw.store.rocksdb`.  You would then pass those options to `pygw.store.data_store_factory.DataStoreFactory.create_data_store`.

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
