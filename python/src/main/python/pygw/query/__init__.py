#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
This module contains classes that are used in constructing queries and their constraints.

It contains the following import shortcuts:
```python
from pygw.query import Query
from pygw.query import QueryConstraints
from pygw.query import QueryHintKey
from pygw.query import VectorQueryBuilder
from pygw.query import FilterFactory
```
"""

from .query import Query
from .query_constraints import QueryConstraints
from .query_hint_key import QueryHintKey
from .vector import VectorQueryBuilder
from .vector import FilterFactory
