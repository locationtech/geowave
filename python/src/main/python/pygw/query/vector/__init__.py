#
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
"""
This module contains the classes needed for querying vector data.

It contains the following import shortcuts:
```python
from pygw.query.vector import SpatialTemporalConstraintsBuilder
from pygw.query.vector import VectorQueryConstraintsFactory
from pygw.query.vector import FilterFactory
from pygw.query.vector import VectorQueryBuilder
from pygw.query.vector import VectorAggregationQueryBuilder
```
"""

from .spatial_temporal_constraints_builder import SpatialTemporalConstraintsBuilder
from .vector_query_constraints_factory import VectorQueryConstraintsFactory
from .filter_factory import FilterFactory
from .vector_query_builder import VectorQueryBuilder
from .vector_aggregation_query_builder import VectorAggregationQueryBuilder
