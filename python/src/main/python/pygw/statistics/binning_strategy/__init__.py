#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
"""
This module contains statistic binning strategies

It contains the following import shortcuts:
```python
from pygw.statistics.binning_strategy import CompositeBinningStrategy
from pygw.statistics.binning_strategy import DataTypeBinningStrategy
from pygw.statistics.binning_strategy import FieldValueBinningStrategy
from pygw.statistics.binning_strategy import NumericRangeFieldValueBinningStrategy
from pygw.statistics.binning_strategy import SpatialFieldValueBinningStrategy
from pygw.statistics.binning_strategy import TimeRangeFieldValueBinningStrategy
from pygw.statistics.binning_strategy import PartitionBinningStrategy
```
"""

from .composite_binning_strategy import CompositeBinningStrategy
from .data_type_binning_strategy import DataTypeBinningStrategy
from .field_value_binning_strategy import FieldValueBinningStrategy
from .numeric_range_field_value_binning_strategy import NumericRangeFieldValueBinningStrategy
from .spatial_field_value_binning_strategy import SpatialFieldValueBinningStrategy
from .time_range_field_value_binning_strategy import TimeRangeFieldValueBinningStrategy
from .partition_binning_strategy import PartitionBinningStrategy
