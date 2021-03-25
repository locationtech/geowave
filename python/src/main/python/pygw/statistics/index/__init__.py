#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
"""
This module contains index statistics

It contains the following import shortcuts:
```python
from pygw.statistics.index import DifferingVisibilityCountStatistic
from pygw.statistics.index import DuplicateEntryCountStatistic
from pygw.statistics.index import FieldVisibilityCountStatistic
from pygw.statistics.index import IndexMetaDataSetStatistic
from pygw.statistics.index import MaxDuplicatesStatistic
from pygw.statistics.index import PartitionsStatistic
from pygw.statistics.index import RowRangeHistogramStatistic
```
"""

from .differing_visibility_count_statistic import DifferingVisibilityCountStatistic
from .duplicate_entry_count_statistic import DuplicateEntryCountStatistic
from .field_visibility_count_statistic import FieldVisibilityCountStatistic
from .index_meta_data_set_statistic import IndexMetaDataSetStatistic
from .max_duplicates_statistic import MaxDuplicatesStatistic
from .partitions_statistic import PartitionsStatistic
from .row_range_histogram_statistic import RowRangeHistogramStatistic
