#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
"""
This module contains classes that are used for GeoWave statistics.

It contains the following import shortcuts:
```python
from pygw.statistics import Statistic
from pygw.statistics import DataTypeStatistic
from pygw.statistics import FieldStatistic
from pygw.statistics import IndexStatistic
from pygw.statistics import StatisticValue
from pygw.statistics import StatisticBinningStrategy
from pygw.statistics import StatisticType
from pygw.statistics import DataTypeStatisticType
from pygw.statistics import FieldStatisticType
from pygw.statistics import IndexStatisticType
from pygw.statistics import BinConstraints
```
"""

from .statistic import Statistic
from .statistic import DataTypeStatistic
from .statistic import FieldStatistic
from .statistic import IndexStatistic
from .statistic_value import StatisticValue
from .statistic_binning_strategy import StatisticBinningStrategy
from .statistic_type import StatisticType
from .statistic_type import DataTypeStatisticType
from .statistic_type import FieldStatisticType
from .statistic_type import IndexStatisticType
from .bin_constraints import BinConstraints

