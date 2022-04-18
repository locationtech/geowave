#
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from .query import Query


class AggregationQuery(Query):
    """
    Base aggregation query class that wraps GeoWave aggregation queries.
    """

    def __init__(self, java_ref, java_transformer):
        super().__init__(java_ref, java_transformer)
