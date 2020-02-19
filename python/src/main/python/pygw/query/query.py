#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject

from .query_result_transformer import QueryResultTransformer

class Query(GeoWaveObject):
    """
    Base Query class that wraps a GeoWave query.
    """

    def __init__(self, java_ref, result_transformer):
        assert isinstance(result_transformer, QueryResultTransformer)
        self.result_transformer = result_transformer
        super().__init__(java_ref)
