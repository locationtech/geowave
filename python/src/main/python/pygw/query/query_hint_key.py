#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from enum import Enum

from pygw.config import geowave_pkg

class QueryHintKey(Enum):
    """
    Keys for query hints.
    """

    MAX_RANGE_DECOMPOSITION = 0

    @classmethod
    def get_key(cls, key):
        """
        Gets the Java hint key from the given QueryHintKey.

        Args:
            key (pygw.query.query_hint_key.QueryHintKey): The enum value of QueryHintKey to get.
        Returns:
            The Java equivalent of the query hint key.
        """
        return {
            QueryHintKey.MAX_RANGE_DECOMPOSITION: geowave_pkg.core.store.util.DataStoreUtils.MAX_RANGE_DECOMPOSITION
        }.get(key)
