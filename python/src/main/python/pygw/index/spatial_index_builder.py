#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import geowave_pkg

from .index_builder import IndexBuilder
from .index import Index

class SpatialIndexBuilder(IndexBuilder):
    """
    Index builder for a spatial index.
    """

    def __init__(self):
        super().__init__(geowave_pkg.core.geotime.index.api.SpatialIndexBuilder())

    def set_include_time_in_common_index_model(self, include):
        """
        Sets whether or not to include time in the common index model.  This can be
        used to speed up queries that may involve temporal constraints.

        Args:
            include (bool): Whether or not to include time in the common index model.
        Returns:
            This index builder.
        """
        self._java_ref.setIncludeTimeInCommonIndexModel(include)
        return self
