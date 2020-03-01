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

class SpatialTemporalIndexBuilder(IndexBuilder):
    """
    Index builder for a spatial temporal index.
    """
    
    def __init__(self):
        super().__init__(geowave_pkg.core.geotime.index.api.SpatialTemporalIndexBuilder())
