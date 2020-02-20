#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from .geowave_object import GeoWaveObject

class DataTypeAdapter(GeoWaveObject):
    """
    Base class for data type adapters.
    """

    def get_type_name(self):
        """
        Returns:
            The type name of the data adapter.
        """
        return self._java_ref.getTypeName()
