#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject

class Index(GeoWaveObject):
    """
    Wrapper class for a GeoWave Index.
    """

    def get_name(self):
        """
        Returns:
            The name of the index.
        """
        return self._java_ref.getName()

    def get_index_strategy(self):
        """
        Returns:
            The class name of the index strategy of the index.
        """
        j_obj = self._java_ref.getIndexStrategy()
        return j_obj.getClass().toString()

    def get_index_model(self):
        """
        Returns:
            The class name of the index model of the index.
        """
        j_obj = self._java_ref.getIndexModel()
        return j_obj.getClass().toString()
