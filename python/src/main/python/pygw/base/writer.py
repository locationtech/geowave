#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from .geowave_object import GeoWaveObject
from .write_results import WriteResults

class Writer(GeoWaveObject):
    """
    Writes data to a GeoWave data store.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)
        self.is_open = True


    def write(self, data):
        """
        Write data into the associated data store.

        Args:
            data (any) : The data to be written.
        Raises:
            RuntimeError: If the writer is closed.
        Returns:
            A `pygw.base.write_results.WriteResults` with the results of the write operation.
        """
        if not self.is_open:
            raise RuntimeError("Writer is already closed!")

        if isinstance(data, GeoWaveObject):
            data = data._java_ref

        return WriteResults(self._java_ref.write(data))

    def close(self):
        """
        Close the writer.
        """
        if self.is_open:
            self._java_ref.close()
            self.is_open = False
