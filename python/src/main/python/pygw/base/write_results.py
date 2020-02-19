#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from .geowave_object import GeoWaveObject

class WriteResults(GeoWaveObject):
    """
    Contains the results of a write operation.
    """

    def __init__(self, java_ref):
        super().__init__(java_ref)

    def get_written_index_names(self):
        """
        Gets a list of all the index names that were written to during the write.

        Returns:
            A list of index names that were written to.
        """
        j_index_names = self._java_ref.getWrittenIndexNames().iterator()
        values = []
        while j_index_names.hasNext():
            values.append(j_index_names.next())
        return values

    def get_insertion_ids_written(self, index_name):
        """
        Gets the insertion IDs that were written to the index with the given index name.

        Args:
            index_name (str): The name of the index.
        Returns:
            A list of insertion ids that were written into the index.
        """
        j_insertion_ids = self._java_ref.getInsertionIdsWritten(index_name).getCompositeInsertionIds().iterator()
        values = []
        while j_insertion_ids.hasNext():
            values.append(j_insertion_ids.next())
        return values;

    def is_empty(self):
        """
        Returns:
            True if nothing was written, False otherwise.
        """
        return self._java_ref.isEmpty()
