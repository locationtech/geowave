#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base import GeoWaveObject

from .index import Index

class IndexBuilder(GeoWaveObject):
    """
    Base class for building indices.
    """

    def set_num_partitions(self, num_partitions):
        """
        Sets the number of partitions for the index to use.

        Args:
            num_partitions (int): The number of partitions to use.
        Returns:
            This index builder.
        """
        self._java_ref.setNumPartitions(num_partitions)
        return self

    def set_name(self, index_name):
        """
        Set the name of the index to the given value.

        Args:
            index_name (str): The name to use.
        Returns:
            This index builder.
        """
        self._java_ref.setName(index_name)
        return self

    def create_index(self):
        """
        Builds the configured index.

        Returns:
            A `pygw.index.index.Index` with the given configuration.
        """
        j_idx = self._java_ref.createIndex()
        return Index(j_idx)
