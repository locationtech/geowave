#
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================

from .geowave_object import GeoWaveObject
from .java_transformer import NoOpTransformer


class CloseableIterator(GeoWaveObject):
    """
    A wrapper for GeoWave CloseableIterators.
    """

    def __init__(self, java_ref, java_transformer=NoOpTransformer()):
        self._java_transformer = java_transformer
        super().__init__(java_ref)

    def __iter__(self):
        return self

    def __next__(self):
        if self._java_ref.hasNext():
            return self._java_transformer.transform(self._java_ref.next())
        else:
            raise StopIteration

    def close(self):
        """
        Closes the Java CloseableIterator.
        """
        self._java_ref.close()
