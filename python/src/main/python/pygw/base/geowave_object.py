#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import java_gateway

class GeoWaveObject:
    """
    Base Class for pygw objects that wrap Java objects.
    """

    def __init__(self, java_ref):
        self._java_ref = java_ref

    def __repr__(self):
        return "pygw {} Object with JavaRef@{}".format(self.__class__, self._java_ref)

    def __eq__(self, other):
        if not isinstance(other, PyGwJavaWrapper):
            return False
        return self._java_ref == other._java_ref

    def is_instance_of(self, java_class):
        """
        Returns:
            True if this object is of the type represented by the given java class.
        """
        return isinstance(java_gateway, self._java_ref, java_class)
