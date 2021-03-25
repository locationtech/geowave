#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
from py4j.java_gateway import is_instance_of

from pygw.config import java_gateway


class GeoWaveObject:
    """
    Base Class for pygw objects that wrap Java objects.
    """

    def __init__(self, java_ref):
        self._java_ref = java_ref

    def __repr__(self):
        return "pygw {} => {}".format(self.__class__, self._java_ref)

    def __eq__(self, other):
        if not isinstance(other, GeoWaveObject):
            return False
        return self._java_ref == other._java_ref

    def __str__(self):
        return self.to_string()

    def is_instance_of(self, java_class):
        """
        Returns:
            True if this object is of the type represented by the given java class.
        """
        return is_instance_of(java_gateway, self._java_ref, java_class)

    def to_string(self):
        return self._java_ref.toString()

    def java_ref(self):
        return self._java_ref

    @staticmethod
    def to_java_array(java_class, objects):
        n = len(objects)
        j_arr = java_gateway.new_array(java_class, n)
        for idx, obj in enumerate(objects):
            if not isinstance(obj, GeoWaveObject) or not obj.is_instance_of(java_class):
                print(obj, objects, java_class)
                raise AttributeError("Given object is not compatible with the given class.")
            j_arr[idx] = obj.java_ref()

        return j_arr
