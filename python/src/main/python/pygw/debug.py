#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
This module exposes a function called `print_obj` that can be used to help with debugging raw java objects. It will print information about the object in question on both the Python side and on the Java server side. There's a `verbose` flag that will give you more information about the object in question.
"""
from pygw.config import java_gateway
from pygw.base import GeoWaveObject

def print_obj(to_print, verbose=False):
    """
    Print method to help with debugging.
    """
    if isinstance(to_print, GeoWaveObject):
        to_print = to_print._java_ref
    print(java_gateway.entry_point.getDebug().printObject(to_print, verbose))
