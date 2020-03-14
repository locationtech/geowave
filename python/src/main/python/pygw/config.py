#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
The `config` module includes a singleton object of type GeoWaveConfiguration called `gw_config` that handles all communication between python and the Py4J Java Gateway.
The module includes several shortcut objects to make accessing the gateway more convenient.  These include:
- *`java_gateway`* Py4J Gateway Object
- *`java_pkg`*: Shortcut for `java_gateway.jvm`.  Can be used to construct JVM objects like `java_pkg.org.geotools.feature.simple.SimpleFeatureTypeBuilder()`
- *`geowave_pkg`*: Similar to `java_pkg`, serves as a shortcut for `java_gateway.jvm.org.locationtech.geowave`.
- *`reflection_util`*: Direct access to the Py4J reflection utility.

These objects can be imported directly using `from pygw.config import <object_name>`.

By default, the configuration will attempt to connect to a Py4J Java Gateway running on the local machine.  To use a gateway on a separate machine, the address and port
can be specified with the `PYGW_GATEWAY_ADDRESS` and `PYGW_GATEWAY_PORT` environment variables.  Alternatively, a JavaGateway can be supplied by creating a `pygw_gateway_config`
module on the Python path that contains a JavaGateway named `gateway`.

NOTE: the GeoWaveConfiguration has an `init()` method. This is INTENTIONALLY not an `__init__` method. Initialization is attempted when the configuration is imported.
"""
from os import environ

from py4j.java_gateway import JavaGateway
from py4j.java_gateway import GatewayParameters
from py4j.java_gateway import java_import
from py4j.java_gateway import DEFAULT_ADDRESS
from py4j.java_gateway import DEFAULT_PORT

from py4j.protocol import Py4JNetworkError

PYGW_ADDRESS_ENV = 'PYGW_GATEWAY_ADDRESS'
PYGW_PORT_ENV = 'PYGW_GATEWAY_PORT'

class GeoWaveConfiguration:
    """
    This class sets up communication between Python and the GeoWave logic running
    on a JVM.
    """

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(GeoWaveConfiguration, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.is_initialized = False

    def init(self):
        """
        Sets up the Py4J Gateway, called automatically on import.
        """
        if not self.is_initialized:
            try:
                try:
                    from pygw_gateway_config import gateway
                    self.GATEWAY = gateway
                except ImportError:
                    gateway_address = environ.get(PYGW_ADDRESS_ENV, DEFAULT_ADDRESS)
                    gateway_port = environ.get(PYGW_PORT_ENV, DEFAULT_PORT)
                    self.GATEWAY = JavaGateway(gateway_parameters=GatewayParameters(auto_field=True, address=gateway_address, port=gateway_port))

                self.PKG = self.GATEWAY.jvm
                self.GEOWAVE_PKG = self.GATEWAY.jvm.org.locationtech.geowave

                ### Reflection utility ###
                self.reflection_util= self.GATEWAY.jvm.py4j.reflection.ReflectionUtil
                self.is_initialized = True
            except Py4JNetworkError as e:
                raise GeoWaveConfiguration.PyGwJavaGatewayNotStartedError("The GeoWave Py4J Java Gateway must be running before you can use pygw.") from e

    class PyGwJavaGatewayNotStartedError(Exception): pass

gw_config = GeoWaveConfiguration()
gw_config.init()

java_gateway = gw_config.GATEWAY
"""py4j.java_gateway.JavaGateway: The gateway between pygw and the JVM."""

java_pkg = gw_config.PKG
"""py4j.java_gateway.JVMView: A shortcut for accessing java packages directly.

For example `java_pkg.org.geotools.feature.simple.SimpleFeatureTypeBuilder`.
"""

geowave_pkg = gw_config.GEOWAVE_PKG
"""py4j.java_gateway.JVMView: A shortcut for accessing geowave packages directly.

For example `geowave_pkg.core.store.api.DataStoreFactory`.
"""

reflection_util = gw_config.reflection_util
"""py4j.java_gateway.JavaClass: A Java reflection utility."""

__all__ = ["java_gateway", "java_pkg", "geowave_pkg", "reflection_util"]
