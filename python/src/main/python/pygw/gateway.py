#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
"""
The `gateway` module includes a singleton object of type GatewayConfiguration called `gateway_config` that handles all
communication between python and the Py4J Java Gateway.

By default, the configuration will attempt to connect to a Py4J Java Gateway running on the local machine.  To use a
gateway on a separate machine, the address and port can be specified with the `PYGW_GATEWAY_ADDRESS` and
`PYGW_GATEWAY_PORT` environment variables.  Alternatively, this module can be used to provide pygw with a Py4J Java
Gateway that has already been initialized by importing the `gateway_config` from this module and calling `set_gateway`
with the gateway object prior to importing any other pygw classes.  It is important to note that GeoWave must be on the
classpath of the gateway that is being provided.

The following snippet shows how a custom gateway (such as one from pyspark) can be provided:
```python
# This snippet should only be called once at the beginning of the application lifecycle
from pygw.gateway import gateway_config
gateway_config.set_gateway(sparkContext._gateway)

# Continue to use pygw normally
```

NOTE: the GatewayConfiguration has an `init()` method. This is INTENTIONALLY not an `__init__` method. Initialization is
attempted when the configuration is imported.
"""
from os import environ

from py4j.java_gateway import JavaGateway
from py4j.java_gateway import GatewayParameters
from py4j.java_gateway import DEFAULT_ADDRESS
from py4j.java_gateway import DEFAULT_PORT

from py4j.protocol import Py4JNetworkError

PYGW_ADDRESS_ENV = 'PYGW_GATEWAY_ADDRESS'
PYGW_PORT_ENV = 'PYGW_GATEWAY_PORT'


class GatewayConfiguration:
    """
    This class sets up communication between Python and the GeoWave logic running
    on a JVM.
    """

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(GatewayConfiguration, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.is_initialized = False
        self.GATEWAY = None
        self.geowave_version = None

    def set_gateway(self, gateway):
        if self.GATEWAY is not None:
            raise GatewayConfiguration.PyGwJavaGatewayAlreadyInitializedError(
                "The pygw gateway has already been initialized. Set the gateway before importing any other pygw "
                "classes.")
        self.GATEWAY = gateway

    def init(self):
        """
        Sets up the Py4J Gateway.  This is called automatically when other pygw classes are imported.
        """
        if not self.is_initialized:
            try:
                if self.GATEWAY is None:
                    gateway_address = environ.get(PYGW_ADDRESS_ENV, DEFAULT_ADDRESS)
                    gateway_port = environ.get(PYGW_PORT_ENV, DEFAULT_PORT)
                    self.GATEWAY = JavaGateway(
                        gateway_parameters=GatewayParameters(auto_field=True, address=gateway_address,
                                                             port=gateway_port))
                    try:
                        self.geowave_version = self.GATEWAY.jvm.org.locationtech.geowave.core.cli.VersionUtils\
                            .getVersion()
                    except TypeError as e:
                        raise GatewayConfiguration.PyGwGeoWaveNotFoundInGateway(
                            "GeoWave was not found in the configured gateway.  Make sure GeoWave jars are available "
                            "on the classpath of the running gateway.") from e

                    self.is_initialized = True

            except Py4JNetworkError as e:
                raise GatewayConfiguration.PyGwJavaGatewayNotStartedError(
                    "The GeoWave Py4J Java Gateway must be running before you can use pygw.") from e

    class PyGwGeoWaveNotFoundInGateway(Exception):
        pass

    class PyGwJavaGatewayNotStartedError(Exception):
        pass

    class PyGwJavaGatewayAlreadyInitializedError(Exception):
        pass


gateway_config = GatewayConfiguration()

__all__ = ["gateway_config"]
