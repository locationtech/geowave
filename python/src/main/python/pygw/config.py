#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
# ===============================================================================================
"""
The `config` module includes several shortcut objects to make accessing the Py4J Java gateway more convenient.  These
include:
- *`java_gateway`* Py4J Gateway Object
- *`java_pkg`*: Shortcut for `java_gateway.jvm`.  Can be used to construct JVM objects like
      `java_pkg.org.geotools.feature.simple.SimpleFeatureTypeBuilder()`
- *`geowave_pkg`*: Similar to `java_pkg`, serves as a shortcut for `java_gateway.jvm.org.locationtech.geowave`.
- *`reflection_util`*: Direct access to the Py4J reflection utility.

These objects can be imported directly using `from pygw.config import <object_name>`.

This module uses the `gateway` module to connect to the Py4J Java gateway.  By default, the gateway module will attempt
to connect to a locally running gateway.  See the documentation of that module for information about configuring `pygw`
to use other gateways.
"""
from pygw.gateway import gateway_config

gateway_config.init()

java_gateway = gateway_config.GATEWAY
"""py4j.java_gateway.JavaGateway: The gateway between pygw and the JVM."""

java_pkg = gateway_config.GATEWAY.jvm
"""py4j.java_gateway.JVMView: A shortcut for accessing java packages directly.

For example `java_pkg.org.geotools.feature.simple.SimpleFeatureTypeBuilder`.
"""

geowave_pkg = gateway_config.GATEWAY.jvm.org.locationtech.geowave
"""py4j.java_gateway.JVMView: A shortcut for accessing geowave packages directly.

For example `geowave_pkg.core.store.api.DataStoreFactory`.
"""

reflection_util = gateway_config.GATEWAY.jvm.py4j.reflection.ReflectionUtil
"""py4j.java_gateway.JavaClass: A Java reflection utility."""

__all__ = ["java_gateway", "java_pkg", "geowave_pkg", "reflection_util"]
