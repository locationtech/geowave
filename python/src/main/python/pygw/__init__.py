#
# Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
# Submodule descriptions
In general each submodule tries to mimic the behavior of the GeoWave Java API.  If there is ever any question about how something should be done with the Python bindings, the answer is most likely the same as how it is done in Java.  The difference being that function names use underscores instead of camel case as is the convention in Java.  For example if the Java version of a class has a function `getName()`, the Python variant would be `get_name()`.

The main difference between the two APIs is how the modules are laid out.  The Python bindings use a simplified module structure to avoid bringing in all the unnecessary complexity of the Java packages that the Java variants belong to.

## config
The `config` module includes a singleton object of type GeoWaveConfiguration called `gw_config` that handles all communication between python and the Py4J Java Gateway.  The module includes several shortcut objects to make accessing the gateway more convenient.  These include:
- *`java_gateway`* Py4J Gateway Object
- *`java_pkg`*: Shortcut for `java_gateway.jvm`.  Can be used to construct JVM objects like `java_pkg.org.geotools.feature.simple.SimpleFeatureTypeBuilder()`
- *`geowave_pkg`*: Similar to `java_pkg`, serves as a shortcut for `java_gateway.jvm.org.locationtech.geowave`.
- *`reflection_util`*: Direct access to the Py4J reflection utility.

These objects can be imported directly using `from pygw.config import <object_name>`.

NOTE: the GeoWaveConfiguration has an `init()` method. This is INTENTIONALLY not an `__init__` method. Initialization is attempted when the configuration is imported.

## base
The `base` module includes common classes that are used by other modules.  This includes the base `GeoWaveObject` class that serves as a python wrapper for a java reference.  It also includes a `type_conversions` submodule that can be used to convert Python types to Java types that are commonly used in GeoWave.

## geotools
The `geotools` module contains classes that wrap the functionality of geotools SimpleFeatures and SimpleFeatureTypes.  These classes can be used to create feature types, features, and data adapters based on simple features.

## index
The `index` module contains classes that are used in creating spatial and spatial/temporal indices.

## query
The `query` module contains classes that are used in constructing queries and their constraints.

## store
The `store` module contains classes that can be used to establish connections to the various GeoWave backends.  Each store type has a submodule which contains a class that can be used to connect to that store type.  For example `from pygw.store.accumulo import AccumuloOptions`.  The `DataStore` object can be constructed by passing the options object to the `DataStoreFactory.create_data_store(<options>)` method.

## debug.py
This exposes a function called `print_obj` that can be used to help with debugging raw java objects. It will print information about the object in question on both the Python side and on the Java server side. There's a `verbose` flag that will give you more information about the object in question.
"""

__version__ = "1.0.0-RC2-SNAPSHOT"
