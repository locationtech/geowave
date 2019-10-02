# pygw
This project aims to provide Python classes that allow users to interact with a GeoWave data store using the same workflows that are available in the programmatic Java API.

## Environment
- Python >=3,<=3.7
- A virtualenv with `requirements.txt` installed
- A running GeoWave Java Gateway

### Installation From Source
- Clone GeoWave: `git clone https://github.com/locationtech/geowave.git`
- Navigate to python directory: `cd geowave/python/src/main/python`
- Set up virtualenv: `virtualenv -p python3 venv`
- Activate virtualenv: `source venv/bin/activate`
- Install requirements: `pip install -r requirements.txt`

## Usage
In order to use `pygw`, you must have an instance of GeoWave Py4J Java Gateway Server running.  The gateway can be started by using the GeoWave command `geowave util python rungateway`.

You can then import `pygw` classes into your Python environment.

## Example
The following is an example of how `pygw` might be used to write and query some feature data:
```python
from datetime import datetime

from shapely.geometry import Point

from pygw.store import DataStoreFactory
from pygw.store.rocksdb import RocksDBOptions
from pygw.geotools import SimpleFeatureBuilder
from pygw.geotools import SimpleFeatureTypeBuilder
from pygw.geotools import AttributeDescriptor
from pygw.geotools import FeatureDataAdapter
from pygw.index import SpatialIndexBuilder
from pygw.query import VectorQueryBuilder

# Create a RocksDB data store
options = RocksDBOptions()
options.set_geowave_namespace("geowave.example")
# NOTE: Directory is relative to the JVM working directory.
options.set_directory("./datastore")
datastore = DataStoreFactory.create_data_store(options)

# Create a point feature type
point_type_builder = SimpleFeatureTypeBuilder()
point_type_builder.set_name("TestPointType")
point_type_builder.add(AttributeDescriptor.point("the_geom"))
point_type_builder.add(AttributeDescriptor.date("date"))
point_type = point_type_builder.build_feature_type()

# Create a builder for this feature type
point_feature_builder = SimpleFeatureBuilder(point_type)

# Create an adapter for point type
point_type_adapter = FeatureDataAdapter(point_type)

# Create a Spatial Index
index = SpatialIndexBuilder().create_index()

# Registering the point adapter with the spatial index to your datastore
datastore.add_type(point_type_adapter, index)

# Creating a writer to ingest data
writer = datastore.create_writer(point_type_adapter.get_type_name())

# Write some features to the data store
point_feature_builder.set_attr("the_geom", Point(1, 1))
point_feature_builder.set_attr("date", datetime.now())
writer.write(point_feature_builder.build("feature1"))

point_feature_builder.set_attr("the_geom", Point(5, 5))
point_feature_builder.set_attr("date", datetime.now())
writer.write(point_feature_builder.build("feature2"))

point_feature_builder.set_attr("the_geom", Point(-5, -5))
point_feature_builder.set_attr("date", datetime.now())
writer.write(point_feature_builder.build("feature3"))

# Close the writer
writer.close()

# Query the data (with no constraints)
query = VectorQueryBuilder().build()
results = datastore.query(query)
for feature in results:
    print(feature.get_id())
    print(feature.get_default_geometry())
results.close()
```
## Dev Notes:

### Building a distributable wheel
To build a wheel file for `pygw`, simply execute the command `python setup.py bdist_wheel --python-tag=py3` under the active virtual environment.  This will create a distributable wheel under the `dist` directory.

### Building API documentation
This project has been documented using Python docstrings.  These can be used to generate full API documentation in HTML form. To generate the documentation, perform the following steps:
 - Ensure that the GeoWave Py4J Java Gateway Server is running: `geowave util python rungateway`
 - Generate documentation: `pdoc --html pygw`

 Note: This command requires that the python virtual environment is active and that the `pygw` requirements have been installed.  This will generate API documentation in the `html/pygw` directory.

### Submodule descriptions
In general each submodule tries to mimic the behavior of the GeoWave Java API.  If there is ever any question about how something should be done with the Python bindings, the answer is most likely the same as how it is done in Java.  The difference being that function names use underscores instead of camel case as is the convention in Java.  For example if the Java version of a class has a function `getName()`, the Python variant would be `get_name()`.

The main difference between the two APIs is how the modules are laid out.  The Python bindings use a simplified module structure to avoid bringing in all the unnecessary complexity of the Java packages that the Java variants belong to.

#### config
The `config` module includes a singleton object of type GeoWaveConfiguration called `gw_config` that handles all communication between python and the Py4J Java Gateway.  The module includes several shortcut objects to make accessing the gateway more convenient.  These include:
- *`java_gateway`* Py4J Gateway Object
- *`java_pkg`*: Shortcut for `java_gateway.jvm`.  Can be used to construct JVM objects like `java_pkg.org.geotools.feature.simple.SimpleFeatureTypeBuilder()`
- *`geowave_pkg`*: Similar to `java_pkg`, serves as a shortcut for `java_gateway.jvm.org.locationtech.geowave`.
- *`reflection_util`*: Direct access to the Py4J reflection utility.

These objects can be imported directly using `from pygw.config import <object_name>`.

NOTE: the GeoWaveConfiguration has an `init()` method. This is INTENTIONALLY not an `__init__` method. Initialization is attempted when the configuration is imported.

#### base
The `base` module includes common classes that are used by other modules.  This includes the base `GeoWaveObject` class that serves as a python wrapper for a java reference.  It also includes a `type_conversions` submodule that can be used to convert Python types to Java types that are commonly used in GeoWave.

#### geotools
The `geotools` module contains classes that wrap the functionality of geotools SimpleFeatures and SimpleFeatureTypes.  These classes can be used to create feature types, features, and data adapters based on simple features.

#### index
The `index` module contains classes that are used in creating spatial and spatial/temporal indices.

#### query
The `query` module contains classes that are used in constructing queries and their constraints.

#### store
The `store` module contains classes that can be used to establish connections to the various GeoWave backends.  Each store type has a submodule which contains a class that can be used to connect to that store type.  For example `from pygw.store.accumulo import AccumuloOptions`.  The `DataStore` object can be constructed by passing the options object to the `DataStoreFactory.create_data_store(<options>)` method.

#### debug.py
This exposes a function called `print_obj` that can be used to help with debugging raw java objects. It will print information about the object in question on both the Python side and on the Java server side. There's a `verbose` flag that will give you more information about the object in question.

### Notes:
- `j_`-prefixed notation : Java reference variables are prefixed with `j_` in order to distinguish them from Python variables
