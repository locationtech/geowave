# Pygw Prototype Dev Guide
Intro dev guide for the pygw framework as it exists now in its current form.

## Python Environment Prereqs:
- Python3
- A virtualenv with `requirements.txt` installed

**install steps**
To set up a new virtualenv run `virtualenv -p python3 venv`
Activate the virtualenv with `source venv/bin/activate`
Install requirements with `pip install -r requirements.txt`

## Using pygw
You must have an instance of Geowave Py4J Java Gateway Server running.
Do this by running `org.locationtech.geowave.python.ApiGateway.java` in Eclipse/ IntelliJ

Now, you can import pygw models into your python environment with: 
`from pygw import *`

## Running through the pygw Simple Feature Type Ingest Example
You can step through a simple ingest pipeline (the same one as `org.locationtech.geowave.examples.ingest.SimpleIngest`)
by following these steps:
```
from pygw.sft_example import *

# Create a point feature type
point = Point()

# Create a builder for this feature type
builder = PointBuilder(point)

# Create an adapter for point type
adapter = PointFeatureDataAdapter(point)

# Create a Spatial Index
index = SpatialIndex()

# Create a RocksDB geowave datastore with gw_namespace of "geowave.hello" and that lives under "./world" directory
# NOTE: "./world" is relative to whatever directory your java gateway is running out of.
ds = RocksDbDs("geowave.hello", "./world")

# Registering the point adapter with the spatial index to your datastore
ds.add_type(adapter, index)

# Creating a writer to ingest data
writer = ds.create_writer(adapter.get_type_name())

# Create fake data (Returns a list of java objects that python can iterate over)
j_data = config.GATEWAY.entry_point.simpleIngest.getGriddedFeatures(builder._java_ref, 1000)

# Ingest this data into the datastore using the writer
for data in j_data:
    writer._java_ref.write(data)

# Close the writer
writer.close()
```
## Dev Notes:

### Submodule descriptions
#### __init__.py
The main entry point for pygw is the top-level pygw __init__.py
In that __init__ file, the important child libraries are linked and imported into context.
Also, configurations are setup here.

#### config.py
A singleton object of type GlobalConfigurations called `config` is declared here that encapsulates all module-wide definitions.

In general, to use config definitions in a module, you would do `from .config import config`. Then, you can reference any
declared definitions in the GlobalConfigurations object with something like `config.GATEWAY`.

NOTE: the GlobalConfigurations has an `init()` method. This is INTENTIONALLY not an `__init__` method. The definitions are only populated when `pygw/__init__.py` has imported everything and calls `config.init()`

#### base_models.py
Base Wrapper models for PyGw. Only put objects here that expose major reusable APIs like important Geowave Java interfaces, for example.

##### base_models.PyGWJavaWrapper
This is the base class that EVERY pygw object that encapsulates a java reference must subclass.
Its `_java_ref` field references the java reference in the JVM that is bound to the PyGw python object.

#### debug.py
This exposes a handy function called `print_obj` that you can use to help you debug wonky situations with raw java objects. It will print information about the object in question on both the python side and on the java server side. There's a `verbose` flag that will give you more information about the object in question.

#### stores.py and indices.py
These contain implementation-specific constructors of extended base_model objects, ex. "RocksDbDs" and "RedisDs"

### misc notes:
- "j_"-prefixed notation : I prefix all my raw java reference variables with a "j_" to distinguish them from python variables
- To use call Java Methods that take in PyGw objects (like an Index, for example) you can't directly invoke the method on the PyGW object. You have to call it on the `._java_ref` member of that object.

