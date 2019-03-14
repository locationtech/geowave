"""
This is some code that attempts to the wrap the Java ingest example.

A simple pipeline is built out that allows you to stand up a data store, register the appropriate indices and
type adapters, create data, and ingest it. BUT it is not totally fleshed out -- this is just a prototype.

See `org.locationtech.geowave.examples.ingest.SimpleIngest` for more about this example pipeline.

#### To try this example for yourself, in a python shell run: ####
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

# Query everything from the datastore
res = ds.query(Query.everything())

#Iterate over the results
for r in res:
    print_obj(r)

```
"""
from pygw import *

class SimpleFeatureType(PyGwJavaWrapper):
    """Nothing for now. Just a place holder"""
    pass

class Point(SimpleFeatureType):
    """A Point Feature"""
    def __init__(self):
        j_point_feature = config.GATEWAY.entry_point.simpleIngest.createPointFeatureType()
        super().__init__(config.GATEWAY, j_point_feature)

class PointFeatureDataAdapter(DataTypeAdapter):
    """A Point Feature Adapter"""
    def __init__(self, simple_feature_type):
        self.simple_feature_type = simple_feature_type
        j_adapter = config.GATEWAY.entry_point.simpleIngest.createDataAdapter(simple_feature_type._java_ref)
        super().__init__(config.GATEWAY, j_adapter)

class PointBuilder(PyGwJavaWrapper):
    """A Point Builder"""
    def __init__(self, point):
        j_builder = config.MODULE__feature_simple.SimpleFeatureBuilder(point._java_ref)
        super().__init__(config.GATEWAY, j_builder)