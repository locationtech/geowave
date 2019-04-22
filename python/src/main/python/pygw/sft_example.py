"""
This is some code that attempts to the wrap the Java ingest example.

A simple pipeline is built out that allows you to stand up a data store, register the appropriate indices and
type adapters, create data, and ingest it. BUT it is not totally fleshed out -- this is just a prototype.

See `org.locationtech.geowave.examples.ingest.SimpleIngest` for more about this example pipeline.

#### To try this example for yourself, in a python shell run: ####
```

from pygw import *
from pygw.sft_example import *
point = Point()
builder = PointBuilder(point)
adapter = PointFeatureDataAdapter(point)
index = SpatialIndex()
ds = RocksDbDs("geowave.hello", "./world")
ds.add_type(adapter, index)
ds.get_indices()
ds2 = RocksDbDs("geowave.hello2","./world2")

ds2.get_indices()

ds.copyTo(ds2)

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
from enum import Enum

class SimpleFeatureType():
    def __init__(self, java_ref):
        super().__init__(config.GATEWAY, java_ref)

    def build(self, time=None, name=None):
        sftb= SimpleFeatureType()
        sftb.set_time(time)
        sftb.set_name(name)
        
        cls(sftb.buildFeatureType())

class SimpleFeatureTypeBuilder(PyGwJavaWrapper):
    def __init__(self):
        j_sftbuilder = config.MODULE__simple_feature.SimpleFeatureTypeBuilder
        super().__init__(config.GATEWAY, j_qbuilder)

    def set_time(self, time=None):
        assert isinstance(time, str)
        self._java_ref.add(time)

    def set_name(self, name=None):
        assert isinstance(name, str)
        self._java_ref.setName(name)

class SimpleFeatureType(PyGwJavaWrapper):
    def __init__(self, feature_name, *attributes):
        j_builder= config.MODULE__feature_simple.SimpleFeatureBuilder()
        j_builder.setName(feature_name)
        for a in attributes:
            j_builder.add(a._java_ref)
        j_builder.buildFeatureType()
        super().__init__(config.GATEWAY, j_builder)

class FeatureTypeAttribute(PyGwJavaWrapper):
    def __init__(self, type_, is_nilable, descriptor):
        self.type_= type_
        self.is_nilable = is_nilable
        self.descriptor = descriptor
        j_builder = config.MODULE__feature.AttributeTypeBuilder()

        assert isinstance(type_, FeatureTypeAttribute.Type)
        j_type_cls = config.reflection_util.classForName(type_.value)

        j_builder.binding(j_type_cls)
        j_builder.nillable(is_nilable)
        j_attribute = j_builder.buildDescriptor(descriptor)
        super().__init__(config.GATEWAY, j_attribute)

    @classmethod
    def string(cls, is_nilable, descriptor):
        return cls(cls.Type.STRING, is_nilable, descriptor)

    class Type(Enum):
        STRING = "java.lang.String"
        DOUBLE = "java.lang.Double"
        DATE = "java.util.Date"
        GEOMETRY = "org.locationtech.jts.geom.Geometry"



class Point(SimpleFeatureType):
    """A Point Feature"""
    def __init__(self):
        j_point_feature = config.GATEWAY.entry_point.getSimpleIngest().createPointFeatureType()
        super().__init__(config.GATEWAY, j_point_feature)
        #most like needs adapter, some named dimensions
        #might need builder class
        #want to use constuctor pattern, instead of build pattern
        #queury.py

class PointFeatureDataAdapter(DataTypeAdapter):
    """A Point Feature Adapter"""
    def __init__(self, simple_feature_type):
        self.simple_feature_type = simple_feature_type
        j_adapter = config.GATEWAY.entry_point.getSimpleIngest().createDataAdapter(simple_feature_type._java_ref)
        super().__init__(config.GATEWAY, j_adapter)

class PointBuilder(PyGwJavaWrapper):
    """A Point Builder"""
    def __init__(self, point):
        j_builder = config.MODULE__feature_simple.SimpleFeatureBuilder(point._java_ref)
        super().__init__(config.GATEWAY, j_builder)