#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================
"""
This project aims to provide Python classes that allow users to interact with a GeoWave data store using the same workflows that are available in the programmatic Java API.

## Environment
- Python >=3,<=3.7
- A virtualenv with `requirements.txt` installed

### Installation
- Set up virtualenv: `virtualenv -p python3 venv`
- Activate virtualenv: `source venv/bin/activate`
- Install requirements: `pip install -r requirements.txt`

## Usage
In order to use `pygw`, you must have an instance of GeoWave Py4J Java Gateway Server running.  The gateway can be started by using the GeoWave command `geowave python rungateway`.

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
"""
from pkg_resources import get_distribution
from pkg_resources import DistributionNotFound

try:
    version = get_distribution('pygw').version
except DistributionNotFound:
    from maven_version import get_maven_version
    version = get_maven_version();

__version__ = version
