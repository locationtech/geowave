from pygw.sft_example import *
from pygw.base_models import DataTypeAdapter
from .base_models import PyGwJavaWrapper

def simple_test():
	point = Point()
	adapter = PointFeatureDataAdapter(point)
	index = SpatialIndex()
	ds = RocksDbDs("geowave.hello", "./world")
	return ds

def test_answer():
	assert simple_test()
