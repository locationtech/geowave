from pygw.geotools import SimpleFeatureType
from pygw.base_models import DataTypeAdapter, PyGwJavaWrapper
from pygw.config import config

class Point(SimpleFeatureType):
    """A Point Feature"""
    def __init__(self):
        j_point_feature = config.GATEWAY.entry_point.getSimpleIngest().createPointFeatureType()
        PyGwJavaWrapper.__init__(self,config.GATEWAY,j_point_feature)

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