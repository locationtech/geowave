from pygw.config import config
from pygw.base_models import PyGwJavaWrapper, DataTypeAdapter

class SimpleFeatureType(PyGwJavaWrapper):
    """Nothing for now. Just a place holder"""
    pass

class SimpleFeatureDataAdapter(DataTypeAdapter):
    def __init__(self, feature_type):
        self.feature_type = feature_type
        assert isinstance(feature_type, SimpleFeatureType)
        j_feat_type = feature_type._java_ref
        j_feat_adapter = config.MODULE__adapter_vector.FeatureDataAdapter(j_feat_type)
        super().__init__(config.GATEWAY, j_feat_adapter)