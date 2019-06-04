from pygw.config import config
from pygw.base_models import PyGwJavaWrapper, DataTypeAdapter
from enum import Enum

class SimpleFeature(PyGwJavaWrapper):
    """
    A Simple (Vector) Feature.
    """
    def __init__(self, type_, id, java_ref):
        assert isinstance(type_, SimpleFeatureType)
        self.type_ = type_
        self.id = id
        super().__init__(config.GATEWAY, java_ref)

class SimpleFeatureBuilder(PyGwJavaWrapper):
    """
    Builds SimpleFeature instances for a given SimpleFeatureType.
    """

    def __init__(self, type_):
        assert isinstance(type_, SimpleFeatureType)
        self.type_ = type_
        self.attributes = { a.descriptor : a for a in type_.attributes }
        j_builder = config.MODULE__feature_simple.SimpleFeatureBuilder(type_._java_ref)
        super().__init__(config.GATEWAY, j_builder)

    def set_attr(self, descriptor, value):
        if descriptor not in self.attributes:
            raise SimpleFeatureBuilder.NoSuchAttributeInTypeError("No matching attribute for {}".format(descriptor))
        attr_switch = {
        # Defines a mapping from attribute type to function to set it
        # Each function MUST BE type: (attribute, value) -> None
            SimpleFeatureTypeAttribute.Type.STRING : self._set_string,
            SimpleFeatureTypeAttribute.Type.INTEGER : self._set_integer, 
            SimpleFeatureTypeAttribute.Type.DOUBLE : self._set_double,
            SimpleFeatureTypeAttribute.Type.GEOMETRY : self._set_geometry,
            SimpleFeatureTypeAttribute.Type.DATE : self._set_date       # TODO
        }
        attr = self.attributes[descriptor]
        attr_switch[attr.type_](attr, value)

    def _set_string(self, attr, value):
        assert isinstance(value, str)
        self._java_ref.set(attr.descriptor, value)

    def _set_integer(self, attr, value):
        assert isinstance(value, int)
        self._java_ref.set(attr.descriptor, value)
    
    def _set_double(self, attr, value):
        assert (isinstance(value, int) or isinstance(value, float))
        value = value * 1.0
        self._java_ref.set(attr.descriptor, value)

    def _set_geometry(self, attr, value):
        # TODO Need to give better error.
        assert isinstance(value, tuple)
        for v in value:
            assert (isinstance(v, int) or isinstance(v, float))
        vals = [float(v) for v in value]
        if len(vals) == 2:
            j_coord = config.MODULE__jts_geom.Coordinate(vals[0], vals[1])
        elif len(vals) == 3:
            j_coord = config.MODULE__jts_geom.Coordinate(vals[0], vals[1], vals[2])
        else:
            raise Exception("Unsupported number of args to Coordinate")
        j_geom = config.MODULE__geotime_util.GeometryUtils.GEOMETRY_FACTORY.createPoint(j_coord)
        self._java_ref.set(attr.descriptor, j_geom)

    def _set_date(self, attr, value):
        # TODO
        # Thoughts for this: Python date -> String -> Java Date from String
        pass

    def build(self, id):
        j_feature = self._java_ref.buildFeature(str(id))
        return SimpleFeature(self.type_, id, j_feature)

    class NoSuchAttributeInTypeError(Exception): pass
    
class SimpleFeatureType(PyGwJavaWrapper):
    """
    Defines a Schema for a SimpleFeature / Vector Feature.
    """

    def __init__(self, feature_name, *attributes):
        j_builder = config.MODULE__feature_simple.SimpleFeatureTypeBuilder()
        j_builder.setName(feature_name)
        for a in attributes:
            assert isinstance(a, SimpleFeatureTypeAttribute)
            j_builder.add(a._java_ref)
        self.attributes = attributes
        self.name = feature_name
        j_feature_type = j_builder.buildFeatureType()
        super().__init__(config.GATEWAY, j_feature_type)
        self.feature_builder = SimpleFeatureBuilder(self)
        
    def get_name(self):
        return self.name
        
    def get_feature_builder(self):
        return self.feature_builder

    def get_type_adapter(self):
        return SimpleFeatureDataAdapter(self)

    def create_feature(self, id, **kwargs):
        for desc, val in kwargs.items():
            self.feature_builder.set_attr(desc, val)
        return self.feature_builder.build(id)

class SimpleFeatureTypeAttribute(PyGwJavaWrapper):
    def __init__(self, type_, is_nilable, descriptor):
        self.type_= type_
        self.is_nilable = is_nilable
        self.descriptor = descriptor
        j_builder = config.MODULE__feature.AttributeTypeBuilder()

        if not isinstance(type_, SimpleFeatureTypeAttribute.Type):
            raise SimpleFeatureTypeAttribute.UnknownTypeError("Invalid argument to `type_`. Must be one of defined types in FeatureTypeAttribute.Type")

        j_type_cls = config.reflection_util.classForName(type_.value)

        j_builder.binding(j_type_cls)
        j_builder.nillable(is_nilable)
        j_attribute = j_builder.buildDescriptor(descriptor)
        super().__init__(config.GATEWAY, j_attribute)

    @classmethod
    def string(cls, descriptor, is_nilable=False):
        return cls(cls.Type.STRING, is_nilable, descriptor)

    @classmethod
    def date(cls, descriptor, is_nilable=False):
        return cls(cls.Type.DATE, is_nilable, descriptor)

    @classmethod
    def double(cls, descriptor, is_nilable=False):
        return cls(cls.Type.DOUBLE, is_nilable, descriptor)

    @classmethod
    def integer(cls, descriptor, is_nilable=False):
        return cls(cls.Type.INTEGER, is_nilable, descriptor)
    
    @classmethod
    def geometry(cls, descriptor, is_nilable=False):
        return cls(cls.Type.GEOMETRY, is_nilable, descriptor)

    class Type(Enum):
        # TODO: Support for more types
        STRING = "java.lang.String"
        DOUBLE = "java.lang.Double"
        DATE = "java.util.Date"
        GEOMETRY = "org.locationtech.jts.geom.Geometry"
        INTEGER = "java.lang.Integer"

    class UnknownTypeError(Exception): pass

class SimpleFeatureDataAdapter(DataTypeAdapter):
    def __init__(self, feature_type):
        self.feature_type = feature_type
        assert isinstance(feature_type, SimpleFeatureType)
        j_feat_type = feature_type._java_ref
        j_feat_adapter = config.MODULE__adapter_vector.FeatureDataAdapter(j_feat_type)
        super().__init__(config.GATEWAY, j_feat_adapter)