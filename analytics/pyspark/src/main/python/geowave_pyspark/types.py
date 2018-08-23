from shapely import wkb
from shapely.geometry import LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon
from shapely.geometry.base import BaseGeometry
from pyspark.sql.types import UserDefinedType, StructField, BinaryType, StructType

class AbstractGeometryUDT(UserDefinedType):
    @classmethod
    def sqlType(cls):
        return StructType([StructField("wkb", BinaryType(), True)])

    @classmethod
    def module(cls):
        return 'geowave_pyspark.types'

    @classmethod
    def scalaUDT(cls):
        return 'mil.nga.giat.geowave.analytic.spark.sparksql.udt.' + cls.__name__

    def serialize(self, obj):
        return _serialize_to_wkb(obj)

    def deserialize(self, datum):
        return _deserialize_from_wkb(datum[0])

class PointUDT(AbstractGeometryUDT):
    pass


class LineStringUDT(AbstractGeometryUDT):
    pass


class PolygonUDT(AbstractGeometryUDT):
    pass


class MultiPointUDT(AbstractGeometryUDT):
    pass


class MultiLineStringUDT(AbstractGeometryUDT):
    pass


class MultiPolygonUDT(AbstractGeometryUDT):
    pass


class GeometryUDT(AbstractGeometryUDT):
    pass


def _serialize_to_wkb(data):
    if isinstance(data, BaseGeometry):
        return bytearray(data.wkb)
    return None


def _deserialize_from_wkb(data):
    if data is None:
        return None
    return wkb.loads(bytes(data))

_deserialize_from_wkb.__safe_for_unpickling__ = True

# Spark expects a private link to the UDT representation of the class
Point.__UDT__ = PointUDT()
MultiPoint.__UDT__ = MultiPointUDT()
LineString.__UDT__ = LineStringUDT()
MultiLineString.__UDT__ = MultiLineStringUDT()
Polygon.__UDT__ = PolygonUDT()
MultiPolygon.__UDT__ = MultiPolygonUDT()
BaseGeometry.__UDT__ = GeometryUDT()

# make Geometry dumps a little cleaner
BaseGeometry.__repr__ = BaseGeometry.__str__