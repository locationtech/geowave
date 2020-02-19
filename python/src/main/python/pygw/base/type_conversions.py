#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from decimal import Decimal
from datetime import date
from datetime import datetime

from py4j.java_gateway import JavaClass
from shapely.geometry.base import BaseGeometry
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry import Polygon
from shapely.geometry import MultiPoint
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPolygon
from shapely.geometry import GeometryCollection
from shapely.wkb import dumps
from shapely.wkb import loads

from pygw.config import java_pkg
from pygw.config import java_gateway

_wkb_reader = java_pkg.org.locationtech.jts.io.WKBReader()
_wkb_writer = java_pkg.org.locationtech.jts.io.WKBWriter()

def _type_to_string(py_type):
    if (isinstance(py_type, tuple)):
        return ", ".join(t.__name__ for t in py_type)
    else:
        return py_type.__name__


class AttributeType():
    """
    Base class for attributes that can be converted to and from Java variants.
    """

    def __init__(self, binding, py_type):
        self.binding = binding
        self._py_type = py_type

    def to_java(self, value):
        """
        Convert a Python variable into its Java counterpart.

        Args:
            value (any): The Python variable to convert.
        Returns:
            The Java counterpart of the Python variable.
        """
        if value is None:
            return value
        if isinstance(value, self._py_type):
            return self._to_java(value)
        else:
            self._value_error(value, _type_to_string(self._py_type))

    def _to_java(self, value):
        return value

    def from_java(self, value):
        """
        Convert a Java variable into its Python counterpart.

        Args:
            value (any): The Java variable to convert.
        Returns:
            The Python counterpart of the Java variable.
        """
        if value is None:
            return value
        else:
            return self._from_java(value)

    def _from_java(self, value):
        return value

    def _value_error(self, value, expected):
        raise ValueError("Value[%s] should be of type %s." % (str(value), expected))


class ArrayAttributeType(AttributeType):
    """
    Base class for attributes that represent an array of values.
    """

    def __init__(self, subtype, py_type=list):
        self.subtype = subtype
        self._j_class = JavaClass(self.subtype.binding, java_gateway._gateway_client)
        super().__init__("[L%s;" % self.subtype.binding, py_type)

    def _to_java(self, value):
        j_arr = java_gateway.new_array(self._j_class, len(value))
        for i in range(len(value)):
            if value[i] is None:
                continue
            j_arr[i] = self.subtype._to_java(value[i])
        return j_arr

    def _from_java(self, value):
        py_array = []
        for j_obj in value:
            py_array.append(self.subtype.from_java(j_obj))
        return py_array


class BigDecimalType(AttributeType):
    """
    Conversion class for BigDecimal.
    """

    def __init__(self):
        super().__init__("java.math.BigDecimal", Decimal)

    def _to_java(self, value):
        return java_pkg.java.math.BigDecimal(str(value))


class BigDecimalArrayType(ArrayAttributeType):
    """
    Conversion class for BigDecimal[].
    """

    def __init__(self):
        super().__init__(BigDecimalType())


class BigIntegerType(AttributeType):
    """
    Conversion class for BigInteger.
    """

    def __init__(self):
        super().__init__("java.math.BigInteger", int)

    def _to_java(self, value):
        return java_pkg.java.math.BigInteger(str(value))

    def _from_java(self, value):
        return int(value.toString())


class BigIntegerArrayType(ArrayAttributeType):
    """
    Conversion class for BigInteger[].
    """

    def __init__(self):
        super().__init__(BigIntegerType())


class BooleanType(AttributeType):
    """
    Conversion class for Boolean.
    """

    def __init__(self):
        super().__init__("java.lang.Boolean", bool)


class BooleanArrayType(ArrayAttributeType):
    """
    Conversion class for Boolean[].
    """

    def __init__(self):
        super().__init__(BooleanType())


class FloatType(AttributeType):
    """
    Conversion class for Float.
    """

    def __init__(self):
        super().__init__("java.lang.Float", (int, float))

    def _to_java(self, value):
        return value * 1.0


class FloatArrayType(ArrayAttributeType):
    """
    Conversion class for Float[].
    """

    def __init__(self):
        super().__init__(FloatType())


class DoubleType(AttributeType):
    """
    Conversion class for Double.
    """

    def __init__(self):
        super().__init__("java.lang.Double", (int, float))

    def _to_java(self, value):
        return value * 1.0


class DoubleArrayType(ArrayAttributeType):
    """
    Conversion class for Double[].
    """

    def __init__(self):
        super().__init__(DoubleType())


class ByteType(AttributeType):
    """
    Conversion class for Byte.
    """

    def __init__(self):
        super().__init__("java.lang.Byte", int)


class ByteArrayType(ArrayAttributeType):
    """
    Conversion class for Byte[].
    """

    def __init__(self):
        super().__init__(ByteType(), (list, bytes))

    def _from_java(self, value):
        if None in value:
            return super()._from_java(value)
        return bytes(super()._from_java(value))


class ShortType(AttributeType):
    """
    Conversion class for Short.
    """

    def __init__(self):
        super().__init__("java.lang.Short", int)


class ShortArrayType(ArrayAttributeType):
    """
    Conversion class for Short[].
    """

    def __init__(self):
        super().__init__(ShortType())


class IntegerType(AttributeType):
    """
    Conversion class for Integer.
    """

    def __init__(self):
        super().__init__("java.lang.Integer", int)


class IntegerArrayType(ArrayAttributeType):
    """
    Conversion class for Integer[].
    """

    def __init__(self):
        super().__init__(IntegerType())


class LongType(AttributeType):
    """
    Conversion class for Long.
    """

    def __init__(self):
        super().__init__("java.lang.Long", int)


class LongArrayType(ArrayAttributeType):
    """
    Conversion class for Long[].
    """

    def __init__(self):
        super().__init__(LongType())


class PrimitiveArrayType(AttributeType):
    """
    Base class for arrays made up of Java primitives.
    """

    def __init__(self, binding, j_class, py_element_type, py_type=list):
        super().__init__(binding, py_type)
        self._j_class = j_class
        self._py_element_type = py_element_type

    def _to_java(self, value):
        j_arr = self._build_array(value)
        for i in range(len(value)):
            if value[i] is None:
                raise ValueError("Value at index %d should not be None for primitive array." % i)
            if isinstance(value[i], self._py_element_type):
                j_arr[i] = self._value_to_java(value[i])
            else:
                self._value_error(value[i], _type_to_string(self._py_element_type))
        return j_arr

    def _from_java(self, value):
        py_array = []
        for j_obj in value:
            py_array.append(self._value_from_java(j_obj))
        return py_array

    def _build_array(self, value):
        return java_gateway.new_array(self._j_class, len(value))

    def _value_to_java(self, value):
        return value

    def _value_from_java(self, value):
        return value


class PrimitiveBooleanArrayType(PrimitiveArrayType):
    """
    Conversion class for boolean[].
    """

    def __init__(self):
        super().__init__("[Z", java_gateway.jvm.boolean, bool)


class PrimitiveFloatArrayType(PrimitiveArrayType):
    """
    Conversion class for float[].
    """

    def __init__(self):
        super().__init__("[F", java_gateway.jvm.float, (int, float))

    def _value_to_java(self, value):
        return value * 1.0


class PrimitiveDoubleArrayType(PrimitiveArrayType):
    """
    Conversion class for double[].
    """

    def __init__(self):
        super().__init__("[D", java_gateway.jvm.double, (int, float))

    def _value_to_java(self, value):
        return value * 1.0


class PrimitiveByteArrayType(PrimitiveArrayType):
    """
    Conversion class for byte[].
    """

    def __init__(self):
        super().__init__("[B", java_gateway.jvm.byte, int, (list, bytes))

    def _to_java(self, value):
        if isinstance(value, bytes):
            return value
        return bytes(super()._to_java(value))

    def _from_java(self, value):
        if isinstance(value, bytes):
            return value
        return bytes(super()._from_java(value))

    def _value_to_java(self, value):
        return value % 256

    def _build_array(self, value):
        return bytearray(super()._build_array(value))


class PrimitiveShortArrayType(PrimitiveArrayType):
    """
    Conversion class for short[].
    """

    def __init__(self):
        super().__init__("[S", java_gateway.jvm.short, int)


class PrimitiveIntArrayType(PrimitiveArrayType):
    """
    Conversion class for int[].
    """

    def __init__(self):
        super().__init__("[I", java_gateway.jvm.int, int)


class PrimitiveLongArrayType(PrimitiveArrayType):
    """
    Conversion class for long[].
    """

    def __init__(self):
        super().__init__("[J", java_gateway.jvm.long, int)


class StringType(AttributeType):
    """
    Conversion class for String.
    """

    def __init__(self):
        super().__init__("java.lang.String", str)


class StringArrayType(ArrayAttributeType):
    """
    Conversion class for String[].
    """

    def __init__(self):
        super().__init__(StringType())


class DateType(AttributeType):
    """
    Conversion class for Date.
    """

    def __init__(self):
        super().__init__("java.util.Date", date)

    def _to_java(self, value):
        return java_pkg.java.util.Date(int(value.timestamp() * 1000))

    def _from_java(self, value):
        return datetime.fromtimestamp(value.getTime() / 1000)


class DateArrayType(ArrayAttributeType):
    """
    Conversion class for Date[].
    """

    def __init__(self):
        super().__init__(DateType())


class CalendarType(AttributeType):
    """
    Conversion class for Calendar.
    """

    def __init__(self):
        super().__init__("java.util.Calendar", date)
        self._date_type = DateType()

    def _to_java(self, value):
        j_timezone = java_pkg.java.util.TimeZone.getTimeZone("GMT")
        j_calendar = java_pkg.java.util.Calendar.getInstance(j_timezone)
        j_calendar.setTime(self._date_type._to_java(value))
        return j_calendar

    def _from_java(self, value):
        return self._date_type._from_java(value.getTime())


class CalendarArrayType(ArrayAttributeType):
    """
    Conversion class for Calendar[].
    """

    def __init__(self):
        super().__init__(CalendarType())


class BaseGeometryType(AttributeType):
    """
    Base type for conversion between shapely geometries and JTS geometries.
    """

    def __init__(self, binding, py_type):
        super().__init__(binding, py_type)

    def _to_java(self, value):
        wkb = dumps(value)
        return _wkb_reader.read(wkb)

    def _from_java(self, value):
        wkb = _wkb_writer.write(value)
        return loads(wkb)


class PointType(BaseGeometryType):
    """
    Conversion class for Point.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.Point", Point)


class PointArrayType(ArrayAttributeType):
    """
    Conversion class for Point[].
    """

    def __init__(self):
        super().__init__(PointType())


class MultiPointType(BaseGeometryType):
    """
    Conversion class for MultiPoint.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.MultiPoint", MultiPoint)


class MultiPointArrayType(ArrayAttributeType):
    """
    Conversion class for MultiPoint[].
    """

    def __init__(self):
        super().__init__(MultiPointType())


class LineStringType(BaseGeometryType):
    """
    Conversion class for LineString.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.LineString", LineString)


class LineStringArrayType(ArrayAttributeType):
    """
    Conversion class for LineString[].
    """

    def __init__(self):
        super().__init__(LineStringType())


class MultiLineStringType(BaseGeometryType):
    """
    Conversion class for MultiLineString.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.MultiLineString", MultiLineString)


class MultiLineStringArrayType(ArrayAttributeType):
    """
    Conversion class for MultiLineString[].
    """

    def __init__(self):
        super().__init__(MultiLineStringType())


class PolygonType(BaseGeometryType):
    """
    Conversion class for Polygon.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.Polygon", Polygon)


class PolygonArrayType(ArrayAttributeType):
    """
    Conversion class for Polygon[].
    """

    def __init__(self):
        super().__init__(PolygonType())


class MultiPolygonType(BaseGeometryType):
    """
    Conversion class for MultiPolygon.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.MultiPolygon", MultiPolygon)


class MultiPolygonArrayType(ArrayAttributeType):
    """
    Conversion class for MultiPolygon[].
    """

    def __init__(self):
        super().__init__(MultiPolygonType())


class GeometryCollectionType(BaseGeometryType):
    """
    Conversion class for GeometryCollection.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.GeometryCollection", GeometryCollection)


class GeometryCollectionArrayType(ArrayAttributeType):
    """
    Conversion class for GeometryCollection[].
    """

    def __init__(self):
        super().__init__(GeometryCollectionType())


class GeometryType(BaseGeometryType):
    """
    Conversion class for Geometry.
    """

    def __init__(self):
        super().__init__("org.locationtech.jts.geom.Geometry", BaseGeometry)


class GeometryArrayType(ArrayAttributeType):
    """
    Conversion class for Geometry[].
    """

    def __init__(self):
        super().__init__(GeometryType())
