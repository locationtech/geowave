#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from datetime import datetime
from decimal import Decimal

from py4j.java_gateway import is_instance_of
from shapely.geometry import *

from pygw.base.type_conversions import *
from pygw.config import java_gateway

def java_round_trip(type, value, check_binding=True):
    j_value = type.to_java(value)

    if value is not None and check_binding:
        assert is_instance_of(java_gateway, j_value, type.binding)

    py_value = type.from_java(j_value)
    assert value == py_value

def test_big_decimal():
    big_decimal_type = BigDecimalType()
    java_round_trip(big_decimal_type, Decimal('1.1234'))
    java_round_trip(big_decimal_type, None)

    big_decimal_array_type = BigDecimalArrayType()
    java_round_trip(big_decimal_array_type, [Decimal('1.1234'), None, Decimal('-15.821')])
    java_round_trip(big_decimal_array_type, [])
    java_round_trip(big_decimal_array_type, None)

def test_big_integer():
    big_integer_type = BigIntegerType()
    java_round_trip(big_integer_type, 1234567890123456789012345678901234567890)
    java_round_trip(big_integer_type, 123231)
    java_round_trip(big_integer_type, None)

    big_integer_array_type = BigIntegerArrayType()
    java_round_trip(big_integer_array_type, [1234567890123456789012345678901234567890, None, 123231])
    java_round_trip(big_integer_array_type, [])
    java_round_trip(big_integer_array_type, None)

def test_boolean():
    boolean_type = BooleanType()
    java_round_trip(boolean_type, True)
    java_round_trip(boolean_type, False)
    java_round_trip(boolean_type, None)

    boolean_array_type = BooleanArrayType()
    java_round_trip(boolean_array_type, [True, None, False])
    java_round_trip(boolean_array_type, [])
    java_round_trip(boolean_array_type, None)

    primitive_boolean_array_type = PrimitiveBooleanArrayType()
    java_round_trip(primitive_boolean_array_type, [True, False, False, True])
    java_round_trip(primitive_boolean_array_type, [])
    java_round_trip(primitive_boolean_array_type, None)

def test_float():
    float_type = FloatType()
    java_round_trip(float_type, 1.234, False)
    java_round_trip(float_type, -10, False)
    java_round_trip(float_type, None)

    float_array_type = FloatArrayType()
    java_round_trip(float_array_type, [1.234, -10, None])
    java_round_trip(float_array_type, [])
    java_round_trip(float_array_type, None)

    primitive_float_array_type = PrimitiveFloatArrayType()
    java_round_trip(primitive_float_array_type, [1.234, -10, 15.5])
    java_round_trip(primitive_float_array_type, [])
    java_round_trip(primitive_float_array_type, None)

def test_double():
    double_type = DoubleType()
    java_round_trip(double_type, 1.234)
    java_round_trip(double_type, -10)
    java_round_trip(double_type, None)

    double_array_type = DoubleArrayType()
    java_round_trip(double_array_type, [1.234, -10, None])
    java_round_trip(double_array_type, [])
    java_round_trip(double_array_type, None)

    primitive_double_array_type = PrimitiveDoubleArrayType()
    java_round_trip(primitive_double_array_type, [1.234, -10, 15.5])
    java_round_trip(primitive_double_array_type, [])
    java_round_trip(primitive_double_array_type, None)

def test_byte():
    byte_type = ByteType()
    java_round_trip(byte_type, 32, False)
    java_round_trip(byte_type, -127, False)
    java_round_trip(byte_type, None)

    byte_array_type = ByteArrayType()
    java_round_trip(byte_array_type, [32, None, -127])
    java_round_trip(byte_array_type, b'1234')
    java_round_trip(byte_array_type, b'')
    java_round_trip(byte_array_type, None)

    primitive_byte_array_type = PrimitiveByteArrayType()
    java_round_trip(primitive_byte_array_type, b'1234')
    java_round_trip(primitive_byte_array_type, b'')
    java_round_trip(primitive_byte_array_type, None)

def test_short():
    short_type = ShortType()
    java_round_trip(short_type, 3232, False)
    java_round_trip(short_type, -1207, False)
    java_round_trip(short_type, None)

    short_array_type = ShortArrayType()
    java_round_trip(short_array_type, [3232, None, -1207])
    java_round_trip(short_array_type, [])
    java_round_trip(short_array_type, None)

    primitive_short_array_type = PrimitiveShortArrayType()
    java_round_trip(primitive_short_array_type, [3232, -1207, 0])
    java_round_trip(primitive_short_array_type, [])
    java_round_trip(primitive_short_array_type, None)

def test_integer():
    integer_type = IntegerType()
    java_round_trip(integer_type, 3232234)
    java_round_trip(integer_type, -1207234)
    java_round_trip(integer_type, None)

    integer_array_type = IntegerArrayType()
    java_round_trip(integer_array_type, [3232234, None, -1207234])
    java_round_trip(integer_array_type, [])
    java_round_trip(integer_array_type, None)

    primitive_int_array_type = PrimitiveIntArrayType()
    java_round_trip(primitive_int_array_type, [3232234, -1207234, 0])
    java_round_trip(primitive_int_array_type, [])
    java_round_trip(primitive_int_array_type, None)

def test_long():
    long_type = LongType()
    java_round_trip(long_type, 3232234234)
    java_round_trip(long_type, -3207234234)
    java_round_trip(long_type, None)

    long_array_type = LongArrayType()
    java_round_trip(long_array_type, [3232234234, None, -3207234234])
    java_round_trip(long_array_type, [])
    java_round_trip(long_array_type, None)

    primitive_long_array_type = PrimitiveLongArrayType()
    java_round_trip(primitive_long_array_type, [3232234234, -3207234234, 0])
    java_round_trip(primitive_long_array_type, [])
    java_round_trip(primitive_long_array_type, None)

def test_string():
    string_type = StringType()
    java_round_trip(string_type, "test")
    java_round_trip(string_type, u"✓ unicode check")
    java_round_trip(string_type, "")
    java_round_trip(string_type, None)

    string_array_type = StringArrayType()
    java_round_trip(string_array_type, ["test", u"✓ unicode check", "", None])
    java_round_trip(string_array_type, [])
    java_round_trip(string_array_type, None)

def test_date():
    date_type = DateType()
    java_round_trip(date_type, datetime.fromtimestamp(1563826071))
    java_round_trip(date_type, datetime.fromtimestamp(0))
    java_round_trip(date_type, None)

    date_array_type = DateArrayType()
    java_round_trip(date_array_type, [datetime.fromtimestamp(1563826071), datetime.fromtimestamp(0), None])
    java_round_trip(date_array_type, [])
    java_round_trip(date_array_type, None)

def test_calendar():
    calendar_type = CalendarType()
    java_round_trip(calendar_type, datetime.fromtimestamp(1563826071))
    java_round_trip(calendar_type, datetime.fromtimestamp(0))
    java_round_trip(calendar_type, None)

    calendar_array_type = CalendarArrayType()
    java_round_trip(calendar_array_type, [datetime.fromtimestamp(1563826071), datetime.fromtimestamp(0), None])
    java_round_trip(calendar_array_type, [])
    java_round_trip(calendar_array_type, None)

_test_point = Point(1, 1)
_test_multi_point = MultiPoint([[0.5, 0.5], [1, 1]])
_test_line_string = LineString([[0.5, 0.5], [1, 1]])
_test_multi_line_string = MultiLineString([[[0.5, 0.5], [1, 1]], [[-0.5, -0.5], [1, 1]]])
_test_polygon = Polygon([[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]])
_test_polygon2 = Polygon([[0, 0], [0, -1], [-1, -1], [-1, 0], [0, 0]])
_test_multi_polygon = MultiPolygon([_test_polygon, _test_polygon2])

def test_point():
    point_type = PointType()
    java_round_trip(point_type, _test_point)
    java_round_trip(point_type, None)

    point_array_type = PointArrayType()
    java_round_trip(point_array_type, [_test_point, None])
    java_round_trip(point_array_type, [])
    java_round_trip(point_array_type, None)

def test_multi_point():
    multi_point_type = MultiPointType()
    java_round_trip(multi_point_type, _test_multi_point)
    java_round_trip(multi_point_type, None)

    multi_point_array_type = MultiPointArrayType()
    java_round_trip(multi_point_array_type, [_test_multi_point, None])
    java_round_trip(multi_point_array_type, [])
    java_round_trip(multi_point_array_type, None)

def test_line_string():
    line_string_type = LineStringType()
    java_round_trip(line_string_type, _test_line_string)
    java_round_trip(line_string_type, None)

    line_string_array_type = LineStringArrayType()
    java_round_trip(line_string_array_type, [_test_line_string, None])
    java_round_trip(line_string_array_type, [])
    java_round_trip(line_string_array_type, None)

def test_multi_line_string():
    multi_line_string_type = MultiLineStringType()
    java_round_trip(multi_line_string_type, _test_multi_line_string)
    java_round_trip(multi_line_string_type, None)

    multi_line_string_array_type = MultiLineStringArrayType()
    java_round_trip(multi_line_string_array_type, [_test_multi_line_string, None])
    java_round_trip(multi_line_string_array_type, [])
    java_round_trip(multi_line_string_array_type, None)

def test_polygon():
    polygon_type = PolygonType()
    java_round_trip(polygon_type, _test_polygon)
    java_round_trip(polygon_type, None)

    polygon_array_type = PolygonArrayType()
    java_round_trip(polygon_array_type, [_test_polygon, None])
    java_round_trip(polygon_array_type, [])
    java_round_trip(polygon_array_type, None)

def test_multi_polygon():
    multi_polygon_type = MultiPolygonType()
    java_round_trip(multi_polygon_type, _test_multi_polygon)
    java_round_trip(multi_polygon_type, None)

    multi_polygon_array_type = MultiPolygonArrayType()
    java_round_trip(multi_polygon_array_type, [_test_multi_polygon, None])
    java_round_trip(multi_polygon_array_type, [])
    java_round_trip(multi_polygon_array_type, None)

def test_geometry():
    geometry_type = GeometryType()
    java_round_trip(geometry_type, _test_point)
    java_round_trip(geometry_type, _test_multi_point)
    java_round_trip(geometry_type, _test_line_string)
    java_round_trip(geometry_type, _test_multi_line_string)
    java_round_trip(geometry_type, _test_polygon)
    java_round_trip(geometry_type, _test_multi_polygon)
    java_round_trip(geometry_type, None)

    geometry_array_type = GeometryArrayType()
    java_round_trip(geometry_array_type, [_test_point, _test_multi_point, _test_line_string, _test_multi_line_string, _test_polygon, _test_multi_polygon, None])
    java_round_trip(geometry_array_type, [])
    java_round_trip(geometry_array_type, None)

def test_geometry_collection():
    geometry_collection_type = GeometryCollectionType()
    test_geometry_collection = GeometryCollection([_test_point, _test_multi_point, _test_line_string, _test_multi_line_string, _test_polygon, _test_multi_polygon])
    java_round_trip(geometry_collection_type, test_geometry_collection)
    java_round_trip(geometry_collection_type, None)

    geometry_collection_array_type = GeometryCollectionArrayType()
    java_round_trip(geometry_collection_array_type, [test_geometry_collection, None])
    java_round_trip(geometry_collection_array_type, [])
    java_round_trip(geometry_collection_array_type, None)
