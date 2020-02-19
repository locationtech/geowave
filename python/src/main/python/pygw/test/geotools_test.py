#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.base.type_conversions import *
from pygw.geotools import SimpleFeatureTypeBuilder
from pygw.geotools import SimpleFeatureBuilder
from pygw.geotools import AttributeDescriptor

def check_attribute(sft, attr_name, nilable, type):
    attr = sft.get_attribute(attr_name)
    assert attr.descriptor == attr_name
    assert attr.is_nilable == nilable
    assert isinstance(attr.field, type)

def test_simple_feature_type():
    sftb = SimpleFeatureTypeBuilder()
    sftb.set_name("TestKitchenSinkType")
    sftb.set_namespace_uri("http://www.example.org")
    sftb.set_srs("EPSG:4326")
    sftb.add(AttributeDescriptor.point("the_geom", False))
    sftb.add(AttributeDescriptor.big_decimal("big_decimal", True))
    sftb.add(AttributeDescriptor.big_decimal_array("big_decimal_array"))
    sftb.add(AttributeDescriptor.big_integer("big_integer"))
    sftb.add(AttributeDescriptor.big_integer_array("big_integer_array"))
    sftb.add(AttributeDescriptor.boolean("boolean"))
    sftb.add(AttributeDescriptor.boolean_array("boolean_array"))
    sftb.add(AttributeDescriptor.primitive_boolean_array("prim_boolean_array"))
    sftb.add(AttributeDescriptor.float("float"))
    sftb.add(AttributeDescriptor.float_array("float_array"))
    sftb.add(AttributeDescriptor.primitive_float_array("prim_float_array"))
    sftb.add(AttributeDescriptor.double("double"))
    sftb.add(AttributeDescriptor.double_array("double_array"))
    sftb.add(AttributeDescriptor.primitive_double_array("prim_double_array"))
    sftb.add(AttributeDescriptor.byte("byte"))
    sftb.add(AttributeDescriptor.byte_array("byte_array"))
    sftb.add(AttributeDescriptor.primitive_byte_array("prim_byte_array"))
    sftb.add(AttributeDescriptor.short("short"))
    sftb.add(AttributeDescriptor.short_array("short_array"))
    sftb.add(AttributeDescriptor.primitive_short_array("prim_short_array"))
    sftb.add(AttributeDescriptor.integer("integer"))
    sftb.add(AttributeDescriptor.integer_array("integer_array"))
    sftb.add(AttributeDescriptor.primitive_int_array("prim_int_array"))
    sftb.add(AttributeDescriptor.long("long"))
    sftb.add(AttributeDescriptor.long_array("long_array"))
    sftb.add(AttributeDescriptor.primitive_long_array("prim_long_array"))
    sftb.add(AttributeDescriptor.string("string"))
    sftb.add(AttributeDescriptor.string_array("string_array"))
    sftb.add(AttributeDescriptor.date("date"))
    sftb.add(AttributeDescriptor.date_array("date_array"))
    sftb.add(AttributeDescriptor.calendar("calendar"))
    sftb.add(AttributeDescriptor.calendar_array("calendar_array"))
    sftb.add(AttributeDescriptor.point_array("point_array"))
    sftb.add(AttributeDescriptor.multi_point("multi_point"))
    sftb.add(AttributeDescriptor.multi_point_array("multi_point_array"))
    sftb.add(AttributeDescriptor.line_string("line_string"))
    sftb.add(AttributeDescriptor.line_string_array("line_string_array"))
    sftb.add(AttributeDescriptor.multi_line_string("multi_line_string"))
    sftb.add(AttributeDescriptor.multi_line_string_array("multi_line_string_array"))
    sftb.add(AttributeDescriptor.polygon("polygon"))
    sftb.add(AttributeDescriptor.polygon_array("polygon_array"))
    sftb.add(AttributeDescriptor.multi_polygon("multi_polygon"))
    sftb.add(AttributeDescriptor.multi_polygon_array("multi_polygon_array"))
    sftb.add(AttributeDescriptor.geometry_collection("geometry_collection"))
    sftb.add(AttributeDescriptor.geometry_collection_array("geometry_collection_array"))
    sftb.add(AttributeDescriptor.geometry("geometry"))
    sftb.add(AttributeDescriptor.geometry_array("geometry_array"))
    sft = sftb.build_feature_type()

    assert sft.get_type_name() == "TestKitchenSinkType"
    check_attribute(sft, "the_geom", False, PointType)
    check_attribute(sft, "big_decimal", True, BigDecimalType)
    check_attribute(sft, "big_decimal_array", False, BigDecimalArrayType)
    check_attribute(sft, "big_integer", False, BigIntegerType)
    check_attribute(sft, "big_integer_array", False, BigIntegerArrayType)
    check_attribute(sft, "boolean", False, BooleanType)
    check_attribute(sft, "boolean_array", False, BooleanArrayType)
    check_attribute(sft, "prim_boolean_array", False, PrimitiveBooleanArrayType)
    check_attribute(sft, "float", False, FloatType)
    check_attribute(sft, "float_array", False, FloatArrayType)
    check_attribute(sft, "prim_float_array", False, PrimitiveFloatArrayType)
    check_attribute(sft, "double", False, DoubleType)
    check_attribute(sft, "double_array", False, DoubleArrayType)
    check_attribute(sft, "prim_double_array", False, PrimitiveDoubleArrayType)
    check_attribute(sft, "byte", False, ByteType)
    check_attribute(sft, "byte_array", False, ByteArrayType)
    check_attribute(sft, "prim_byte_array", False, PrimitiveByteArrayType)
    check_attribute(sft, "short", False, ShortType)
    check_attribute(sft, "short_array", False, ShortArrayType)
    check_attribute(sft, "prim_short_array", False, PrimitiveShortArrayType)
    check_attribute(sft, "integer", False, IntegerType)
    check_attribute(sft, "integer_array", False, IntegerArrayType)
    check_attribute(sft, "prim_int_array", False, PrimitiveIntArrayType)
    check_attribute(sft, "long", False, LongType)
    check_attribute(sft, "long_array", False, LongArrayType)
    check_attribute(sft, "prim_long_array", False, PrimitiveLongArrayType)
    check_attribute(sft, "string", False, StringType)
    check_attribute(sft, "string_array", False, StringArrayType)
    check_attribute(sft, "date", False, DateType)
    check_attribute(sft, "date_array", False, DateArrayType)
    check_attribute(sft, "calendar", False, CalendarType)
    check_attribute(sft, "calendar_array", False, CalendarArrayType)
    check_attribute(sft, "point_array", False, PointArrayType)
    check_attribute(sft, "multi_point", False, MultiPointType)
    check_attribute(sft, "multi_point_array", False, MultiPointArrayType)
    check_attribute(sft, "line_string", False, LineStringType)
    check_attribute(sft, "line_string_array", False, LineStringArrayType)
    check_attribute(sft, "multi_line_string", False, MultiLineStringType)
    check_attribute(sft, "multi_line_string_array", False, MultiLineStringArrayType)
    check_attribute(sft, "polygon", False, PolygonType)
    check_attribute(sft, "polygon_array", False, PolygonArrayType)
    check_attribute(sft, "multi_polygon", False, MultiPolygonType)
    check_attribute(sft, "multi_polygon_array", False, MultiPolygonArrayType)
    check_attribute(sft, "geometry_collection", False, GeometryCollectionType)
    check_attribute(sft, "geometry_collection_array", False, GeometryCollectionArrayType)
    check_attribute(sft, "geometry", False, GeometryType)
    check_attribute(sft, "geometry_array", False, GeometryArrayType)

    # Get Attribute by index
    assert sft.get_attribute(2).descriptor == "big_decimal_array"
    assert sft.get_attribute(15).descriptor == "byte_array"

    # Get non-existent attribute
    assert sft.get_attribute("nonexistent") is None

def test_simple_feature():
    sftb = SimpleFeatureTypeBuilder()
    sftb.set_name("TestPointType")
    sftb.add(AttributeDescriptor.point("the_geom", False))
    sftb.add(AttributeDescriptor.big_integer("big_int", True))
    sftb.add(AttributeDescriptor.string("string", True))
    sftb.add(AttributeDescriptor.byte("byte", False))
    sftb.add(AttributeDescriptor.float("float", True))
    sft = sftb.build_feature_type()

    big_number = 123123123123123123123123123
    sfb = SimpleFeatureBuilder(sft)
    sfb.set_attr("the_geom", Point(1, 1))
    sfb.set_attr("big_int", big_number)
    sfb.set_attr("string", "test value")
    sfb.set_attr("byte", 38)
    feature = sfb.build("fid1")

    assert feature.get_id() == "fid1"
    assert feature.get_type() is sft
    assert feature.get_feature_type() is sft
    assert feature.get_default_geometry() == Point(1, 1)
    assert feature.get_attribute_count() == 5
    assert feature.get_attribute("string") == "test value"
    assert feature.get_attribute(1) == big_number
    attrs = feature.get_attributes()
    assert attrs[0] == Point(1, 1)
    assert attrs[1] == big_number
    assert attrs[2] == "test value"
    assert attrs[3] == 38
    assert attrs[4] is None

    feature_dict = feature.to_dict()
    assert feature_dict["id"] == "fid1"
    assert feature_dict["the_geom"] == Point(1, 1)
    assert feature_dict["string"] == "test value"
    assert feature_dict["byte"] == 38
    assert feature_dict["big_int"] == big_number
