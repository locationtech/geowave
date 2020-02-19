#
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation

#
# See the NOTICE file distributed with this work for additional information regarding copyright
# ownership. All rights reserved. This program and the accompanying materials are made available
# under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
# available at http://www.apache.org/licenses/LICENSE-2.0.txt
#===============================================================================================

from pygw.config import java_pkg
from pygw.config import reflection_util
from pygw.base import GeoWaveObject
from pygw.base.type_conversions import *

from enum import Enum

_binding_map = {}

class AttributeDescriptor(GeoWaveObject):
    """
    Describes a single attribute of a feature type.
    """

    def __init__(self, attribute_type, is_nilable, descriptor, j_attribute=None):
        if not isinstance(attribute_type, AttributeDescriptor.Type):
            raise AttributeDescriptor.UnknownTypeError("Invalid argument to `attribute_type`. Must be one of defined types in AttributeDescriptor.Type")
        self.field = attribute_type.value()
        self.is_nilable = is_nilable
        self.descriptor = descriptor
        if j_attribute is None:
            j_builder = java_pkg.org.geotools.feature.AttributeTypeBuilder()
            j_type_cls = reflection_util.classForName(self.field.binding)
            j_builder.binding(j_type_cls)
            j_builder.nillable(is_nilable)
            j_attribute = j_builder.buildDescriptor(descriptor)
        super().__init__(j_attribute)

    def to_java(self, value):
        """
        Converts a Python variable into its Java counterpart.

        Args:
            value (any): The Python variable to convert.
        Retuurns:
            The Java equivalent of the Python variable.
        """
        return self.field.to_java(value)

    def from_java(self, value):
        """
        Converts a Java variable into its Python counterpart.

        Args:
            value (any): The Java variable to convert.
        Retuurns:
            The Python equivalent of the Java variable.
        """
        return self.field.from_java(value)

    @classmethod
    def from_java_attribute_descriptor(cls, java_attribute_descriptor):
        """
        Constructs an attribute descriptor from a Java geotools attribute descriptor.

        Args:
            java_attribute_descriptor (AttributeDescriptor): The Java attribute descriptor.
        Retuurns:
            A `pygw.geotools.attribute_descriptor.AttributeDescriptor` that matches the Java one.
        """
        nilable = java_attribute_descriptor.isNillable()
        descriptor = java_attribute_descriptor.getName().getLocalPart()
        binding = java_attribute_descriptor.getType().getBinding().getName()
        return cls(_binding_map[binding], nilable, descriptor, java_attribute_descriptor)

    @classmethod
    def big_decimal(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for BigDecimal values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A BigDecimal `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BIG_DECIMAL, is_nilable, descriptor)

    @classmethod
    def big_decimal_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for BigDecimal[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A BigDecimal[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BIG_DECIMAL_ARRAY, is_nilable, descriptor)

    @classmethod
    def big_integer(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for BigInteger values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A BigInteger `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BIG_INTEGER, is_nilable, descriptor)

    @classmethod
    def big_integer_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for BigInteger[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A BigInteger[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BIG_INTEGER_ARRAY, is_nilable, descriptor)

    @classmethod
    def boolean(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Boolean values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Boolean `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BOOLEAN, is_nilable, descriptor)

    @classmethod
    def boolean_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Boolean[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Boolean[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BOOLEAN_ARRAY, is_nilable, descriptor)

    @classmethod
    def float(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Float values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Float `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.FLOAT, is_nilable, descriptor)

    @classmethod
    def float_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Float[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Float[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.FLOAT_ARRAY, is_nilable, descriptor)

    @classmethod
    def double(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Double values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Double `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.DOUBLE, is_nilable, descriptor)

    @classmethod
    def double_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Double values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Double[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.DOUBLE_ARRAY, is_nilable, descriptor)

    @classmethod
    def byte(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Byte values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Byte `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BYTE, is_nilable, descriptor)

    @classmethod
    def byte_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Byte[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Byte[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.BYTE_ARRAY, is_nilable, descriptor)

    @classmethod
    def short(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Short values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Short `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.SHORT, is_nilable, descriptor)

    @classmethod
    def short_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Short[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Short[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.SHORT_ARRAY, is_nilable, descriptor)

    @classmethod
    def integer(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Integer values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Integer `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.INTEGER, is_nilable, descriptor)

    @classmethod
    def integer_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Integer[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Integer[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.INTEGER_ARRAY, is_nilable, descriptor)

    @classmethod
    def long(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Long values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Long `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.LONG, is_nilable, descriptor)

    @classmethod
    def long_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Long[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Long[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.LONG_ARRAY, is_nilable, descriptor)

    @classmethod
    def primitive_boolean_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for boolean[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A boolean[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.PRIMITIVE_BOOLEAN_ARRAY, is_nilable, descriptor)

    @classmethod
    def primitive_float_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for float[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A float[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.PRIMITIVE_FLOAT_ARRAY, is_nilable, descriptor)

    @classmethod
    def primitive_double_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for double[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A double[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.PRIMITIVE_DOUBLE_ARRAY, is_nilable, descriptor)

    @classmethod
    def primitive_byte_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for byte[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A byte[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.PRIMITIVE_BYTE_ARRAY, is_nilable, descriptor)

    @classmethod
    def primitive_short_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for short[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A short[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.PRIMITIVE_SHORT_ARRAY, is_nilable, descriptor)

    @classmethod
    def primitive_int_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for int[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A int[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.PRIMITIVE_INT_ARRAY, is_nilable, descriptor)

    @classmethod
    def primitive_long_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for long[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A long[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.PRIMITIVE_LONG_ARRAY, is_nilable, descriptor)

    @classmethod
    def string(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for String values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A String `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.STRING, is_nilable, descriptor)

    @classmethod
    def string_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for String[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A String[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.STRING_ARRAY, is_nilable, descriptor)

    @classmethod
    def date(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Date values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Date `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.DATE, is_nilable, descriptor)

    @classmethod
    def date_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Date[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Date[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.DATE_ARRAY, is_nilable, descriptor)

    @classmethod
    def calendar(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Calendar values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Calendar `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.CALENDAR, is_nilable, descriptor)

    @classmethod
    def calendar_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Calendar[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Calendar[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.CALENDAR_ARRAY, is_nilable, descriptor)

    @classmethod
    def point(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Point values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Point `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.POINT, is_nilable, descriptor)

    @classmethod
    def point_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Point[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Point[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.POINT_ARRAY, is_nilable, descriptor)

    @classmethod
    def multi_point(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for MultiPoint values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A MultiPoint `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.MULTI_POINT, is_nilable, descriptor)

    @classmethod
    def multi_point_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for MultiPoint[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A MultiPoint[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.MULTI_POINT_ARRAY, is_nilable, descriptor)

    @classmethod
    def line_string(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for LineString values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A LineString `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.LINE_STRING, is_nilable, descriptor)

    @classmethod
    def line_string_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for LineString[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A LineString[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.LINE_STRING_ARRAY, is_nilable, descriptor)

    @classmethod
    def multi_line_string(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for MultiLineString values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A MultiLineString `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.MULTI_LINE_STRING, is_nilable, descriptor)

    @classmethod
    def multi_line_string_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for MultiLineString[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A MultiLineString[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.MULTI_LINE_STRING_ARRAY, is_nilable, descriptor)

    @classmethod
    def polygon(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Polygon values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Polygon `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.POLYGON, is_nilable, descriptor)

    @classmethod
    def polygon_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Polygon[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Polygon[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.POLYGON_ARRAY, is_nilable, descriptor)

    @classmethod
    def multi_polygon(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for MultiPolygon values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A MultiPolygon `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.MULTI_POLYGON, is_nilable, descriptor)

    @classmethod
    def multi_polygon_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for MultiPolygon[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A MultiPolygon[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.MULTI_POLYGON_ARRAY, is_nilable, descriptor)

    @classmethod
    def geometry_collection(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for GeometryCollection values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A GeometryCollection `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.GEOMETRY_COLLECTION, is_nilable, descriptor)

    @classmethod
    def geometry_collection_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for GeometryCollection[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A GeometryCollection[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.GEOMETRY_COLLECTION_ARRAY, is_nilable, descriptor)

    @classmethod
    def geometry(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Geometry values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Geometry `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.GEOMETRY, is_nilable, descriptor)

    @classmethod
    def geometry_array(cls, descriptor, is_nilable=False):
        """
        Constructs an attribute descriptor for Geometry[] values.

        Args:
            descriptor (str): The name of the attribute.
            is_nilable (bool): Whether or not the attribute can be None. Default is False.
        Returns:
            A Geometry[] `pygw.geotools.attribute_descriptor.AttributeDescriptor`.
        """
        return cls(cls.Type.GEOMETRY_ARRAY, is_nilable, descriptor)

    class Type(Enum):
        """
        The types of attributes that are available.
        """

        BIG_DECIMAL = BigDecimalType
        BIG_DECIMAL_ARRAY = BigDecimalArrayType
        BIG_INTEGER = BigIntegerType
        BIG_INTEGER_ARRAY = BigIntegerArrayType
        BOOLEAN = BooleanType
        BOOLEAN_ARRAY = BooleanArrayType
        FLOAT = FloatType
        FLOAT_ARRAY = FloatArrayType
        DOUBLE = DoubleType
        DOUBLE_ARRAY = DoubleArrayType
        BYTE = ByteType
        BYTE_ARRAY = ByteArrayType
        SHORT = ShortType
        SHORT_ARRAY = ShortArrayType
        INTEGER = IntegerType
        INTEGER_ARRAY = IntegerArrayType
        LONG = LongType
        LONG_ARRAY = LongArrayType
        PRIMITIVE_BOOLEAN_ARRAY = PrimitiveBooleanArrayType
        PRIMITIVE_FLOAT_ARRAY = PrimitiveFloatArrayType
        PRIMITIVE_DOUBLE_ARRAY = PrimitiveDoubleArrayType
        PRIMITIVE_BYTE_ARRAY = PrimitiveByteArrayType
        PRIMITIVE_SHORT_ARRAY = PrimitiveShortArrayType
        PRIMITIVE_INT_ARRAY = PrimitiveIntArrayType
        PRIMITIVE_LONG_ARRAY = PrimitiveLongArrayType
        STRING = StringType
        STRING_ARRAY = StringArrayType
        DATE = DateType
        DATE_ARRAY = DateArrayType
        CALENDAR = CalendarType
        CALENDAR_ARRAY = CalendarArrayType
        POINT = PointType
        POINT_ARRAY = PointArrayType
        MULTI_POINT = MultiPointType
        MULTI_POINT_ARRAY = MultiPointArrayType
        LINE_STRING = LineStringType
        LINE_STRING_ARRAY = LineStringArrayType
        MULTI_LINE_STRING = MultiLineStringType
        MULTI_LINE_STRING_ARRAY = MultiLineStringArrayType
        POLYGON = PolygonType
        POLYGON_ARRAY = PolygonArrayType
        MULTI_POLYGON = MultiPolygonType
        MULTI_POLYGON_ARRAY = MultiPolygonArrayType
        GEOMETRY_COLLECTION = GeometryCollectionType
        GEOMETRY_COLLECTION_ARRAY = GeometryCollectionArrayType
        GEOMETRY = GeometryType
        GEOMETRY_ARRAY = GeometryArrayType

    class UnknownTypeError(Exception): pass

for attr in AttributeDescriptor.Type:
    _binding_map[attr.value().binding]  = attr
