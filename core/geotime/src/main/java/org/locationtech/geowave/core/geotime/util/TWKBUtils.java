package org.locationtech.geowave.core.geotime.util;

public class TWKBUtils
{
	public static final byte POINT_TYPE = 1;
	public static final byte LINESTRING_TYPE = 2;
	public static final byte POLYGON_TYPE = 3;
	public static final byte MULTIPOINT_TYPE = 4;
	public static final byte MULTILINESTRING_TYPE = 5;
	public static final byte MULTIPOLYGON_TYPE = 6;
	public static final byte GEOMETRYCOLLECTION_TYPE = 7;

	public static final byte EXTENDED_DIMENSIONS = 1 << 3;
	public static final byte EMPTY_GEOMETRY = 1 << 4;

	public static final byte MAX_COORD_PRECISION = 7;
	public static final byte MIN_COORD_PRECISION = -8;

	public static final byte MAX_EXTENDED_PRECISION = 3;
	public static final byte MIN_EXTENDED_PRECISION = -4;

	public static int zigZagEncode(
			int value ) {
		return (value << 1) ^ (value >> 31);
	}

	public static int zigZagDecode(
			int value ) {
		int temp = (((value << 31) >> 31) ^ value) >> 1;
		return temp ^ (value & (1 << 31));
	}
}
