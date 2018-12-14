package org.locationtech.geowave.core.geotime.util;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;

import com.clearspring.analytics.util.Varint;

public class TWKBReader
{
	public TWKBReader() {

	}

	public Geometry read(
			byte[] bytes )
			throws ParseException {
		try {
			ByteArrayInputStream in = new ByteArrayInputStream(
					bytes);
			DataInput input = new DataInputStream(
					in);
			return read(input);
		}
		catch (IOException e) {
			throw new ParseException(
					"Error reading TWKB geometry.",
					e);
		}
	}

	public Geometry read(
			DataInput input )
			throws IOException {
		byte typeAndPrecision = input.readByte();
		byte type = (byte) (typeAndPrecision & 0x0F);
		double precision = Math.pow(
				10,
				TWKBUtils.zigZagDecode((typeAndPrecision & 0xF0) >> 4));
		switch (type) {
			case TWKBUtils.POINT_TYPE:
				return readPoint(
						precision,
						input);
			case TWKBUtils.LINESTRING_TYPE:
				return readLineString(
						precision,
						input);
			case TWKBUtils.POLYGON_TYPE:
				return readPolygon(
						precision,
						input);
			case TWKBUtils.MULTIPOINT_TYPE:
				return readMultiPoint(
						precision,
						input);
			case TWKBUtils.MULTILINESTRING_TYPE:
				return readMultiLineString(
						precision,
						input);
			case TWKBUtils.MULTIPOLYGON_TYPE:
				return readMultiPolygon(
						precision,
						input);
			case TWKBUtils.GEOMETRYCOLLECTION_TYPE:
				return readGeometryCollection(input);
		}
		return null;
	}

	private Point readPoint(
			double precision,
			DataInput input )
			throws IOException {
		byte metadata = input.readByte();
		if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
			return GeometryUtils.GEOMETRY_FACTORY.createPoint();
		}

		long x = Varint.readSignedVarLong(input);
		long y = Varint.readSignedVarLong(input);
		double xCoord = x / precision;
		double yCoord = y / precision;
		return GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
				xCoord,
				yCoord));
	}

	private LineString readLineString(
			double precision,
			DataInput input )
			throws IOException {
		byte metadata = input.readByte();
		if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
			return GeometryUtils.GEOMETRY_FACTORY.createLineString();
		}

		Coordinate[] coordinates = readPointArray(
				precision,
				input);
		return GeometryUtils.GEOMETRY_FACTORY.createLineString(coordinates);
	}

	private Polygon readPolygon(
			double precision,
			DataInput input )
			throws IOException {
		byte metadata = input.readByte();
		if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
			return GeometryUtils.GEOMETRY_FACTORY.createPolygon();
		}
		int numRings = Varint.readUnsignedVarInt(input);
		LinearRing exteriorRing = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(readPointArray(
				precision,
				input));
		LinearRing[] interiorRings = new LinearRing[numRings - 1];
		for (int i = 0; i < numRings - 1; i++) {
			interiorRings[i] = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(readPointArray(
					precision,
					input));
		}
		return GeometryUtils.GEOMETRY_FACTORY.createPolygon(
				exteriorRing,
				interiorRings);
	}

	private MultiPoint readMultiPoint(
			double precision,
			DataInput input )
			throws IOException {
		byte metadata = input.readByte();
		if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
			return GeometryUtils.GEOMETRY_FACTORY.createMultiPoint();
		}
		Coordinate[] points = readPointArray(
				precision,
				input);
		return GeometryUtils.GEOMETRY_FACTORY.createMultiPointFromCoords(points);
	}

	private MultiLineString readMultiLineString(
			double precision,
			DataInput input )
			throws IOException {
		byte metadata = input.readByte();
		if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
			return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString();
		}
		int numLines = Varint.readUnsignedVarInt(input);
		LineString[] lines = new LineString[numLines];
		for (int i = 0; i < numLines; i++) {
			lines[i] = GeometryUtils.GEOMETRY_FACTORY.createLineString(readPointArray(
					precision,
					input));
		}
		return GeometryUtils.GEOMETRY_FACTORY.createMultiLineString(lines);
	}

	private MultiPolygon readMultiPolygon(
			double precision,
			DataInput input )
			throws IOException {
		byte metadata = input.readByte();
		if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
			return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon();
		}
		int numPolygons = Varint.readUnsignedVarInt(input);
		Polygon[] polygons = new Polygon[numPolygons];
		int numRings;
		for (int i = 0; i < numPolygons; i++) {
			numRings = Varint.readUnsignedVarInt(input);
			if (numRings == 0) {
				polygons[i] = GeometryUtils.GEOMETRY_FACTORY.createPolygon();
				continue;
			}
			LinearRing exteriorRing = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(readPointArray(
					precision,
					input));
			LinearRing[] interiorRings = new LinearRing[numRings - 1];
			for (int j = 0; j < numRings - 1; j++) {
				interiorRings[j] = GeometryUtils.GEOMETRY_FACTORY.createLinearRing(readPointArray(
						precision,
						input));
			}
			polygons[i] = GeometryUtils.GEOMETRY_FACTORY.createPolygon(
					exteriorRing,
					interiorRings);
		}
		return GeometryUtils.GEOMETRY_FACTORY.createMultiPolygon(polygons);
	}

	private GeometryCollection readGeometryCollection(
			DataInput input )
			throws IOException {
		byte metadata = input.readByte();
		if ((metadata & TWKBUtils.EMPTY_GEOMETRY) != 0) {
			return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection();
		}
		int numGeometries = Varint.readUnsignedVarInt(input);
		Geometry[] geometries = new Geometry[numGeometries];
		for (int i = 0; i < numGeometries; i++) {
			geometries[i] = read(input);
		}
		return GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(geometries);
	}

	private Coordinate[] readPointArray(
			double precision,
			DataInput input )
			throws IOException {
		int numCoordinates = Varint.readUnsignedVarInt(input);
		Coordinate[] coordinates = new Coordinate[numCoordinates];
		long lastX = 0;
		long lastY = 0;
		for (int i = 0; i < numCoordinates; i++) {
			lastX = Varint.readSignedVarLong(input) + lastX;
			lastY = Varint.readSignedVarLong(input) + lastY;
			double x = ((double) lastX) / precision;
			double y = ((double) lastY) / precision;
			coordinates[i] = new Coordinate(
					x,
					y);
		}
		return coordinates;
	}
}
