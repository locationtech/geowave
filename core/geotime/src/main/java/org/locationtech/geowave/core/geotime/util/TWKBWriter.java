package org.locationtech.geowave.core.geotime.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.clearspring.analytics.util.Varint;

public class TWKBWriter
{
	private static final byte MAX_COORD_PRECISION = 7;
	private static final byte MIN_COORD_PRECISION = -8;

	private int maxPrecision;

	public TWKBWriter() {
		this(
				MAX_COORD_PRECISION);
	}

	public TWKBWriter(
			int maxPrecision ) {
		this.maxPrecision = Math.min(
				MAX_COORD_PRECISION,
				maxPrecision);
	}

	public byte[] write(
			Geometry geom ) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			DataOutput output = new DataOutputStream(
					out);
			write(
					geom,
					output);
			return out.toByteArray();
		}
		catch (IOException e) {
			throw new RuntimeException(
					"Error writing TWKB geometry.",
					e);
		}
	}

	public void write(
			Geometry geom,
			DataOutput output )
			throws IOException {
		if (geom instanceof Point) {
			writePoint(
					(Point) geom,
					output);
		}
		else if (geom instanceof LineString) {
			writeLineString(
					(LineString) geom,
					output);
		}
		else if (geom instanceof Polygon) {
			writePolygon(
					(Polygon) geom,
					output);
		}
		else if (geom instanceof MultiPoint) {
			writeMultiPoint(
					(MultiPoint) geom,
					output);
		}
		else if (geom instanceof MultiLineString) {
			writeMultiLineString(
					(MultiLineString) geom,
					output);
		}
		else if (geom instanceof MultiPolygon) {
			writeMultiPolygon(
					(MultiPolygon) geom,
					output);
		}
		else if (geom instanceof GeometryCollection) {
			writeGeometryCollection(
					(GeometryCollection) geom,
					output);
		}
	}

	private void writePoint(
			Point point,
			DataOutput output )
			throws IOException {
		int precision = getPrecision(point.getCoordinates());
		output.writeByte(getTypeAndPrecisionByte(
				TWKBUtils.POINT_TYPE,
				precision));
		byte metadata = 0;

		if (point.isEmpty()) {
			metadata |= TWKBUtils.EMPTY_GEOMETRY;
			output.writeByte(metadata);
			return;
		}

		double precisionMultiplier = Math.pow(
				10,
				precision);
		long xValue = Math.round(point.getCoordinate().getX() * precisionMultiplier);
		long yValue = Math.round(point.getCoordinate().getY() * precisionMultiplier);
		output.writeByte(metadata);
		Varint.writeSignedVarLong(
				xValue,
				output);
		Varint.writeSignedVarLong(
				yValue,
				output);
	}

	private void writeLineString(
			LineString line,
			DataOutput output )
			throws IOException {
		int precision = getPrecision(line.getCoordinates());
		output.writeByte(getTypeAndPrecisionByte(
				TWKBUtils.LINESTRING_TYPE,
				precision));
		byte metadata = 0;

		if (line.isEmpty()) {
			metadata |= TWKBUtils.EMPTY_GEOMETRY;
			output.writeByte(metadata);
			return;
		}

		output.writeByte(metadata);
		double precisionMultiplier = Math.pow(
				10,
				precision);
		writePointArray(
				line.getCoordinates(),
				precisionMultiplier,
				output);
	}

	private void writePolygon(
			Polygon polygon,
			DataOutput output )
			throws IOException {
		int precision = getPrecision(polygon.getCoordinates());
		output.writeByte(getTypeAndPrecisionByte(
				TWKBUtils.POLYGON_TYPE,
				precision));
		byte metadata = 0;

		if (polygon.isEmpty()) {
			metadata |= TWKBUtils.EMPTY_GEOMETRY;
			output.writeByte(metadata);
			return;
		}

		output.writeByte(metadata);
		Varint.writeUnsignedVarInt(
				polygon.getNumInteriorRing() + 1,
				output);
		double precisionMultiplier = Math.pow(
				10,
				precision);
		writePointArray(
				polygon.getExteriorRing().getCoordinates(),
				precisionMultiplier,
				output);
		for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
			writePointArray(
					polygon.getInteriorRingN(
							i).getCoordinates(),
					precisionMultiplier,
					output);
		}
	}

	private void writeMultiPoint(
			MultiPoint multiPoint,
			DataOutput output )
			throws IOException {
		int precision = getPrecision(multiPoint.getCoordinates());
		output.writeByte(getTypeAndPrecisionByte(
				TWKBUtils.MULTIPOINT_TYPE,
				precision));
		byte metadata = 0;

		if (multiPoint.isEmpty()) {
			metadata |= TWKBUtils.EMPTY_GEOMETRY;
			output.writeByte(metadata);
			return;
		}

		output.writeByte(metadata);
		double precisionMultiplier = Math.pow(
				10,
				precision);
		writePointArray(
				multiPoint.getCoordinates(),
				precisionMultiplier,
				output);
	}

	private void writeMultiLineString(
			MultiLineString multiLine,
			DataOutput output )
			throws IOException {
		int precision = getPrecision(multiLine.getCoordinates());
		output.writeByte(getTypeAndPrecisionByte(
				TWKBUtils.MULTILINESTRING_TYPE,
				precision));
		byte metadata = 0;

		if (multiLine.isEmpty()) {
			metadata |= TWKBUtils.EMPTY_GEOMETRY;
			output.writeByte(metadata);
			return;
		}

		output.writeByte(metadata);
		double precisionMultiplier = Math.pow(
				10,
				precision);
		Varint.writeUnsignedVarInt(
				multiLine.getNumGeometries(),
				output);
		for (int i = 0; i < multiLine.getNumGeometries(); i++) {
			writePointArray(
					multiLine.getGeometryN(
							i).getCoordinates(),
					precisionMultiplier,
					output);
		}
	}

	private void writeMultiPolygon(
			MultiPolygon multiPolygon,
			DataOutput output )
			throws IOException {
		int precision = getPrecision(multiPolygon.getCoordinates());
		output.writeByte(getTypeAndPrecisionByte(
				TWKBUtils.MULTIPOLYGON_TYPE,
				precision));
		byte metadata = 0;

		if (multiPolygon.isEmpty()) {
			metadata |= TWKBUtils.EMPTY_GEOMETRY;
			output.writeByte(metadata);
			return;
		}

		output.writeByte(metadata);
		double precisionMultiplier = Math.pow(
				10,
				precision);
		Varint.writeUnsignedVarInt(
				multiPolygon.getNumGeometries(),
				output);
		for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
			Polygon polygon = (Polygon) multiPolygon.getGeometryN(i);
			if (polygon.isEmpty()) {
				Varint.writeUnsignedVarInt(
						0,
						output);
				continue;
			}
			Varint.writeUnsignedVarInt(
					polygon.getNumInteriorRing() + 1,
					output);
			writePointArray(
					polygon.getExteriorRing().getCoordinates(),
					precisionMultiplier,
					output);
			for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
				writePointArray(
						polygon.getInteriorRingN(
								j).getCoordinates(),
						precisionMultiplier,
						output);
			}
		}
	}

	private void writeGeometryCollection(
			GeometryCollection geoms,
			DataOutput output )
			throws IOException {
		output.writeByte(getTypeAndPrecisionByte(
				TWKBUtils.GEOMETRYCOLLECTION_TYPE,
				0));
		byte metadata = 0;

		if (geoms.isEmpty()) {
			metadata |= TWKBUtils.EMPTY_GEOMETRY;
			output.writeByte(metadata);
			return;
		}

		output.writeByte(metadata);
		Varint.writeUnsignedVarInt(
				geoms.getNumGeometries(),
				output);
		for (int i = 0; i < geoms.getNumGeometries(); i++) {
			Geometry geom = geoms.getGeometryN(i);
			write(
					geom,
					output);
		}
	}

	private void writePointArray(
			Coordinate[] coordinates,
			double precisionMultiplier,
			DataOutput output )
			throws IOException {
		long lastX = 0;
		long lastY = 0;
		Varint.writeUnsignedVarInt(
				coordinates.length,
				output);
		for (Coordinate c : coordinates) {
			long x = Math.round(c.getX() * precisionMultiplier);
			long y = Math.round(c.getY() * precisionMultiplier);
			Varint.writeSignedVarLong(
					x - lastX,
					output);
			Varint.writeSignedVarLong(
					y - lastY,
					output);
			lastX = x;
			lastY = y;
		}
	}

	private byte getTypeAndPrecisionByte(
			byte type,
			int precision ) {
		byte typeAndPrecision = type;
		typeAndPrecision |= TWKBUtils.zigZagEncode(precision) << 4;
		return typeAndPrecision;
	}

	private int getPrecision(
			Coordinate[] coordinates ) {
		int max = MIN_COORD_PRECISION;
		for (int i = 0; i < coordinates.length; i++) {
			BigDecimal xCoord = new BigDecimal(
					Double.toString(coordinates[i].getX())).stripTrailingZeros();
			max = Math.max(
					xCoord.scale(),
					max);
			BigDecimal yCoord = new BigDecimal(
					Double.toString(coordinates[i].getY())).stripTrailingZeros();
			max = Math.max(
					yCoord.scale(),
					max);
		}
		return Math.min(
				max,
				maxPrecision);
	}
}
