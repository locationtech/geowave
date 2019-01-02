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
	private int maxPrecision;

	public TWKBWriter() {
		this(
				TWKBUtils.MAX_COORD_PRECISION);
	}

	public TWKBWriter(
			int maxPrecision ) {
		this.maxPrecision = Math.min(
				TWKBUtils.MAX_COORD_PRECISION,
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
		byte type = getType(geom);
		if (geom.isEmpty()) {
			output.writeByte(getTypeAndPrecisionByte(
					type,
					0));
			output.writeByte(TWKBUtils.EMPTY_GEOMETRY);
			return;
		}
		byte metadata = 0;
		Coordinate[] coordinates = geom.getCoordinates();
		PrecisionWriter precision;
		if (Double.isNaN(coordinates[0].getZ()) || Double.isNaN(coordinates[0].getM())) {
			metadata |= TWKBUtils.EXTENDED_DIMENSIONS;
			precision = new ExtendedPrecisionWriter().calculate(
					coordinates,
					maxPrecision);
		}
		else {
			precision = new PrecisionWriter().calculate(
					coordinates,
					maxPrecision);
		}
		output.writeByte(getTypeAndPrecisionByte(
				type,
				precision.precision));
		output.writeByte(metadata);
		precision.writeExtendedPrecision(output);

		switch (type) {
			case TWKBUtils.POINT_TYPE:
				writePoint(
						(Point) geom,
						precision,
						output);
				break;
			case TWKBUtils.LINESTRING_TYPE:
				writeLineString(
						(LineString) geom,
						precision,
						output);
				break;
			case TWKBUtils.POLYGON_TYPE:
				writePolygon(
						(Polygon) geom,
						precision,
						output);
				break;
			case TWKBUtils.MULTIPOINT_TYPE:
				writeMultiPoint(
						(MultiPoint) geom,
						precision,
						output);
				break;
			case TWKBUtils.MULTILINESTRING_TYPE:
				writeMultiLineString(
						(MultiLineString) geom,
						precision,
						output);
				break;
			case TWKBUtils.MULTIPOLYGON_TYPE:
				writeMultiPolygon(
						(MultiPolygon) geom,
						precision,
						output);
				break;
			case TWKBUtils.GEOMETRYCOLLECTION_TYPE:
				writeGeometryCollection(
						(GeometryCollection) geom,
						precision,
						output);
				break;
			default:
				break;
		}
	}

	private void writePoint(
			Point point,
			PrecisionWriter precision,
			DataOutput output )
			throws IOException {
		precision.writePoint(
				point.getCoordinate(),
				output);
	}

	private void writeLineString(
			LineString line,
			PrecisionWriter precision,
			DataOutput output )
			throws IOException {
		precision.writePointArray(
				line.getCoordinates(),
				output);
	}

	private void writePolygon(
			Polygon polygon,
			PrecisionWriter precision,
			DataOutput output )
			throws IOException {
		Varint.writeUnsignedVarInt(
				polygon.getNumInteriorRing() + 1,
				output);
		precision.writePointArray(
				polygon.getExteriorRing().getCoordinates(),
				output);
		for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
			precision.writePointArray(
					polygon.getInteriorRingN(
							i).getCoordinates(),
					output);
		}
	}

	private void writeMultiPoint(
			MultiPoint multiPoint,
			PrecisionWriter precision,
			DataOutput output )
			throws IOException {
		precision.writePointArray(
				multiPoint.getCoordinates(),
				output);
	}

	private void writeMultiLineString(
			MultiLineString multiLine,
			PrecisionWriter precision,
			DataOutput output )
			throws IOException {
		Varint.writeUnsignedVarInt(
				multiLine.getNumGeometries(),
				output);
		for (int i = 0; i < multiLine.getNumGeometries(); i++) {
			precision.writePointArray(
					multiLine.getGeometryN(
							i).getCoordinates(),
					output);
		}
	}

	private void writeMultiPolygon(
			MultiPolygon multiPolygon,
			PrecisionWriter precision,
			DataOutput output )
			throws IOException {
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
			precision.writePointArray(
					polygon.getExteriorRing().getCoordinates(),
					output);
			for (int j = 0; j < polygon.getNumInteriorRing(); j++) {
				precision.writePointArray(
						polygon.getInteriorRingN(
								j).getCoordinates(),
						output);
			}
		}
	}

	private void writeGeometryCollection(
			GeometryCollection geoms,
			PrecisionWriter precision,
			DataOutput output )
			throws IOException {
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

	private byte getTypeAndPrecisionByte(
			byte type,
			int precision ) {
		byte typeAndPrecision = type;
		typeAndPrecision |= TWKBUtils.zigZagEncode(precision) << 4;
		return typeAndPrecision;
	}

	private byte getType(
			Geometry geom ) {
		if (geom instanceof Point) {
			return TWKBUtils.POINT_TYPE;
		}
		else if (geom instanceof LineString) {
			return TWKBUtils.LINESTRING_TYPE;
		}
		else if (geom instanceof Polygon) {
			return TWKBUtils.POLYGON_TYPE;
		}
		else if (geom instanceof MultiPoint) {
			return TWKBUtils.MULTIPOINT_TYPE;
		}
		else if (geom instanceof MultiLineString) {
			return TWKBUtils.MULTILINESTRING_TYPE;
		}
		else if (geom instanceof MultiPolygon) {
			return TWKBUtils.MULTIPOLYGON_TYPE;
		}
		return TWKBUtils.GEOMETRYCOLLECTION_TYPE;
	}

	private static class PrecisionWriter
	{
		private int precision = TWKBUtils.MIN_COORD_PRECISION;
		protected double precisionMultiplier = 0;

		public PrecisionWriter calculate(
				Coordinate[] coordinates,
				int maxPrecision ) {
			for (int i = 0; i < coordinates.length; i++) {
				checkCoordinate(coordinates[i]);
			}
			finalize(maxPrecision);
			return this;
		}

		protected void checkCoordinate(
				Coordinate c ) {
			BigDecimal xCoord = new BigDecimal(
					Double.toString(c.getX())).stripTrailingZeros();
			precision = Math.max(
					xCoord.scale(),
					precision);
			BigDecimal yCoord = new BigDecimal(
					Double.toString(c.getY())).stripTrailingZeros();
			precision = Math.max(
					yCoord.scale(),
					precision);
		}

		protected void finalize(
				int maxPrecision ) {
			precision = Math.min(
					maxPrecision,
					precision);
			precisionMultiplier = Math.pow(
					10,
					precision);
		}

		public void writeExtendedPrecision(
				DataOutput output )
				throws IOException {
			return;
		}

		public void writePoint(
				Coordinate coordinate,
				DataOutput output )
				throws IOException {
			Varint.writeSignedVarLong(
					Math.round(coordinate.getX() * precisionMultiplier),
					output);
			Varint.writeSignedVarLong(
					Math.round(coordinate.getY() * precisionMultiplier),
					output);
		}

		public void writePointArray(
				Coordinate[] coordinates,
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
	}

	private static class ExtendedPrecisionWriter extends
			PrecisionWriter
	{
		private boolean hasZ = false;
		private int zPrecision = TWKBUtils.MIN_EXTENDED_PRECISION;
		private double zPrecisionMultiplier = 0;
		private boolean hasM = false;
		private int mPrecision = TWKBUtils.MIN_EXTENDED_PRECISION;
		private double mPrecisionMultiplier = 0;

		@Override
		public PrecisionWriter calculate(
				Coordinate[] coordinates,
				int maxPrecision ) {
			this.hasZ = !Double.isNaN(coordinates[0].getZ());
			this.hasM = !Double.isNaN(coordinates[0].getM());
			super.calculate(
					coordinates,
					maxPrecision);
			return this;
		}

		@Override
		protected void checkCoordinate(
				Coordinate c ) {
			super.checkCoordinate(c);
			if (hasZ) {
				BigDecimal zCoord = new BigDecimal(
						Double.toString(c.getZ())).stripTrailingZeros();
				zPrecision = Math.max(
						zCoord.scale(),
						zPrecision);
			}
			if (hasM) {
				BigDecimal mCoord = new BigDecimal(
						Double.toString(c.getM())).stripTrailingZeros();
				mPrecision = Math.max(
						mCoord.scale(),
						mPrecision);
			}
		}

		@Override
		protected void finalize(
				int maxPrecision ) {
			super.finalize(maxPrecision);
			if (hasZ) {
				zPrecision = Math.min(
						TWKBUtils.MAX_EXTENDED_PRECISION,
						zPrecision);
				zPrecisionMultiplier = Math.pow(
						10,
						zPrecision);
			}
			if (hasM) {
				mPrecision = Math.min(
						TWKBUtils.MAX_EXTENDED_PRECISION,
						mPrecision);
				mPrecisionMultiplier = Math.pow(
						10,
						mPrecision);
			}
		}

		@Override
		public void writeExtendedPrecision(
				DataOutput output )
				throws IOException {
			byte extendedDimensions = 0;
			if (hasZ) {
				extendedDimensions |= 0x1;
				extendedDimensions |= TWKBUtils.zigZagEncode(zPrecision) << 2;
			}
			if (hasM) {
				extendedDimensions |= 0x2;
				extendedDimensions |= TWKBUtils.zigZagEncode(mPrecision) << 5;
			}
			output.writeByte(extendedDimensions);
		}

		@Override
		public void writePoint(
				Coordinate coordinate,
				DataOutput output )
				throws IOException {
			super.writePoint(
					coordinate,
					output);
			if (hasZ) {
				Varint.writeSignedVarLong(
						Math.round(coordinate.getZ() * zPrecisionMultiplier),
						output);
			}
			if (hasM) {
				Varint.writeSignedVarLong(
						Math.round(coordinate.getM() * mPrecisionMultiplier),
						output);
			}
		}

		@Override
		public void writePointArray(
				Coordinate[] coordinates,
				DataOutput output )
				throws IOException {
			long lastX = 0;
			long lastY = 0;
			long lastZ = 0;
			long lastM = 0;
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
				if (hasZ) {
					long z = Math.round(c.getZ() * zPrecisionMultiplier);
					Varint.writeSignedVarLong(
							z - lastZ,
							output);
					lastZ = z;
				}
				if (hasM) {
					long m = Math.round(c.getZ() * mPrecisionMultiplier);
					Varint.writeSignedVarLong(
							m - lastM,
							output);
					lastM = m;
				}
			}
		}
	}
}
