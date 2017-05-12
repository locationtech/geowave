package mil.nga.giat.geowave.adapter.raster.adapter.merge.nodata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class NoDataByFilter implements
		NoDataMetadata
{
	private final static Logger LOGGER = LoggerFactory.getLogger(NoDataByFilter.class);
	private Geometry shape;
	private double[][] noDataPerBand;

	protected NoDataByFilter() {}

	public NoDataByFilter(
			final Geometry shape,
			final double[][] noDataPerBand ) {
		this.shape = shape;
		this.noDataPerBand = noDataPerBand;
	}

	public Geometry getShape() {
		return shape;
	}

	public double[][] getNoDataPerBand() {
		return noDataPerBand;
	}

	@Override
	public boolean isNoData(
			final SampleIndex index,
			final double value ) {
		if ((noDataPerBand != null) && (noDataPerBand.length > index.getBand())) {
			for (final double noDataVal : noDataPerBand[index.getBand()]) {
				// use object equality to capture NaN, and positive and negative
				// infinite equality
				if (new Double(
						value).equals(new Double(
						noDataVal))) {
					return true;
				}
			}
		}
		if ((shape != null) && !shape.intersects(new GeometryFactory().createPoint(new Coordinate(
				index.getX(),
				index.getY())))) {
			return true;
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final byte[] noDataBinary;
		if ((noDataPerBand != null) && (noDataPerBand.length > 0)) {
			int totalBytes = 4;
			final List<byte[]> noDataValuesBytes = new ArrayList<byte[]>(
					noDataPerBand.length);
			for (final double[] noDataValues : noDataPerBand) {
				final int thisBytes = 4 + (noDataValues.length * 8);
				totalBytes += thisBytes;
				final ByteBuffer noDataBuf = ByteBuffer.allocate(thisBytes);
				noDataBuf.putInt(noDataValues.length);
				for (final double noDataValue : noDataValues) {
					noDataBuf.putDouble(noDataValue);
				}
				noDataValuesBytes.add(noDataBuf.array());
			}
			final ByteBuffer noDataBuf = ByteBuffer.allocate(totalBytes);
			noDataBuf.putInt(noDataPerBand.length);
			for (final byte[] noDataValueBytes : noDataValuesBytes) {
				noDataBuf.put(noDataValueBytes);
			}
			noDataBinary = noDataBuf.array();
		}
		else {
			noDataBinary = new byte[] {};
		}
		final byte[] geometryBinary;
		if (shape == null) {
			geometryBinary = new byte[0];
		}
		else {
			geometryBinary = GeometryUtils.geometryToBinary(shape);
		}
		final ByteBuffer buf = ByteBuffer.allocate(geometryBinary.length + noDataBinary.length + 4);
		buf.putInt(noDataBinary.length);
		buf.put(noDataBinary);
		buf.put(geometryBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int noDataBinaryLength = buf.getInt();
		final byte[] geometryBinary = new byte[bytes.length - noDataBinaryLength - 4];
		if (noDataBinaryLength == 0) {
			noDataPerBand = new double[][] {};
		}
		else {
			noDataPerBand = new double[buf.getInt()][];
			for (int b = 0; b < noDataPerBand.length; b++) {
				noDataPerBand[b] = new double[buf.getInt()];
				for (int i = 0; i < noDataPerBand[b].length; i++) {
					noDataPerBand[b][i] = buf.getDouble();
				}
			}
		}
		if (geometryBinary.length > 0) {
			buf.get(geometryBinary);
			shape = GeometryUtils.geometryFromBinary(geometryBinary);
		}
		else {
			shape = null;
		}
	}

	@Override
	public Set<SampleIndex> getNoDataIndices() {
		return null;
	}
}
