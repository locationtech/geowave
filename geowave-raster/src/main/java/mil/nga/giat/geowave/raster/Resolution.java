package mil.nga.giat.geowave.raster;

import java.nio.ByteBuffer;
import java.util.Arrays;

import mil.nga.giat.geowave.index.Persistable;

public class Resolution implements
		Comparable<Resolution>,
		Persistable
{
	private double[] resolutionPerDimension;

	protected Resolution() {}

	public Resolution(
			final double[] resolutionPerDimension ) {
		this.resolutionPerDimension = resolutionPerDimension;
	}

	public int getDimensions() {
		return resolutionPerDimension.length;
	}

	public double getResolution(
			final int dimension ) {
		return resolutionPerDimension[dimension];
	}

	public double[] getResolutionPerDimension() {
		return resolutionPerDimension;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(resolutionPerDimension);
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Resolution other = (Resolution) obj;
		if (!Arrays.equals(
				resolutionPerDimension,
				other.resolutionPerDimension)) {
			return false;
		}
		return true;
	}

	@Override
	public int compareTo(
			final Resolution o ) {
		double resSum = 0;
		double otherResSum = 0;
		for (final double res : resolutionPerDimension) {
			resSum += res;
		}
		for (final double res : o.resolutionPerDimension) {
			otherResSum += res;
		}
		return Double.compare(
				resSum,
				otherResSum);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(resolutionPerDimension.length * 8);
		for (final double val : resolutionPerDimension) {
			buf.putDouble(val);
		}
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int length = bytes.length / 8;
		resolutionPerDimension = new double[length];
		for (int i = 0; i < length; i++) {
			resolutionPerDimension[i] = buf.getDouble();
		}
	}
}
