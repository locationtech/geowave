package mil.nga.giat.geowave.format.gpx;

import java.nio.ByteBuffer;

import com.beust.jcommander.Parameter;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.core.index.persist.Persistable;

public class MaxExtentOptProvider implements
		Persistable
{
	@Parameter(names = "--maxLength", description = "Maximum extent (in both dimensions) for gpx track in degrees. Used to remove excessively long gpx tracks")
	private double maxExtent = Double.MAX_VALUE;

	@Override
	public byte[] toBinary() {
		final byte[] backingBuffer = new byte[Double.BYTES];
		ByteBuffer buf = ByteBuffer.wrap(backingBuffer);
		buf.putDouble(maxExtent);
		return backingBuffer;
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		maxExtent = ByteBuffer.wrap(
				bytes).getDouble();
	}

	public double getMaxExtent() {
		return maxExtent;
	}

	public boolean filterMaxExtent(
			Geometry geom ) {
		return (geom.getEnvelopeInternal().maxExtent() < maxExtent);
	}
}
