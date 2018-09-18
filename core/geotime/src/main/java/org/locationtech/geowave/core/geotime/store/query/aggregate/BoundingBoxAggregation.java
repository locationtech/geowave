package org.locationtech.geowave.core.geotime.store.query.aggregate;

import java.nio.ByteBuffer;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.Aggregation;

import com.vividsolutions.jts.geom.Envelope;

abstract public class BoundingBoxAggregation<P extends Persistable, T> implements
		Aggregation<P, Envelope, T>
{

	protected double minX = Double.MAX_VALUE;
	protected double minY = Double.MAX_VALUE;
	protected double maxX = -Double.MAX_VALUE;
	protected double maxY = -Double.MAX_VALUE;

	@Override
	public byte[] toBinary() {
		return new byte[0];
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public P getParameters() {
		return null;
	}

	@Override
	public void setParameters(
			final P parameters ) {}

	public boolean isSet() {
		if ((minX == Double.MAX_VALUE) || (minY == Double.MAX_VALUE) || (maxX == -Double.MAX_VALUE)
				|| (maxY == -Double.MAX_VALUE)) {
			return false;
		}
		return true;
	}

	@Override
	public Envelope getResult() {
		if (!isSet()) {
			return new Envelope();
		}
		return new Envelope(
				minX,
				maxX,
				minY,
				maxY);
	}

	@Override
	public Envelope merge(
			final Envelope result1,
			final Envelope result2 ) {
		if (result1.isNull()) {
			return result2;
		}
		else if (result2.isNull()) {
			return result1;
		}
		final double minX = Math.min(
				result1.getMinX(),
				result2.getMinX());
		final double minY = Math.min(
				result1.getMinY(),
				result2.getMinY());
		final double maxX = Math.max(
				result1.getMaxX(),
				result2.getMaxX());
		final double maxY = Math.max(
				result1.getMaxY(),
				result2.getMaxY());
		return new Envelope(
				minX,
				maxX,
				minY,
				maxY);
	}

	@Override
	public byte[] resultToBinary(
			final Envelope result ) {
		final ByteBuffer buffer = ByteBuffer.allocate(32);
		buffer.putDouble(minX);
		buffer.putDouble(minY);
		buffer.putDouble(maxX);
		buffer.putDouble(maxY);
		return buffer.array();
	}

	@Override
	public Envelope resultFromBinary(
			final byte[] binary ) {
		final ByteBuffer buffer = ByteBuffer.wrap(binary);
		final double minX = buffer.getDouble();
		final double minY = buffer.getDouble();
		final double maxX = buffer.getDouble();
		final double maxY = buffer.getDouble();
		return new Envelope(
				minX,
				maxX,
				minY,
				maxY);
	}

	@Override
	public void clearResult() {
		minX = Double.MAX_VALUE;
		minY = Double.MAX_VALUE;
		maxX = -Double.MAX_VALUE;
		maxY = -Double.MAX_VALUE;
	}

	@Override
	public void aggregate(
			final T entry ) {
		final Envelope env = getEnvelope(entry);
		aggregate(env);
	}

	protected void aggregate(
			final Envelope env ) {
		if ((env != null) && !env.isNull()) {
			minX = Math.min(
					minX,
					env.getMinX());
			minY = Math.min(
					minY,
					env.getMinY());
			maxX = Math.max(
					maxX,
					env.getMaxX());
			maxY = Math.max(
					maxY,
					env.getMaxY());
		}
	}

	abstract protected Envelope getEnvelope(
			final T entry );

}
