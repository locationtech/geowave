package mil.nga.giat.geowave.store.adapter.statistics;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.store.IngestEntryInfo;

import com.vividsolutions.jts.geom.Envelope;

abstract public class BoundingBoxDataStatistics<T> extends
		AbstractDataStatistics<T>
{
	public final static ByteArrayId STATS_ID = new ByteArrayId(
			"BOUNDING_BOX");

	private double minX = Double.MAX_VALUE;
	private double minY = Double.MAX_VALUE;
	private double maxX = -Double.MAX_VALUE;
	private double maxY = -Double.MAX_VALUE;

	protected BoundingBoxDataStatistics() {
		super();
	}

	public BoundingBoxDataStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId);
	}

	public boolean isSet() {
		if ((minX == Double.MAX_VALUE) || (minY == Double.MAX_VALUE) || (maxX == -Double.MAX_VALUE) || (maxY == -Double.MAX_VALUE)) {
			return false;
		}
		return true;
	}

	public double getMinX() {
		return minX;
	}

	public double getMinY() {
		return minY;
	}

	public double getMaxX() {
		return maxX;
	}

	public double getMaxY() {
		return maxY;
	}

	public double getWidth() {
		return maxX - minX;
	}

	public double getHeight() {
		return maxY - minY;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = ByteBuffer.allocate(32);
		buffer.putDouble(minX);
		buffer.putDouble(minY);
		buffer.putDouble(maxX);
		buffer.putDouble(maxY);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		minX = buffer.getDouble();
		minY = buffer.getDouble();
		maxX = buffer.getDouble();
		maxY = buffer.getDouble();
	}

	@Override
	public void entryIngested(
			final IngestEntryInfo entryInfo,
			final T entry ) {
		final Envelope env = getEnvelope(entry);
		if (env != null) {
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

	@Override
	public ByteArrayId getStatisticsId() {
		return STATS_ID;
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof BoundingBoxDataStatistics)) {
			final BoundingBoxDataStatistics<T> bboxStats = (BoundingBoxDataStatistics<T>) statistics;
			if (bboxStats.isSet()) {
				minX = Math.min(
						minX,
						bboxStats.minX);
				minY = Math.min(
						minY,
						bboxStats.minY);
				maxX = Math.max(
						maxX,
						bboxStats.maxX);
				maxY = Math.max(
						maxY,
						bboxStats.maxY);
			}
		}
	}
}
