package mil.nga.giat.geowave.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.Mergeable;
import mil.nga.giat.geowave.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.store.query.BasicQuery.Constraints;

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
				dataAdapterId,
				STATS_ID);
	}

	public BoundingBoxDataStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId staticticsId ) {
		super(
				dataAdapterId,
				staticticsId);
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
		final ByteBuffer buffer = super.binaryBuffer(32);
		buffer.putDouble(minX);
		buffer.putDouble(minY);
		buffer.putDouble(maxX);
		buffer.putDouble(maxY);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		minX = buffer.getDouble();
		minY = buffer.getDouble();
		maxX = buffer.getDouble();
		maxY = buffer.getDouble();
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
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

	public Constraints getConstraints() {
		// Create a NumericRange object using the x axis
		final NumericRange rangeLongitude = new NumericRange(
				minX,
				maxX);

		// Create a NumericRange object using the y axis
		final NumericRange rangeLatitude = new NumericRange(
				minY,
				maxY);

		final Map<Class<? extends NumericDimensionDefinition>, ConstraintData> constraintsPerDimension = new HashMap<Class<? extends NumericDimensionDefinition>, ConstraintData>();
		// Create and return a new IndexRange array with an x and y axis
		// range
		constraintsPerDimension.put(
				LongitudeDefinition.class,
				new ConstraintData(
						rangeLongitude,
						true));
		constraintsPerDimension.put(
				LatitudeDefinition.class,
				new ConstraintData(
						rangeLatitude,
						true));
		return new Constraints(
				constraintsPerDimension);
	}

	abstract protected Envelope getEnvelope(
			final T entry );

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
