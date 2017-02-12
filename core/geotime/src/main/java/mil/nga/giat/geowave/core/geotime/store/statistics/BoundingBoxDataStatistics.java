package mil.nga.giat.geowave.core.geotime.store.statistics;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import com.vividsolutions.jts.geom.Envelope;

import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.LongitudeDefinition;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintData;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;

abstract public class BoundingBoxDataStatistics<T> extends
		AbstractDataStatistics<T>
{
	public final static ByteArrayId STATS_TYPE = new ByteArrayId(
			"BOUNDING_BOX");

	protected double minX = Double.MAX_VALUE;
	protected double minY = Double.MAX_VALUE;
	protected double maxX = -Double.MAX_VALUE;
	protected double maxY = -Double.MAX_VALUE;

	protected BoundingBoxDataStatistics() {
		super();
	}

	public BoundingBoxDataStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId,
				STATS_TYPE);
	}

	public BoundingBoxDataStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId staticticsId ) {
		super(
				dataAdapterId,
				staticticsId);
	}

	public boolean isSet() {
		if ((minX == Double.MAX_VALUE) || (minY == Double.MAX_VALUE) || (maxX == -Double.MAX_VALUE)
				|| (maxY == -Double.MAX_VALUE)) {
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

	public ConstraintSet getConstraints() {
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
		return new ConstraintSet(
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

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"bbox[adapter=").append(
				super.getDataAdapterId().getString());
		if (isSet()) {
			buffer.append(
					", minX=").append(
					minX);
			buffer.append(
					", maxX=").append(
					maxX);
			buffer.append(
					", minY=").append(
					minY);
			buffer.append(
					", maxY=").append(
					maxY);
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Fixed Bin Numeric statistics to a JSON object
	 */

	public JSONObject toJSONObject()
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"statisticsId",
				statisticsId.getString());

		if (isSet()) {
			jo.put(
					"minX",
					minX);
			jo.put(
					"maxX",
					maxX);
			jo.put(
					"minY",
					minY);
			jo.put(
					"maxY",
					maxY);
		}
		else {
			jo.put(
					"boundaries",
					"No Values");
		}
		return jo;
	}

}
