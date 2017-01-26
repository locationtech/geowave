package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

abstract public class NumericRangeDataStatistics<T> extends
		AbstractDataStatistics<T>
{

	private double min = Double.MAX_VALUE;
	private double max = -Double.MAX_VALUE;

	protected NumericRangeDataStatistics() {
		super();
	}

	public NumericRangeDataStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsId ) {
		super(
				dataAdapterId,
				statisticsId);
	}

	public boolean isSet() {
		if ((min == Double.MAX_VALUE) && (max == -Double.MAX_VALUE)) {
			return false;
		}
		return true;
	}

	public double getMin() {
		return min;
	}

	public double getMax() {
		return max;
	}

	public double getRange() {
		return max - min;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(16);
		buffer.putDouble(min);
		buffer.putDouble(max);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		min = buffer.getDouble();
		max = buffer.getDouble();
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		final NumericRange range = getRange(entry);
		if (range != null) {
			min = Math.min(
					min,
					range.getMin());
			max = Math.max(
					max,
					range.getMax());
		}
	}

	abstract protected NumericRange getRange(
			final T entry );

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof NumericRangeDataStatistics)) {
			final NumericRangeDataStatistics<T> stats = (NumericRangeDataStatistics<T>) statistics;
			if (stats.isSet()) {
				min = Math.min(
						min,
						stats.getMin());
				max = Math.max(
						max,
						stats.getMax());
			}
		}
	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"range[adapter=").append(
				super.getDataAdapterId().getString());
		if (isSet()) {
			buffer.append(
					", min=").append(
					getMin());
			buffer.append(
					", max=").append(
					getMax());
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Feature Numeric Range statistics to a JSON object
	 */

	public JSONObject toJSONObject()
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				"GENERIC_RANGE");

		jo.put(
				"statisticsID",
				statisticsId.getString());

		if (!isSet()) {
			jo.put(
					"range",
					"No Values");
		}
		else {
			jo.put(
					"range_min",
					getMin());
			jo.put(
					"range_max",
					getMax());
		}

		return jo;
	}

}
