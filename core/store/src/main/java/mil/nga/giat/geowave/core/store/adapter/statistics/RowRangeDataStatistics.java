package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.ArrayUtils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;

public class RowRangeDataStatistics<T> extends
		AbstractDataStatistics<T>
{
	public final static ByteArrayId STATS_TYPE = new ByteArrayId(
			"ROW_RANGE");

	private byte[] min = new byte[] {
		(byte) 0xff
	};
	private byte[] max = new byte[] {
		(byte) 0x00
	};

	public RowRangeDataStatistics() {}

	public RowRangeDataStatistics(
			ByteArrayId statisticsId ) {
		super(
				statisticsId,
				composeId(statisticsId));
	}

	public static ByteArrayId composeId(
			ByteArrayId statisticsId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						ArrayUtils.addAll(
								STATS_TYPE.getBytes(),
								STATS_SEPARATOR.getBytes()),
						statisticsId.getBytes()));
	}

	public boolean isSet() {
		return ((min.length > 1) || (min[0] != (byte) 0xff));
	}

	public byte[] getMin() {
		return min;
	}

	public byte[] getMax() {
		return max;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(min.length + max.length + 4);
		buffer.putInt(min.length);
		buffer.put(min);
		buffer.put(max);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final int minLength = buffer.getInt();
		min = new byte[minLength];
		buffer.get(min);
		max = new byte[buffer.capacity() - buffer.position()];
		buffer.get(max);
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final ByteArrayId ids : entryInfo.getRowIds()) {
			final byte[] idBytes = ids.getBytes();
			min = compare(
					min,
					idBytes,
					cardinality(
							min,
							idBytes)) > 0 ? idBytes : min;
			max = compare(
					max,
					idBytes,
					cardinality(
							max,
							idBytes)) < 0 ? idBytes : max;
		}
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if ((statistics != null) && (statistics instanceof RowRangeDataStatistics)) {
			@SuppressWarnings("unchecked")
			final RowRangeDataStatistics<T> stats = (RowRangeDataStatistics<T>) statistics;
			if (stats.isSet()) {
				min = compare(
						min,
						stats.min,
						cardinality(
								min,
								stats.min)) > 0 ? stats.min : min;
				max = compare(
						max,
						stats.max,
						cardinality(
								max,
								stats.max)) < 0 ? stats.max : max;
			}
		}
	}

	private int compare(
			final byte[] a,
			final byte[] b,
			final int cardinality ) {
		final BigInteger one = toInteger(
				a,
				cardinality);

		final BigInteger two = toInteger(
				b,
				cardinality);
		return one.compareTo(two);
	}

	private BigInteger toInteger(
			final byte[] data,
			final int cardinality ) {
		return new BigInteger(
				expandBy(
						data,
						cardinality));
	}

	private byte[] expandBy(
			final byte[] b,
			int cardinality ) {
		final byte[] newD = new byte[cardinality + 1];
		System.arraycopy(
				b,
				0,
				newD,
				1,
				b.length);
		return newD;
	}

	private int cardinality(
			final byte[] a,
			final byte[] b ) {
		return Math.max(
				a.length,
				b.length);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"rows[index=").append(
				getStatisticsId());
		if (isSet()) {
			buffer.append(
					", min=").append(
					StringUtils.stringFromBinary(getMin()));
			buffer.append(
					", max=").append(
					StringUtils.stringFromBinary(getMax()));
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	public JSONObject toJSONObject()
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());

		jo.put(
				"statisticsID",
				statisticsId.getString());

		if (isSet()) {
			jo.put(
					"min",
					StringUtils.stringFromBinary(getMin()));
			jo.put(
					"max",
					StringUtils.stringFromBinary(getMax()));
		}
		else {
			jo.put(
					"values",
					"not set");
		}

		return jo;
	}

}
