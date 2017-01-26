package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.HashSet;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;

public class CountDataStatistics<T> extends
		AbstractDataStatistics<T> implements
		IngestCallback<T>,
		DeleteCallback<T>
{
	public final static ByteArrayId STATS_TYPE = new ByteArrayId(
			"COUNT_DATA");

	private long count = Long.MIN_VALUE;

	protected CountDataStatistics() {
		super();
	}

	public CountDataStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statisticsID ) {
		super(
				dataAdapterId,
				statisticsID);
	}

	public CountDataStatistics(
			final ByteArrayId dataAdapterId ) {
		super(
				dataAdapterId,
				STATS_TYPE);
	}

	public boolean isSet() {
		return count != Long.MIN_VALUE;
	}

	public long getCount() {
		return count;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(8);
		buffer.putLong(count);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		count = buffer.getLong();
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (!isSet()) {
			count = 0;
		}
		count += 1;
	}

	@Override
	public void merge(
			final Mergeable statistics ) {
		if (!isSet()) {
			count = 0;
		}
		if ((statistics != null) && (statistics instanceof CountDataStatistics)) {
			@SuppressWarnings("unchecked")
			final CountDataStatistics<T> cStats = (CountDataStatistics<T>) statistics;
			if (cStats.isSet()) {
				count = count + cStats.count;
			}
		}
	}

	/**
	 * This is expensive, but necessary since there may be duplicates
	 */
	private transient HashSet<ByteArrayId> ids = new HashSet<ByteArrayId>();

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (ids.add(new ByteArrayId(
				entryInfo.getDataId()))) {
			if (!isSet()) {
				count = 0;
			}
			count -= 1;
		}

	}

	public String toString() {
		StringBuffer buffer = new StringBuffer();
		buffer.append(
				"count[adapter=").append(
				super.getDataAdapterId().getString());
		buffer.append(
				", count=").append(
				count);
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Count statistics to a JSON object
	 */

	public JSONObject toJSONObject()
			throws JSONException {
		JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"statisticsID",
				statisticsId.getString());
		jo.put(
				"count",
				count);
		return jo;
	}
}
