package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class DuplicateEntryCount<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"DUPLICATE_ENTRY_COUNT");
	private long entriesWithDuplicates = 0;

	protected DuplicateEntryCount() {
		super();
	}

	public long getEntriesWithDuplicatesCount() {
		return entriesWithDuplicates;
	}

	public boolean isAnyEntryHaveDuplicates() {
		return entriesWithDuplicates > 0;
	}

	private DuplicateEntryCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId statsId,
			final long entriesWithDuplicates ) {
		super(
				dataAdapterId,
				statsId);
		this.entriesWithDuplicates = entriesWithDuplicates;
	}

	public DuplicateEntryCount(
			final ByteArrayId dataAdapterId,
			final ByteArrayId indexId ) {
		super(
				dataAdapterId,
				composeId(indexId));
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						ArrayUtils.addAll(
								STATS_TYPE.getBytes(),
								STATS_SEPARATOR.getBytes()),
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new DuplicateEntryCount<>(
				dataAdapterId,
				statisticsId,
				entriesWithDuplicates);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = super.binaryBuffer(8);
		buf.putLong(entriesWithDuplicates);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		entriesWithDuplicates = buf.getLong();
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (entryHasDuplicates(entryInfo)) {
			entriesWithDuplicates++;
		}
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		if (entryHasDuplicates(entryInfo)) {
			entriesWithDuplicates--;
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if (merge != null && merge instanceof DuplicateEntryCount) {
			entriesWithDuplicates += ((DuplicateEntryCount) merge).entriesWithDuplicates;
		}
	}

	private static boolean entryHasDuplicates(
			final DataStoreEntryInfo entryInfo ) {
		if ((entryInfo != null) && (entryInfo.getRowIds() != null)) {
			return entryInfo.getRowIds().size() > 1;
		}
		return false;
	}

	public static DuplicateEntryCount getDuplicateCounts(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIdsToQuery,
			DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		DuplicateEntryCount combinedDuplicateCount = null;
		for (final ByteArrayId adapterId : adapterIdsToQuery) {
			final DuplicateEntryCount adapterVisibilityCount = (DuplicateEntryCount) statisticsStore.getDataStatistics(
					adapterId,
					DuplicateEntryCount.composeId(index.getId()),
					authorizations);
			if (combinedDuplicateCount == null) {
				combinedDuplicateCount = adapterVisibilityCount;
			}
			else {
				combinedDuplicateCount.merge(adapterVisibilityCount);
			}
		}
		return combinedDuplicateCount;
	}

	/**
	 * Convert Duplicate Count statistics to a JSON object
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
				entriesWithDuplicates);

		return jo;
	}

}
