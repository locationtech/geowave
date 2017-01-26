package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.IndexStrategy;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class IndexMetaDataSet<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T>
{
	private List<IndexMetaData> metaData;
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"INDEX_METADATA");

	protected IndexMetaDataSet() {}

	private IndexMetaDataSet(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			final List<IndexMetaData> metaData ) {
		super(
				adapterId,
				composeId(statisticsId));
		this.metaData = metaData;
	}

	public IndexMetaDataSet(
			final ByteArrayId adapterId,
			final ByteArrayId statisticsId,
			final IndexStrategy<?, ?> indexStrategy ) {
		super(
				adapterId,
				composeId(statisticsId));
		this.metaData = indexStrategy.createMetaData();
	}

	public static ByteArrayId composeId(
			final ByteArrayId statisticsId ) {
		return composeId(
				STATS_TYPE.getString(),
				statisticsId.getString());
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new IndexMetaDataSet<T>(
				dataAdapterId,
				statisticsId,
				this.metaData);
	}

	public List<IndexMetaData> getMetaData() {
		return metaData;
	}

	@Override
	public byte[] toBinary() {
		final byte[] metaBytes = PersistenceUtils.toBinary(metaData);
		final ByteBuffer buffer = super.binaryBuffer(metaBytes.length);
		buffer.put(metaBytes);
		return buffer.array();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final byte[] metaBytes = new byte[buffer.remaining()];
		buffer.get(metaBytes);

		metaData = (List) PersistenceUtils.fromBinary(metaBytes);
	}

	public IndexMetaData[] toArray() {
		return metaData.toArray(new IndexMetaData[metaData.size()]);
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof IndexMetaDataSet)) {
			for (int i = 0; i < metaData.size(); i++) {
				metaData.get(
						i).merge(
						((IndexMetaDataSet<T>) merge).metaData.get(i));
			}
		}
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final IndexMetaData imd : this.metaData) {
			imd.insertionIdsAdded(entryInfo.getInsertionIds());
		}
	}

	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		for (final IndexMetaData imd : this.metaData) {
			imd.insertionIdsRemoved(entryInfo.getInsertionIds());
		}
	}

	public static IndexMetaData[] getIndexMetadata(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		IndexMetaDataSet combinedMetaData = null;
		for (final ByteArrayId adapterId : adapterIdsToQuery) {
			final IndexMetaDataSet adapterMetadata = (IndexMetaDataSet) statisticsStore.getDataStatistics(
					adapterId,
					IndexMetaDataSet.composeId(index.getId()),
					authorizations);
			if (combinedMetaData == null) {
				combinedMetaData = adapterMetadata;
			}
			else {
				combinedMetaData.merge(adapterMetadata);
			}
		}
		return combinedMetaData != null ? combinedMetaData.toArray() : null;
	}

	/**
	 * Convert Index Metadata statistics to a JSON object
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

		JSONArray mdArray = new JSONArray();
		for (final IndexMetaData imd : this.metaData) {
			mdArray.add(imd.toJSONObject());
		}
		jo.put(
				"metadata",
				mdArray);

		return jo;
	}

}
